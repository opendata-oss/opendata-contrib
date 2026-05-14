//! `RealClickHouseSink` — wraps the production
//! `ClickHouseSink<OtlpLogsClickHouseAdapter>` and adds the
//! [`crate::test_observable_sink::TestObservableSink`] surface so
//! the bench's correctness scenarios run against the real sink
//! end-to-end.
//!
//! Sink::write interposes:
//!  1. Block hook — if a `CancellationToken` is registered for the
//!     sequence, park on it before any other side effect.
//!  2. Forced-outcome hook — if a scripted attempt is queued for
//!     the sequence, return its result instead of delegating. The
//!     `MaybeCommittedThenOk` script pushes a `MaybeCommitted` for
//!     the first call and an `Ok` (delegate to inner) for the
//!     second; per the trait's docs, the runtime's
//!     `check_committed → retry` path drives the second call.
//!  3. Delegate to inner — production `ClickHouseSink::write`.
//!  4. Commit observer — fires synchronously after inner Ok.
//!  5. Captured-writes log — appends a `CapturedWrite` for every
//!     resolved call (Ok or Failure).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use opendata_ingest_clickhouse::adapter::logs::OtlpLogsClickHouseAdapter;
use opendata_ingest_clickhouse::sink::ClickHouseSink;
use opendata_ingest_runtime::error::RuntimeResult;
use opendata_ingest_runtime::identity::CommitIdentity;
use opendata_ingest_runtime::sink::{
    CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
};
use tokio_util::sync::CancellationToken;

use crate::test_observable_sink::{
    CapturedWrite, CommitObserver, ScriptedWrite, SinkCommitFailureKind, TestObservableSink,
    WriteOutcome,
};

/// Internal per-attempt outcome the queue stores. Maps from the
/// trait's high-level `ScriptedWrite` to a flat sequence of
/// attempts the wrapper consumes per `write` call. `DelegateOk`
/// means "delegate to the inner sink"; the runtime's retry path
/// fires this on the second attempt of a `MaybeCommittedThenOk`
/// script.
#[derive(Debug, Clone)]
enum Attempt {
    DelegateOk,
    MaybeCommitted,
}

/// Wraps the production `ClickHouseSink<OtlpLogsClickHouseAdapter>`
/// with bench scripting surface. Cloneable — the bench's
/// correctness scenarios hold one handle to install hooks, then
/// pass another (same internal state) to the runtime via
/// `Runtime::builder().set_sink(...)`.
#[derive(Clone)]
pub struct RealClickHouseSink {
    inner: Arc<ClickHouseSink<OtlpLogsClickHouseAdapter>>,
    id: SinkId,
    blocks: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    scripts: Arc<Mutex<HashMap<u64, VecDeque<Attempt>>>>,
    commit_observer: Arc<Mutex<Option<Arc<dyn CommitObserver>>>>,
    captured: Arc<Mutex<Vec<CapturedWrite>>>,
}

impl RealClickHouseSink {
    pub fn new(id: impl Into<SinkId>, inner: ClickHouseSink<OtlpLogsClickHouseAdapter>) -> Self {
        Self {
            inner: Arc::new(inner),
            id: id.into(),
            blocks: Arc::new(Mutex::new(HashMap::new())),
            scripts: Arc::new(Mutex::new(HashMap::new())),
            commit_observer: Arc::new(Mutex::new(None)),
            captured: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Park on the block-token for this sequence if one was
    /// registered. `CancellationToken` is lost-wakeup-safe so a
    /// cancel that landed before the await still resolves.
    async fn await_block(&self, seq: u64) {
        let token = self.blocks.lock().unwrap().get(&seq).cloned();
        if let Some(t) = token {
            t.cancelled().await;
        }
    }

    /// Pop the next scripted attempt for this sequence; `None`
    /// means "delegate to the inner sink unconditionally".
    fn next_attempt(&self, seq: u64) -> Option<Attempt> {
        let mut map = self.scripts.lock().unwrap();
        if let Some(queue) = map.get_mut(&seq)
            && let Some(next) = queue.pop_front()
        {
            return Some(next);
        }
        None
    }
}

#[async_trait]
impl Sink for RealClickHouseSink {
    fn id(&self) -> &SinkId {
        &self.id
    }
    fn write_budget(&self) -> SinkBudget {
        self.inner.write_budget()
    }
    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        let seq = commit.identity.range.high;
        self.await_block(seq).await;

        // Forced outcome takes precedence over delegating. Pop one
        // attempt per `write` call; the runtime's
        // `check_committed → retry` path drives subsequent calls
        // for the same sequence and pops further attempts.
        match self.next_attempt(seq) {
            Some(Attempt::MaybeCommitted) => {
                self.captured.lock().unwrap().push(CapturedWrite {
                    identity: commit.identity.clone(),
                    outcome: WriteOutcome::Failure(SinkCommitFailureKind::MaybeCommitted),
                });
                return Err(SinkCommitFailure::MaybeCommitted(
                    format!("scripted MaybeCommitted on seq={seq}").into(),
                ));
            }
            Some(Attempt::DelegateOk) | None => {
                // Fall through to delegate.
            }
        }

        let result = self.inner.write(commit.clone()).await;
        match &result {
            Ok(commit_result) => {
                let observer = self.commit_observer.lock().unwrap().clone();
                if let Some(cb) = observer {
                    cb.record_commit(&commit.identity);
                }
                self.captured.lock().unwrap().push(CapturedWrite {
                    identity: commit.identity.clone(),
                    outcome: WriteOutcome::Committed {
                        rows: commit_result.rows_written,
                    },
                });
            }
            Err(e) => {
                let kind = match e {
                    SinkCommitFailure::NotCommitted(_) => SinkCommitFailureKind::NotCommitted,
                    SinkCommitFailure::MaybeCommitted(_) => SinkCommitFailureKind::MaybeCommitted,
                    SinkCommitFailure::Fatal(_) => SinkCommitFailureKind::Fatal,
                };
                self.captured.lock().unwrap().push(CapturedWrite {
                    identity: commit.identity.clone(),
                    outcome: WriteOutcome::Failure(kind),
                });
            }
        }
        result
    }
    async fn check_committed(&self, identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        self.inner.check_committed(identity).await
    }
}

#[async_trait]
impl TestObservableSink for RealClickHouseSink {
    fn set_per_sequence_block(&self, seq: u64) -> CancellationToken {
        let token = CancellationToken::new();
        self.blocks.lock().unwrap().insert(seq, token.clone());
        token
    }

    fn set_per_sequence_forced_outcome(&self, seq: u64, outcome: ScriptedWrite) {
        let queue: Vec<Attempt> = match outcome {
            ScriptedWrite::Ok => vec![Attempt::DelegateOk],
            ScriptedWrite::MaybeCommittedThenOk => {
                vec![Attempt::MaybeCommitted, Attempt::DelegateOk]
            }
        };
        self.scripts.lock().unwrap().insert(seq, queue.into());
    }

    fn set_commit_observer(&self, observer: Arc<dyn CommitObserver>) {
        *self.commit_observer.lock().unwrap() = Some(observer);
    }

    fn drain_captured_writes(&self) -> Vec<CapturedWrite> {
        std::mem::take(&mut *self.captured.lock().unwrap())
    }
}
