//! Per-source byte budget (Phase 6 design §Public API Surface
//! > Per-source byte budget).
//!
//! Custom actor — **not** `tokio::sync::Semaphore`. Reservations are
//! divisible (`reconcile` adjusts up or down) so admission can
//! reserve pessimistically and decode can reconcile to actual
//! post-decode bytes. Semaphore permits cannot be split, so the
//! count-axis backpressure stays on `Semaphore`; the byte axis lives
//! here.
//!
//! Contract summary:
//!
//! - `SourceByteBudget::reserve(bytes)` parks when
//!   `in_flight + bytes > capacity`. **The only blocking
//!   operation in the budget protocol.**
//! - `ByteReservation::reconcile(actual)` is **synchronous and
//!   always succeeds**. Grow does an atomic add that may push
//!   `in_flight` above `capacity` — intentional brief
//!   over-subscription so an already-admitted unit cannot deadlock
//!   waiting for the budget to drain (admission is the only thing
//!   that drains the budget via downstream commit). Shrink does an
//!   atomic sub and `notify_waiters` so any parked
//!   `reserve` re-evaluates capacity.
//! - `ByteReservation::drop` releases its `held` bytes and notifies
//!   waiters.
//!
//! The over-subscription window is bounded by an explicit
//! oversize-fault gate in the decode worker (Phase 6 design
//! §Algorithms > Per-Source Decode Workers); the
//! `SourceBackpressureOptions::oversize_fault_multiplier` knob caps
//! the worst-case overage.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

use crate::source::SourceId;

/// Per-source in-flight byte budget.
pub struct SourceByteBudget {
    source: SourceId,
    capacity: u64,
    in_flight: AtomicU64,
    notify: Notify,
}

impl SourceByteBudget {
    pub fn new(source: SourceId, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            source,
            capacity,
            in_flight: AtomicU64::new(0),
            notify: Notify::new(),
        })
    }

    pub fn source(&self) -> &SourceId {
        &self.source
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Park until enough capacity is available, then atomically
    /// claim `bytes`. Cancellation-safe: if the future is dropped
    /// before the claim succeeds, no bytes are held.
    ///
    /// A zero-byte reservation is admitted unconditionally; this
    /// matches the design contract that `reconcile` may shrink an
    /// existing reservation to zero without unwinding the holder's
    /// claim on the per-source batch slot.
    pub async fn reserve(self: &Arc<Self>, bytes: u64) -> ByteReservation {
        if bytes == 0 {
            return ByteReservation {
                budget: Arc::clone(self),
                held: 0,
                stage: None,
            };
        }

        loop {
            // Subscribe to the notifier *before* the load so the
            // wait below cannot miss a notify that fires between
            // the load and the park. Tokio's `Notify::notified()`
            // returns a future that is "armed" by the call itself.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let current = self.in_flight.load(Ordering::SeqCst);
            if current.saturating_add(bytes) <= self.capacity
                && self
                    .in_flight
                    .compare_exchange(current, current + bytes, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return ByteReservation {
                    budget: Arc::clone(self),
                    held: bytes,
                    stage: None,
                };
            }
            notified.await;
        }
    }

    /// Snapshot of currently in-flight bytes. Used by the
    /// `runtime_stage_inflight_bytes{stage,source}` gauge.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst)
    }
}

/// Drop-on-end byte reservation. `held()` reflects the bytes
/// currently accounted to this reservation against
/// `budget.in_flight`.
///
/// Optionally carries a "stage" atomic that mirrors `held` —
/// `attach_stage(s)` adds `held` to `s` and detaches the prior
/// stage (subtracts `held` from the previous atomic if any); the
/// reconcile path adjusts the attached stage by the same delta it
/// applies to `budget.in_flight`; `Drop` decrements the attached
/// stage before clearing the budget. Used by Phase 6.x §1.4 to
/// drive per-stage `runtime_stage_inflight_bytes{stage=...}`
/// gauges without manual decrement bookkeeping on every error /
/// drop path.
pub struct ByteReservation {
    budget: Arc<SourceByteBudget>,
    held: u64,
    stage: Option<Arc<AtomicU64>>,
}

impl ByteReservation {
    pub fn held(&self) -> u64 {
        self.held
    }

    pub fn budget(&self) -> &Arc<SourceByteBudget> {
        &self.budget
    }

    /// Attach the reservation to `stage`, which gets `held` added
    /// to it. If a prior stage was attached, it's detached first
    /// (its atomic gets `held` subtracted). Idempotent — attaching
    /// the same atomic twice is a no-op net delta.
    pub fn attach_stage(&mut self, stage: Arc<AtomicU64>) {
        if let Some(prev) = self.stage.take()
            && self.held > 0
        {
            prev.fetch_sub(self.held, Ordering::SeqCst);
        }
        if self.held > 0 {
            stage.fetch_add(self.held, Ordering::SeqCst);
        }
        self.stage = Some(stage);
    }

    /// Adjust the reservation to `actual_bytes`. **Synchronous,
    /// non-blocking, always succeeds.**
    ///
    /// Two cases:
    /// - `actual_bytes <= self.held`: atomic sub on
    ///   `budget.in_flight` for the difference and
    ///   `notify_waiters` so any parked `reserve` re-evaluates
    ///   capacity.
    /// - `actual_bytes > self.held`: atomic add on
    ///   `budget.in_flight` for the difference. **This may take
    ///   `in_flight` above `capacity`** — intentional brief
    ///   over-subscription. Reconcile-grow never parks because
    ///   parking a decode worker can deadlock against admission
    ///   (admission is the only path that drains the budget via
    ///   downstream sink commit). Admission's `reserve` is the
    ///   only gate.
    ///
    /// The attached stage atomic (if any) tracks the delta on
    /// each call so the per-stage gauge follows the reconcile.
    pub fn reconcile(&mut self, actual_bytes: u64) {
        if actual_bytes == self.held {
            return;
        }
        if actual_bytes < self.held {
            let delta = self.held - actual_bytes;
            self.budget.in_flight.fetch_sub(delta, Ordering::SeqCst);
            if let Some(stage) = self.stage.as_ref() {
                stage.fetch_sub(delta, Ordering::SeqCst);
            }
            self.held = actual_bytes;
            self.budget.notify.notify_waiters();
        } else {
            let delta = actual_bytes - self.held;
            // Reconcile-grow is unconditional. May exceed
            // `capacity`. Bounded by the oversize-fault gate in
            // the decode worker.
            self.budget.in_flight.fetch_add(delta, Ordering::SeqCst);
            if let Some(stage) = self.stage.as_ref() {
                stage.fetch_add(delta, Ordering::SeqCst);
            }
            self.held = actual_bytes;
        }
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        if self.held > 0 {
            if let Some(stage) = self.stage.take() {
                stage.fetch_sub(self.held, Ordering::SeqCst);
            }
            self.budget.in_flight.fetch_sub(self.held, Ordering::SeqCst);
            self.budget.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn budget(capacity: u64) -> Arc<SourceByteBudget> {
        SourceByteBudget::new(SourceId::from("test"), capacity)
    }

    #[tokio::test]
    async fn budget_reserve_blocks_when_full() {
        let b = budget(100);
        let r1 = b.reserve(100).await;
        assert_eq!(b.in_flight(), 100);

        // The waiter holds onto its reservation by communicating
        // via a oneshot — that way the outer test can observe the
        // budget state while the waiter is still holding its
        // claim. Otherwise the spawned task's drop would race the
        // outer assertion.
        let b2 = Arc::clone(&b);
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let waiter = tokio::spawn(async move {
            let r = b2.reserve(1).await;
            assert_eq!(r.held(), 1);
            held_tx.send(()).expect("send held");
            release_rx.await.expect("await release");
            drop(r);
        });

        // Park briefly so the waiter has time to reach the await.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "second reserve must remain parked while budget is full",
        );

        drop(r1);

        tokio::time::timeout(Duration::from_secs(2), held_rx)
            .await
            .expect("waiter should claim within 2s")
            .expect("waiter dropped channel");
        assert_eq!(b.in_flight(), 1);

        release_tx.send(()).unwrap();
        waiter.await.expect("task panicked");
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn budget_reconcile_shrink_releases_excess() {
        let b = budget(200);
        let mut r = b.reserve(100).await;
        assert_eq!(b.in_flight(), 100);
        r.reconcile(60);
        assert_eq!(b.in_flight(), 60);
        assert_eq!(r.held(), 60);
    }

    #[tokio::test]
    async fn budget_reconcile_grow_oversubscribes_without_blocking() {
        let b = budget(100);
        let _r1 = b.reserve(80).await;
        let mut r2 = b.reserve(20).await;
        assert_eq!(b.in_flight(), 100);

        // Grow may temporarily exceed capacity. `reconcile` is
        // synchronous and must return immediately.
        let before = std::time::Instant::now();
        r2.reconcile(40);
        assert!(
            before.elapsed() < Duration::from_millis(10),
            "reconcile-grow must not block",
        );
        assert_eq!(b.in_flight(), 120);
        assert_eq!(r2.held(), 40);

        // A parked admission attempt does NOT wake from reconcile-grow
        // alone; only shrink or drop wakes waiters.
        let b3 = Arc::clone(&b);
        let waiter = tokio::spawn(async move {
            let _r = b3.reserve(1).await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "reconcile-grow must not wake a parked reserve",
        );
        waiter.abort();
    }

    #[tokio::test]
    async fn budget_drop_after_oversubscription_unwinds_atomically() {
        let b = budget(100);
        let r1 = b.reserve(80).await;
        let mut r2 = b.reserve(20).await;
        r2.reconcile(40); // in_flight = 120
        drop(r2); // in_flight = 80
        assert_eq!(b.in_flight(), 80);

        // Park-then-claim with a held reservation observable to the
        // outer test.
        let b2 = Arc::clone(&b);
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let waiter = tokio::spawn(async move {
            let r = b2.reserve(20).await;
            assert_eq!(r.held(), 20);
            held_tx.send(()).expect("send held");
            release_rx.await.expect("await release");
            drop(r);
        });
        tokio::time::timeout(Duration::from_secs(2), held_rx)
            .await
            .expect("waiter should claim within 2s")
            .expect("waiter dropped channel");
        // Capacity is 100, in_flight was 80, so 20 fits — and the
        // waiter is now actually holding its claim.
        assert_eq!(b.in_flight(), 100);

        release_tx.send(()).unwrap();
        waiter.await.expect("task panicked");
        drop(r1);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn budget_drop_releases_held_bytes_and_notifies() {
        let b = budget(100);
        let r = b.reserve(50).await;
        assert_eq!(b.in_flight(), 50);

        let b2 = Arc::clone(&b);
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let waiter = tokio::spawn(async move {
            let r = b2.reserve(50).await;
            assert_eq!(r.held(), 50);
            held_tx.send(()).expect("send held");
            release_rx.await.expect("await release");
            drop(r);
        });

        // Saturate the budget so the waiter parks.
        let r2 = b.reserve(50).await;
        assert_eq!(b.in_flight(), 100);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        drop(r);
        tokio::time::timeout(Duration::from_secs(2), held_rx)
            .await
            .expect("waiter should claim within 2s")
            .expect("waiter dropped channel");
        assert_eq!(b.in_flight(), 100);

        release_tx.send(()).unwrap();
        waiter.await.expect("task panicked");
        drop(r2);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn attach_stage_moves_held_bytes_between_stages() {
        let b = budget(200);
        let mut r = b.reserve(100).await;
        let stage_a = Arc::new(AtomicU64::new(0));
        let stage_b = Arc::new(AtomicU64::new(0));

        r.attach_stage(Arc::clone(&stage_a));
        assert_eq!(stage_a.load(Ordering::SeqCst), 100);
        assert_eq!(stage_b.load(Ordering::SeqCst), 0);

        r.attach_stage(Arc::clone(&stage_b));
        assert_eq!(stage_a.load(Ordering::SeqCst), 0);
        assert_eq!(stage_b.load(Ordering::SeqCst), 100);

        // reconcile-shrink follows the attached stage.
        r.reconcile(60);
        assert_eq!(stage_b.load(Ordering::SeqCst), 60);
        assert_eq!(b.in_flight(), 60);

        // reconcile-grow likewise.
        r.reconcile(150);
        assert_eq!(stage_b.load(Ordering::SeqCst), 150);
        assert_eq!(b.in_flight(), 150);

        // Drop decrements the attached stage and the budget.
        drop(r);
        assert_eq!(stage_a.load(Ordering::SeqCst), 0);
        assert_eq!(stage_b.load(Ordering::SeqCst), 0);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn budget_zero_reservation_does_not_block_or_account() {
        let b = budget(100);
        let _full = b.reserve(100).await;
        // Zero-byte reserve must admit even when the budget is full.
        let r = b.reserve(0).await;
        assert_eq!(r.held(), 0);
        assert_eq!(b.in_flight(), 100);
    }
}
