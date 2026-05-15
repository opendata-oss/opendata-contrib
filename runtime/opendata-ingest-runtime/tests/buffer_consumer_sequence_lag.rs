//! Pins `BufferSource::pending_count()` — the value the runtime
//! feeds into the `buffer_consumer_sequence_lag` gauge consumed by
//! the Phase 8 cell-bench bottleneck classifier. The gauge itself is
//! emitted from inside `run_per_source_actor`'s completion arm; this
//! test exercises the source-side method that supplies the value, so
//! a regression in the underlying buffer accounting fails here
//! deterministically rather than presenting as a silent zero gauge in
//! production.

#[path = "support/mod.rs"]
mod support;

use bytes::Bytes;
use opendata_ingest_runtime::source::SourceBudget;
use support::{in_memory_buffer_source, logs_envelope};

fn unbounded_budget() -> SourceBudget {
    SourceBudget {
        bytes_remaining: u64::MAX,
        batches_remaining: u32::MAX,
    }
}

#[tokio::test]
async fn pending_count_reflects_unacked_batches_through_lifecycle() {
    let mut fx = in_memory_buffer_source("ingest/test/lag/manifest", "ingest/test/lag/data").await;

    // Empty queue → no entries yet.
    assert_eq!(
        fx.source.pending_count(),
        0,
        "fresh source over empty manifest should report 0 pending"
    );

    // Produce 5 batches.
    let batch_count = 5u64;
    for i in 0..batch_count {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    // The consumer's len() reflects manifest state observed at the
    // last read; force a read by calling next_descriptors.
    let descriptors = fx
        .source
        .next_descriptors(64, unbounded_budget())
        .await
        .expect("next_descriptors");
    assert_eq!(descriptors.len(), batch_count as usize);
    assert_eq!(
        fx.source.pending_count(),
        batch_count as usize,
        "after producer flushes {batch_count} batches and the consumer reads them, pending_count must equal the unacked count"
    );

    // Ack through the first 3. ack_through alone doesn't durably
    // dequeue under the runtime's AckFlushPolicy contract; flush_acks
    // makes the buffer-side dequeue happen.
    let ack_seq = descriptors[2].sequence;
    fx.source.ack_through(ack_seq).await.expect("ack_through");
    fx.source.flush_acks().await.expect("flush_acks");

    // Force a fresh manifest read so the consumer's len() updates.
    let _ = fx
        .source
        .next_descriptors(64, unbounded_budget())
        .await
        .expect("next_descriptors after ack");
    assert_eq!(
        fx.source.pending_count(),
        (batch_count - 3) as usize,
        "after acking through 3 of {batch_count}, pending_count must drop to {} ",
        batch_count - 3,
    );

    // Ack through the remaining tail.
    let final_seq = descriptors.last().unwrap().sequence;
    fx.source
        .ack_through(final_seq)
        .await
        .expect("ack_through tail");
    fx.source.flush_acks().await.expect("flush_acks tail");
    let _ = fx
        .source
        .next_descriptors(64, unbounded_budget())
        .await
        .expect("next_descriptors after final ack");
    assert_eq!(
        fx.source.pending_count(),
        0,
        "fully drained queue reports 0 pending"
    );
}
