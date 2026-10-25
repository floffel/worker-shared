//! Property tests: invariants that must hold for *every* input, not just the
//! ones in the example tests.
//!
//! Each block names the property it pins.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::TestClock;
use proptest::prelude::*;

use worker_shared::config::parse_duration;
use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
use worker_shared::Error;

fn cache_with(clock: Arc<TestClock>, ttl: u64, margin: u64) -> SecretCache<Fixed> {
    SecretCache::new(
        Fixed,
        CacheConfig::new(Duration::from_secs(ttl))
            .with_refresh_margin(Duration::from_secs(margin))
            .with_jitter(Duration::ZERO),
    )
    .expect("valid config")
    .with_clock(clock)
}

struct Fixed;

#[async_trait::async_trait]
impl SecretFetcher for Fixed {
    async fn fetch(&self, id: &str) -> Result<String, Error> {
        Ok(format!("value-{id}"))
    }
}

proptest! {
    /// A cached value is served exactly while `age < ttl - margin`, and never
    /// after `age >= ttl`. This is the "never serve an expired secret" rule.
    #[test]
    fn cache_never_serves_a_value_past_its_lifetime(
        ttl in 2u64..600,
        margin in 0u64..2,
        check_at_percent in 0u32..200,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let clock = Arc::new(TestClock::new());
            let cache = cache_with(Arc::clone(&clock), ttl, margin);
            cache.get("db").await.expect("first fetch");

            let effective = Duration::from_secs(ttl - margin);
            let age = effective.mul_f64(f64::from(check_at_percent) / 100.0);
            clock.advance(age);

            let served = cache.peek("db");
            if age < effective {
                prop_assert!(served.is_some(), "fresh entry disappeared at {:?}", age);
            } else {
                prop_assert!(served.is_none(), "expired entry still served at {:?}", age);
            }
            Ok(())
        })?;
    }

    /// Duration parsing is total: no input panics, and a parsed duration is
    /// never negative.
    #[test]
    fn duration_parsing_is_total(raw in ".{0,40}") {
        if let Some(duration) = parse_duration(&raw) {
            prop_assert!(duration >= Duration::ZERO);
        }
    }

    /// `"<n><unit>"` always parses to the arithmetic value of that unit.
    #[test]
    fn duration_units_are_consistent(amount in 0u64..1_000_000u64, unit in 0usize..5) {
        let (suffix, millis) = match unit {
            0 => ("", 1_000u64),
            1 => ("ms", 1),
            2 => ("s", 1_000),
            3 => ("m", 60_000),
            _ => ("h", 3_600_000),
        };
        let raw = format!("{amount}{suffix}");
        let expected = amount.checked_mul(millis).map(Duration::from_millis);
        prop_assert_eq!(parse_duration(&raw), expected, "input {}", raw);
    }

    /// A batch is either rejected or within both service limits — there is no
    /// third state, so no dispatch can exceed 256 KiB or 10 entries.
    #[test]
    #[cfg(feature = "eventbridge")]
    fn batch_is_either_rejected_or_within_limits(sizes in prop::collection::vec(0usize..40_000, 0..20)) {
        use worker_shared::eventbridge::{Batch, Entry, MAX_BATCH_BYTES, MAX_ENTRIES};

        let mut batch = Batch::new();
        for (index, size) in sizes.iter().enumerate() {
            let entry = Entry::new("app", "Ping", serde_json::json!({"blob": "x".repeat(*size)}))
                .expect("entry");
            match batch.push(entry) {
                Ok(()) => {}
                Err(error) => {
                    prop_assert!(!error.is_retryable());
                    break;
                }
            }
            prop_assert!(batch.len() <= MAX_ENTRIES);
            prop_assert!(batch.total_bytes() <= MAX_BATCH_BYTES);
            prop_assert!(index < MAX_ENTRIES);
        }
        prop_assert!(batch.total_bytes() <= MAX_BATCH_BYTES);
    }

    /// `plan` is a partition: every entry appears exactly once, in order, and
    /// each batch is sendable.
    #[test]
    #[cfg(feature = "eventbridge")]
    fn plan_preserves_every_entry_in_order(count in 0usize..45) {
        use worker_shared::eventbridge::{plan, Entry, MAX_BATCH_BYTES, MAX_ENTRIES};

        let entries: Vec<Entry> = (0..count)
            .map(|index| Entry::new("app", "Ping", serde_json::json!({"i": index})).expect("entry"))
            .collect();
        let batches = plan(entries).expect("plan of small entries");

        let mut seen = Vec::new();
        for batch in &batches {
            prop_assert!(batch.len() <= MAX_ENTRIES);
            prop_assert!(batch.total_bytes() <= MAX_BATCH_BYTES);
            for entry in batch.entries() {
                let index = serde_json::from_str::<serde_json::Value>(entry.detail())
                    .expect("detail")["i"]
                    .as_u64()
                    .expect("number");
                seen.push(index);
            }
        }
        prop_assert_eq!(seen, (0..count as u64).collect::<Vec<_>>());
        prop_assert_eq!(batches.len(), count.div_ceil(MAX_ENTRIES));
    }

    /// Partial failures are reported in batch order, one per rejected entry,
    /// and accepted + failed always equals the batch size.
    #[test]
    #[cfg(feature = "eventbridge")]
    fn partial_failures_are_ordered_and_complete(outcomes in prop::collection::vec(any::<bool>(), 1..10)) {
        use worker_shared::eventbridge::{dispatch, Batch, Entry};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let mut batch = Batch::new();
            for index in 0..outcomes.len() {
                batch
                    .push(Entry::new("app", "Ping", serde_json::json!({"i": index})).expect("entry"))
                    .expect("fits");
            }

            let rejects: Vec<usize> = outcomes
                .iter()
                .enumerate()
                .filter(|(_, failed)| **failed)
                .map(|(index, _)| index)
                .collect();
            let dispatcher = common::RecordingDispatcher::rejecting(&rejects);
            let report = dispatch(&batch, &dispatcher).await.expect("dispatch");

            prop_assert_eq!(report.total(), outcomes.len());
            prop_assert_eq!(report.accepted, outcomes.len() - rejects.len());
            prop_assert_eq!(
                report.failed_indexes().collect::<Vec<_>>(),
                rejects.clone(),
                "failures must be ordered by position"
            );
            prop_assert_eq!(report.is_complete(), rejects.is_empty());
            Ok(())
        })?;
    }

    /// The batch failure report never contains a duplicate or an empty id,
    /// and preserves first-occurrence order, whatever the caller feeds in.
    #[test]
    #[cfg(feature = "sqs")]
    fn failure_report_is_deduplicated(ids in prop::collection::vec(".{0,4}", 0..30)) {
        use worker_shared::sqs::BatchItemFailures;

        let mut failures = BatchItemFailures::new();
        for id in &ids {
            failures.push(id.clone());
        }

        let mut expected: Vec<String> = Vec::new();
        for id in &ids {
            if !id.is_empty() && !expected.contains(id) {
                expected.push(id.clone());
            }
        }
        prop_assert_eq!(failures.as_slice(), expected.as_slice());
        prop_assert!(failures.as_slice().iter().all(|id| !id.is_empty()));
    }

    /// Envelope parsing is total for arbitrary JSON text: every input yields a
    /// value or a `Validation` error, never a panic.
    #[test]
    #[cfg(feature = "eventbridge")]
    fn envelope_parsing_is_total(raw in ".{0,120}") {
        use worker_shared::eventbridge::EventEnvelope;
        if let Err(error) = EventEnvelope::parse(&raw) {
            prop_assert_eq!(error.code(), "validation");
        }
    }

    /// SQS batch parsing is total for arbitrary JSON text.
    #[test]
    #[cfg(feature = "sqs")]
    fn sqs_parsing_is_total(raw in ".{0,120}") {
        use worker_shared::sqs::SqsBatchEvent;
        match SqsBatchEvent::parse(&raw) {
            Ok(event) => {
                prop_assert!(event.records.iter().all(|record| !record.message_id.is_empty()));
            }
            Err(error) => prop_assert_eq!(error.code(), "validation"),
        }
    }

    /// DDB Streams parsing is total for arbitrary JSON text.
    #[test]
    #[cfg(feature = "ddb")]
    fn ddb_parsing_is_total(raw in ".{0,120}") {
        use worker_shared::ddb_streams::DdbStreamEvent;
        match DdbStreamEvent::parse(&raw) {
            Ok(event) => prop_assert!(!event.records.is_empty()),
            Err(error) => prop_assert_eq!(error.code(), "validation"),
        }
    }
}
