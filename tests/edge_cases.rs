//! The explicit edge matrix: the just-barely-valid values on one side of every
//! boundary and the invalid ones on the other.
//!
//! Each test names the boundary it pins, because "we handle edge cases" is not
//! a claim anyone can check.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{gated, CountingFetcher, TestClock};

use worker_shared::secrets::{CacheConfig, SecretCache};
use worker_shared::{Error, SecretString};

fn config(ttl: u64, margin: u64) -> CacheConfig {
    CacheConfig::new(Duration::from_secs(ttl))
        .with_refresh_margin(Duration::from_secs(margin))
        .with_jitter(Duration::ZERO)
}

#[tokio::test]
async fn edge_empty_secret_fails_closed() {
    assert!(SecretString::new("").is_err(), "empty is not a value");

    let fetcher = Arc::new(CountingFetcher {
        empty: true,
        ..CountingFetcher::default()
    });
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");
    let error = cache.get("k").await.expect_err("empty secret");
    assert!(matches!(error, Error::EmptySecret { .. }));
    assert_eq!(cache.len(), 0, "a rejected value is never cached");
}

#[tokio::test]
async fn edge_whitespace_only_secret_is_a_value() {
    // Whitespace is legal for some providers (a passphrase could be " "), so it
    // is not rejected — only the empty string is.
    let secret = SecretString::new(" ").expect("whitespace is a value");
    assert_eq!(secret.len(), 1);
    assert!(!secret.is_empty());
}

#[tokio::test]
async fn edge_secret_rotation_mid_flight_never_resurrects_the_old_value() {
    let gated = gated();
    let cache =
        Arc::new(SecretCache::new(Arc::clone(&gated.fetcher), config(60, 10)).expect("cache"));
    let mut entered = gated.entered.clone();

    let handle = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move { cache.get("rotating").await })
    };
    while *entered.borrow() == 0 {
        entered.changed().await.expect("entered");
    }

    assert!(!cache.invalidate("rotating"));
    gated.release.send_replace(true);
    handle.await.expect("join").expect("value");
    assert!(cache.peek("rotating").is_none());

    // The next read fetches again and does store, because no invalidation
    // happened after it started.
    let fresh = cache.get("rotating").await.expect("second fetch");
    assert_eq!(fresh.expose_secret(), "value-2-for-rotating");
    assert!(cache.peek("rotating").is_some());
}

#[tokio::test]
async fn edge_throttled_api_is_retried_by_the_next_caller() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");
    fetcher.fail_with(Error::Throttled {
        service: "secretsmanager",
        retry_after: Some(Duration::from_millis(500)),
    });

    let error = cache.get("k").await.expect_err("throttled");
    assert!(error.is_retryable());
    assert!(!error.is_ambiguous());

    fetcher.recover();
    cache.get("k").await.expect("retry");
    assert_eq!(fetcher.calls(), 2);
    assert!(cache.peek("k").is_some());
}

#[test]
#[cfg(feature = "eventbridge")]
fn edge_oversized_eventbridge_batch_is_refused_before_it_is_sent() {
    use worker_shared::eventbridge::{Batch, Entry, MAX_BATCH_BYTES, MAX_ENTRIES};

    // Just under the byte limit: accepted.
    let mut under = Batch::new();
    under
        .push(
            Entry::new(
                "app",
                "Ping",
                serde_json::json!({"b": "x".repeat(200 * 1024)}),
            )
            .expect("entry"),
        )
        .expect("under the limit");
    assert!(under.total_bytes() < MAX_BATCH_BYTES);

    // Just over: rejected, and the batch is unchanged.
    let before = under.len();
    let error = under
        .push(
            Entry::new(
                "app",
                "Ping",
                serde_json::json!({"b": "x".repeat(100 * 1024)}),
            )
            .expect("entry"),
        )
        .expect_err("over the limit");
    assert!(matches!(error, Error::BatchTooLarge { .. }));
    assert_eq!(under.len(), before);

    // Exactly ten entries is fine, eleven is not.
    let mut exact = Batch::new();
    for _ in 0..MAX_ENTRIES {
        exact
            .push(Entry::new("a", "B", serde_json::json!({})).expect("entry"))
            .expect("fits");
    }
    assert_eq!(exact.len(), MAX_ENTRIES);
    assert!(exact
        .push(Entry::new("a", "B", serde_json::json!({})).expect("entry"))
        .is_err());
}

#[test]
#[cfg(feature = "sqs")]
fn edge_zero_sqs_messages_yield_an_empty_report() {
    use worker_shared::sqs::{BatchItemFailures, SqsBatchEvent};

    let event = SqsBatchEvent::parse(r#"{"Records":[]}"#).expect("zero messages");
    assert!(event.is_empty());
    assert_eq!(
        BatchItemFailures::new().to_json_string().expect("json"),
        r#"{"batchItemFailures":[]}"#
    );
}

#[test]
#[cfg(all(feature = "eventbridge", feature = "sqs", feature = "ddb"))]
fn edge_unicode_payloads_survive_every_parser() {
    use worker_shared::ddb_streams::DdbStreamEvent;
    use worker_shared::eventbridge::EventEnvelope;
    use worker_shared::sqs::SqsBatchEvent;

    let text = "Grüße, 世界, 🚀, \u{0}\u{7}";

    let envelope = EventEnvelope::from_value(serde_json::json!({
        "id": "ü-1",
        "source": text,
        "detail-type": text,
        "detail": {"text": text}
    }))
    .expect("envelope");
    assert_eq!(envelope.detail["text"], text);

    let sqs = SqsBatchEvent::from_value(serde_json::json!({
        "Records": [{"messageId": text, "body": text}]
    }))
    .expect("sqs");
    assert_eq!(sqs.records[0].body, text);

    let ddb = DdbStreamEvent::from_value(serde_json::json!({
        "Records": [{"eventID": text, "eventName": "INSERT",
                     "dynamodb": {"NewImage": {"k": {"S": text}}}}]
    }))
    .expect("ddb");
    assert_eq!(ddb.records[0].new_image["k"]["S"], text);
}

#[tokio::test]
async fn edge_very_long_secret_and_id_round_trip() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");
    let id = "a".repeat(4_096);
    let secret = cache.get(&id).await.expect("long id");
    assert_eq!(secret.len(), "value-for-".len() + 4_096);
    assert!(format!("{secret:?}").len() < 64, "Debug stays small");
}

#[tokio::test]
async fn edge_boundary_of_the_ttl_window() {
    let fetcher = CountingFetcher::new();
    let clock = Arc::new(TestClock::new());
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10))
        .expect("cache")
        .with_clock(clock.clone());

    cache.get("k").await.expect("first");
    // Exactly at the lifetime boundary the entry is already stale: `<` not `<=`.
    clock.advance(Duration::from_secs(50));
    assert!(cache.peek("k").is_none());
    cache.get("k").await.expect("refetch");
    assert_eq!(fetcher.calls(), 2);
}

#[tokio::test]
#[cfg(feature = "eventbridge")]
async fn edge_partial_failure_with_a_single_failing_entry() {
    use common::RecordingDispatcher;
    use worker_shared::eventbridge::{dispatch, Batch, Entry};

    let mut batch = Batch::new();
    batch
        .push(Entry::new("a", "B", serde_json::json!({"i": 0})).expect("entry"))
        .expect("push");
    batch
        .push(Entry::new("a", "B", serde_json::json!({"i": 1})).expect("entry"))
        .expect("push");

    let dispatcher = RecordingDispatcher::rejecting(&[0]);
    let report = dispatch(&batch, &dispatcher).await.expect("dispatch");
    assert_eq!(report.accepted, 1);
    assert_eq!(report.failed_indexes().collect::<Vec<_>>(), vec![0]);
    assert!(!report.is_complete());
}

#[tokio::test]
async fn edge_cancelled_work_reports_cancelled_not_a_timeout() {
    use worker_shared::handler::{run_bounded, Cancellation, Deadline};

    let cancellation = Cancellation::new();
    let _ = cancellation.cancel();
    let error = run_bounded(
        cancellation.token(),
        Deadline::after(Duration::from_secs(30)),
        async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        },
    )
    .await
    .expect_err("cancelled");
    assert_eq!(error, Error::Cancelled);
    assert!(!error.is_ambiguous(), "nothing was written");
}

#[tokio::test]
async fn edge_fetcher_error_is_returned_unchanged() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");
    fetcher.fail_with(Error::Secret {
        id: "k".to_string(),
        message: "not found".to_string(),
    });

    let error = cache.get("k").await.expect_err("backend error");
    assert_eq!(error.code(), "secret");
    assert!(error.is_retryable());
    assert!(cache.is_empty());
}
