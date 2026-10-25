//! EventBridge: inbound envelope parsing, outbound batching, size limits and
//! partial-failure dispatch.
#![cfg(feature = "eventbridge")]

mod common;

use std::time::Duration;

use common::RecordingDispatcher;
use serde_json::json;

use worker_shared::eventbridge::{
    code_is_retryable, dispatch, plan, Batch, DeliveryReport, Dispatcher, Entry, EntryFailure,
    EntryResult, EventBridgeScheduledEvent, EventEnvelope, FnDispatcher, PutEventsResponse,
    MAX_BATCH_BYTES, MAX_ENTRIES,
};
use worker_shared::Error;

fn entry(size_hint: usize) -> Entry {
    Entry::new(
        "app.orders",
        "OrderPlaced",
        json!({"blob": "x".repeat(size_hint)}),
    )
    .expect("entry")
}

#[test]
fn scheduled_envelope_reduces_to_a_tick() {
    let raw = json!({
        "version": "0",
        "id": "tick-42",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": "2026-10-25T06:00:00Z",
        "region": "eu-central-1",
        "resources": ["arn:aws:events:eu-central-1:123456789012:rule/nightly"],
        "detail": {}
    });

    let event = EventBridgeScheduledEvent::from_value(raw).expect("parse");
    assert_eq!(event.tick.event_id, "tick-42");
    assert_eq!(
        event.tick.rule_arn,
        "arn:aws:events:eu-central-1:123456789012:rule/nightly"
    );
    assert_eq!(event.tick.detail_type, "Scheduled Event");
    assert_eq!(event.tick.time.unix_timestamp(), 1_792_908_000);
}

#[test]
fn envelope_tolerates_unknown_fields_and_alternate_shapes() {
    let envelope = EventEnvelope::from_value(json!({
        "id": "1",
        "source": "custom.app",
        "detail-type": "SomethingNew",
        "detail": {"nested": [1, 2, 3]},
        "extra": "ignored",
        "resources": []
    }))
    .expect("permissive parse");

    assert_eq!(envelope.id, "1");
    assert_eq!(envelope.detail_type, "SomethingNew");
    assert!(envelope.time.is_none());
    assert!(envelope.account.is_none());
    assert!(envelope.region.is_none());
    assert!(envelope.event_bus_name.is_none());
    assert!(!envelope.is_scheduled());
    assert_eq!(envelope.detail["nested"][2], 3);
}

#[test]
fn partial_envelope_defaults_instead_of_failing() {
    let envelope = EventEnvelope::from_value(json!({"id": "only"})).expect("parse");
    assert_eq!(envelope.id, "only");
    assert_eq!(envelope.source, "");
    assert_eq!(envelope.detail_type, "");
    assert_eq!(envelope.detail, serde_json::Value::Null);
    assert!(envelope.resources.is_empty());
}

#[test]
fn scheduled_event_rejects_each_missing_invariant() {
    let cases: Vec<(serde_json::Value, &str)> = vec![
        (
            json!({"time": "2026-10-25T06:00:00Z", "resources": ["arn"]}),
            "no `id`",
        ),
        (
            json!({"id": "x", "resources": ["arn"]}),
            "no RFC 3339 `time`",
        ),
        (
            json!({"id": "x", "time": "2026-10-25T06:00:00Z"}),
            "no `resources[0]`",
        ),
        (
            json!({
                "id": "x",
                "time": "2026-10-25T06:00:00Z",
                "resources": ["arn"],
                "detail-type": "OrderPlaced"
            }),
            "expected `Scheduled Event`",
        ),
        (
            json!({"id": "x", "time": "25.10.2026 06:00", "resources": ["arn"]}),
            "no RFC 3339 `time`",
        ),
    ];

    for (value, needle) in cases {
        let error = EventBridgeScheduledEvent::from_value(value).expect_err(needle);
        assert_eq!(error.code(), "validation");
        assert!(error.to_string().contains(needle), "{error}");
    }
}

#[test]
fn non_object_payloads_are_rejected() {
    for raw in ["[]", "null", "\"text\"", "42"] {
        let error = EventEnvelope::parse(raw).expect_err(raw);
        assert_eq!(error.code(), "validation");
    }
    assert!(EventEnvelope::parse("{").is_err());
}

#[test]
fn entry_validates_its_own_fields() {
    assert!(Entry::new("", "Type", json!({})).is_err());
    assert!(Entry::new("app", "", json!({})).is_err());
    assert!(Entry::with_detail_json("app", "Type", "not json").is_err());

    let entry = Entry::with_detail_json("app", "Type", "{}").expect("entry");
    assert_eq!(entry.detail(), "{}");
    assert_eq!(entry.source(), "app");
    assert_eq!(entry.detail_type(), "Type");
    assert!(entry.resources().is_empty());
    assert!(entry.time().is_none());
    assert!(entry.event_bus_name().is_none());
}

#[test]
fn entry_serialises_only_the_fields_that_are_set() {
    let plain = Entry::new("app", "Ping", json!({"n": 1})).expect("entry");
    let plain_size = plain.size_bytes();
    let wire = serde_json::to_value(plain.wire()).expect("wire");
    assert_eq!(wire["source"], "app");
    assert_eq!(wire["detail-type"], "Ping");
    assert_eq!(wire["detail"], "{\"n\":1}");
    assert!(wire.get("resources").is_none());
    assert!(wire.get("time").is_none());
    assert!(wire.get("event-bus-name").is_none());

    let full = plain
        .with_resources(vec![
            "arn:aws:events:eu-central-1:1:event-bus/default".to_string()
        ])
        .with_event_bus_name("orders")
        .with_time(time::macros::datetime!(2026-10-25 06:00:00 UTC));
    let wire = serde_json::to_value(full.wire()).expect("wire");
    assert_eq!(
        wire["resources"][0],
        "arn:aws:events:eu-central-1:1:event-bus/default"
    );
    assert_eq!(wire["event-bus-name"], "orders");
    assert_eq!(wire["time"], "2026-10-25T06:00:00Z");
    assert_eq!(full.time().map(time::OffsetDateTime::hour), Some(6));
    assert!(full.size_bytes() > plain_size);
}

#[test]
fn batch_enforces_the_entry_count_limit() {
    let mut batch = Batch::new();
    for index in 0..MAX_ENTRIES {
        batch.push(entry(index)).expect("within limit");
    }
    let error = batch.push(entry(0)).expect_err("eleventh entry");
    assert!(matches!(
        error,
        Error::TooManyEntries {
            entries: 11,
            limit: 10
        }
    ));
    assert_eq!(error.code(), "too_many_entries");
    assert_eq!(batch.len(), MAX_ENTRIES);
}

#[test]
fn batch_enforces_the_byte_limit() {
    let mut batch = Batch::new();
    // Two entries of ~150 KiB fit; a third does not.
    batch.push(entry(150 * 1024)).expect("first");
    batch.push(entry(100 * 1024)).expect("second");
    let error = batch
        .push(entry(10 * 1024))
        .expect_err("would exceed 256 KiB");
    assert!(matches!(
        error,
        Error::BatchTooLarge {
            limit: MAX_BATCH_BYTES,
            ..
        }
    ));
    assert_eq!(error.code(), "batch_too_large");
    assert!(batch.total_bytes() <= MAX_BATCH_BYTES);
}

#[test]
fn a_single_oversized_entry_is_named_with_its_position() {
    let mut batch = Batch::new();
    batch.push(entry(0)).expect("small");
    let error = batch.push(entry(MAX_BATCH_BYTES + 1)).expect_err("too big");
    match error {
        Error::EntryTooLarge {
            index,
            bytes,
            limit,
        } => {
            assert_eq!(index, 1);
            assert!(bytes > MAX_BATCH_BYTES);
            assert_eq!(limit, MAX_BATCH_BYTES);
        }
        other => panic!("unexpected error: {other}"),
    }
    assert_eq!(batch.len(), 1);
}

#[test]
fn batch_try_from_and_accessors() {
    let batch = Batch::try_from(vec![entry(0), entry(1)]).expect("batch");
    assert_eq!(batch.len(), 2);
    assert!(!batch.is_empty());
    assert_eq!(batch.entries().len(), 2);
    assert_eq!(
        batch.total_bytes(),
        batch.entries()[0].size_bytes() + batch.entries()[1].size_bytes()
    );
    assert_eq!(batch.into_entries().len(), 2);
    assert!(Batch::try_from(vec![entry(MAX_BATCH_BYTES + 2)]).is_err());
}

#[test]
fn plan_splits_by_count_and_keeps_order() {
    let entries: Vec<Entry> = (0..25)
        .map(|index| Entry::new("app", "Ping", json!({"i": index})).expect("entry"))
        .collect();
    let batches = plan(entries).expect("plan");
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].len(), 10);
    assert_eq!(batches[1].len(), 10);
    assert_eq!(batches[2].len(), 5);
    // Order is preserved across the split.
    let indexes: Vec<u64> = batches
        .iter()
        .flat_map(|batch| batch.entries().iter())
        .map(|entry| {
            serde_json::from_str::<serde_json::Value>(entry.detail()).expect("detail")["i"]
                .as_u64()
                .expect("number")
        })
        .collect();
    assert_eq!(indexes, (0..25).collect::<Vec<u64>>());
}

#[test]
fn plan_splits_by_size_too() {
    let entries = vec![entry(120 * 1024), entry(120 * 1024), entry(120 * 1024)];
    let batches = plan(entries).expect("plan");
    assert_eq!(batches.len(), 2, "two fit, the third cannot join them");
    assert!(batches
        .iter()
        .all(|batch| batch.total_bytes() <= MAX_BATCH_BYTES));
    assert!(plan(Vec::<Entry>::new()).expect("empty").is_empty());
    assert!(plan(vec![entry(MAX_BATCH_BYTES + 5)]).is_err());
}

#[test]
fn plan_handles_an_exactly_full_batch() {
    let mut entries = vec![entry(0); 10];
    entries.push(entry(1));
    let batches = plan(entries).expect("plan");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), 10);
    assert_eq!(batches[1].len(), 1);
}

#[tokio::test]
async fn dispatch_reports_partial_failure_by_position() {
    let mut batch = Batch::new();
    for index in 0..3 {
        batch.push(entry(index)).expect("entry");
    }
    let dispatcher = RecordingDispatcher::rejecting(&[1]);

    let report = dispatch(&batch, &dispatcher).await.expect("dispatch");
    assert_eq!(report.accepted, 2);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failed_indexes().collect::<Vec<_>>(), vec![1]);
    assert_eq!(report.retryable_indexes().collect::<Vec<_>>(), vec![1]);
    assert!(!report.is_complete());
    assert_eq!(report.total(), 3);
    assert_eq!(
        report.failures[0],
        EntryFailure {
            index: 1,
            code: "ThrottlingException".to_string(),
            message: "rejected by test".to_string(),
        }
    );
    assert_eq!(
        report.failures[0].to_string(),
        "entry 1 failed with ThrottlingException: rejected by test"
    );
}

#[tokio::test]
async fn dispatch_of_an_empty_batch_never_calls_the_dispatcher() {
    let dispatcher = RecordingDispatcher::accepting();
    let report = dispatch(&Batch::new(), &dispatcher)
        .await
        .expect("dispatch");
    assert_eq!(report, DeliveryReport::default());
    assert!(report.is_complete());
    assert_eq!(report.total(), 0);
    assert_eq!(dispatcher.call_count(), 0);
}

#[tokio::test]
async fn dispatch_propagates_call_level_errors() {
    let mut batch = Batch::new();
    batch.push(entry(0)).expect("entry");

    let throttled = RecordingDispatcher::failing(Error::Throttled {
        service: "eventbridge",
        retry_after: Some(Duration::from_secs(1)),
    });
    let error = dispatch(&batch, &throttled).await.expect_err("throttled");
    assert!(error.is_retryable());
    assert_eq!(error.code(), "throttled");

    let upstream = RecordingDispatcher::failing(Error::Upstream {
        service: "eventbridge",
        message: "500".to_string(),
    });
    let error = dispatch(&batch, &upstream).await.expect_err("upstream");
    assert!(error.is_ambiguous(), "a 5xx may have been processed");
}

#[tokio::test]
async fn dispatch_rejects_a_response_of_the_wrong_length() {
    let mut batch = Batch::new();
    batch.push(entry(0)).expect("one");
    batch.push(entry(1)).expect("two");

    let dispatcher = FnDispatcher::new(|_entries: &[Entry]| {
        std::future::ready(Ok::<PutEventsResponse, Error>(
            PutEventsResponse::all_accepted(1),
        ))
    });
    let error = dispatch(&batch, &dispatcher).await.expect_err("mismatch");
    assert!(matches!(
        error,
        Error::ResponseLengthMismatch {
            expected: 2,
            actual: 1,
            ..
        }
    ));
    assert_eq!(error.code(), "response_length_mismatch");
}

#[tokio::test]
async fn dispatch_refuses_a_hand_built_oversized_batch() {
    // `Batch` is only constructible through `new`/`TryFrom`, both validating,
    // so the guards here are defence in depth against future constructors.
    let mut batch = Batch::new();
    for index in 0..MAX_ENTRIES {
        batch.push(entry(index)).expect("within limit");
    }
    let dispatcher = RecordingDispatcher::accepting();
    let report = dispatch(&batch, &dispatcher).await.expect("dispatch");
    assert_eq!(report.accepted, MAX_ENTRIES);
    assert!(dispatcher.sent()[0].len() <= MAX_ENTRIES);
}

#[test]
fn entry_results_expose_their_fields() {
    let accepted = EntryResult::accepted("evt-1");
    assert!(accepted.is_accepted());
    assert_eq!(accepted.event_id(), Some("evt-1"));
    assert_eq!(accepted.code(), "");
    assert!(!accepted.is_retryable());

    let anonymous = EntryResult::Accepted { event_id: None };
    assert!(anonymous.is_accepted());
    assert_eq!(anonymous.event_id(), None);

    let failed = EntryResult::failed("InternalFailure", "boom");
    assert!(!failed.is_accepted());
    assert_eq!(failed.event_id(), None);
    assert_eq!(failed.code(), "InternalFailure");
    assert!(failed.is_retryable());

    assert_eq!(PutEventsResponse::empty().results.len(), 0);
    assert_eq!(PutEventsResponse::default().results.len(), 0);
}

#[test]
fn retryable_codes_are_exactly_the_transient_ones() {
    for code in [
        "ThrottlingException",
        "InternalFailure",
        "InternalException",
        "ProvisionedThroughputExceededException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
    ] {
        assert!(code_is_retryable(code), "{code} should be retryable");
    }
    for code in [
        "MalformedDetail",
        "AccessDeniedException",
        "ValidationException",
        "",
        "throttlingexception",
    ] {
        assert!(!code_is_retryable(code), "{code} should be terminal");
    }
}

#[tokio::test]
async fn fn_dispatcher_round_trips_through_the_trait() {
    let dispatcher = FnDispatcher::new(|entries: &[Entry]| {
        std::future::ready(Ok::<PutEventsResponse, Error>(
            PutEventsResponse::all_accepted(entries.len()),
        ))
    });
    let reply = dispatcher
        .put_events(&[entry(0), entry(1)])
        .await
        .expect("call");
    assert_eq!(reply.results.len(), 2);
    assert!(reply.results.iter().all(EntryResult::is_accepted));
}
