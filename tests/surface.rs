//! Completeness sweep: every public item, accessor and trait impl gets called
//! at least once here.
//!
//! The other files prove behaviour; this one keeps the coverage gate honest — a
//! public method nobody calls is a public method nobody has tested. It doubles
//! as the audit of the error surface: every variant's `code()`,
//! `is_retryable()`, `is_ambiguous()` and `Display` is asserted, so adding a
//! variant without deciding those three questions fails the build.

mod common;

use std::time::Duration;

use worker_shared::{ConfigError, Error};

/// One row per `Error` variant: this is the audit that a new variant cannot
/// be added without deciding its code, retryability and ambiguity.
fn error_table() -> Vec<(Error, &'static str, bool, bool)> {
    vec![
        (
            Error::Config(ConfigError::Missing { key: "K".into() }),
            "config",
            false,
            false,
        ),
        (Error::Validation("v".into()), "validation", false, false),
        (
            Error::Secret {
                id: "s".into(),
                message: "m".into(),
            },
            "secret",
            true,
            false,
        ),
        (
            Error::EmptySecret { id: "s".into() },
            "empty_secret",
            false,
            false,
        ),
        (
            Error::TooManyEntries {
                entries: 11,
                limit: 10,
            },
            "too_many_entries",
            false,
            false,
        ),
        (
            Error::BatchTooLarge {
                bytes: 300_000,
                limit: 262_144,
            },
            "batch_too_large",
            false,
            false,
        ),
        (
            Error::EntryTooLarge {
                index: 0,
                bytes: 300_000,
                limit: 262_144,
            },
            "entry_too_large",
            false,
            false,
        ),
        (
            Error::Throttled {
                service: "eventbridge",
                retry_after: Some(Duration::from_secs(1)),
            },
            "throttled",
            true,
            false,
        ),
        (
            Error::Upstream {
                service: "sqs",
                message: "reset".into(),
            },
            "upstream",
            true,
            true,
        ),
        (
            Error::ResponseLengthMismatch {
                service: "eventbridge",
                expected: 2,
                actual: 1,
            },
            "response_length_mismatch",
            false,
            false,
        ),
        (Error::DeadlineExceeded, "deadline_exceeded", false, true),
        (Error::Cancelled, "cancelled", false, false),
        (Error::Internal("bug".into()), "internal", false, false),
    ]
}

#[test]
fn every_error_variant_answers_the_three_questions() {
    for (error, code, retryable, ambiguous) in error_table() {
        assert_eq!(error.code(), code, "{error}");
        assert_eq!(error.is_retryable(), retryable, "{error}");
        assert_eq!(error.is_ambiguous(), ambiguous, "{error}");
        assert!(!error.to_string().is_empty(), "{code} has no message");
        // Errors are cheap to clone and compare, which callers rely on.
        assert_eq!(error.clone(), error);
        assert!(!format!("{error:?}").is_empty());
    }
}

#[test]
fn error_messages_name_the_offending_values() {
    assert_eq!(
        Error::Throttled {
            service: "eventbridge",
            retry_after: None
        }
        .to_string(),
        "upstream eventbridge rejected the call as throttled"
    );
    assert_eq!(
        Error::TooManyEntries {
            entries: 11,
            limit: 10
        }
        .to_string(),
        "batch of 11 entries exceeds the 10-entry limit"
    );
    assert_eq!(
        Error::BatchTooLarge {
            bytes: 300_000,
            limit: 262_144
        }
        .to_string(),
        "batch of 300000 bytes exceeds the 262144-byte limit"
    );
    assert_eq!(
        Error::EntryTooLarge {
            index: 3,
            bytes: 300_000,
            limit: 262_144
        }
        .to_string(),
        "entry 3 of 300000 bytes exceeds the 262144-byte limit"
    );
    assert_eq!(
        Error::ResponseLengthMismatch {
            service: "eventbridge",
            expected: 2,
            actual: 1
        }
        .to_string(),
        "upstream eventbridge returned 1 results for 2 entries"
    );
    assert_eq!(
        Error::Secret {
            id: "prod/db".into(),
            message: "not found".into()
        }
        .to_string(),
        "secret `prod/db`: not found"
    );
    assert_eq!(
        Error::EmptySecret {
            id: "prod/db".into()
        }
        .to_string(),
        "secret `prod/db` is empty"
    );
    assert_eq!(Error::Cancelled.to_string(), "cancelled");
    assert_eq!(Error::DeadlineExceeded.to_string(), "deadline exceeded");
    assert_eq!(
        Error::Internal("bug".into()).to_string(),
        "internal error: bug"
    );
    assert_eq!(
        Error::Validation("bad".into()).to_string(),
        "invalid payload: bad"
    );
    // No secret value can appear in a rendered error: the type has no field
    // for one, and the id is the only named detail.
    assert!(!Error::Secret {
        id: "prod/db".into(),
        message: "not found".into()
    }
    .to_string()
    .contains("hunter2"));
}

#[test]
fn json_errors_become_validation_errors() {
    let serde_error = serde_json::from_str::<serde_json::Value>("{").expect_err("bad json");
    let error: Error = serde_error.into();
    assert_eq!(error.code(), "validation");
    assert!(error.to_string().starts_with("invalid payload: JSON:"));
}

#[test]
fn every_config_error_variant_renders_and_names_its_key() {
    let cases = [
        (
            ConfigError::Missing { key: "A".into() },
            "missing required environment variable `A`",
        ),
        (
            ConfigError::Empty { key: "B".into() },
            "environment variable `B` is empty",
        ),
        (
            ConfigError::Invalid {
                key: "C".into(),
                message: "bad".into(),
            },
            "environment variable `C` is invalid: bad",
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(error.to_string(), expected);
        assert_eq!(error.key(), expected.split('`').nth(1).expect("key"));
        assert_eq!(error.clone(), error);
        assert!(!format!("{error:?}").is_empty());
    }
}

#[test]
fn error_variants_convert_from_config_errors() {
    let error: Error = ConfigError::Empty { key: "X".into() }.into();
    assert!(matches!(error, Error::Config(_)));
    assert_eq!(error.code(), "config");
}

#[cfg(feature = "eventbridge")]
#[tokio::test]
async fn eventbridge_surface_is_complete() {
    use worker_shared::eventbridge::{
        Entry, EventEnvelope, ScheduledTick, WireEntry, MAX_BATCH_BYTES, MAX_ENTRIES,
        SCHEDULED_EVENT,
    };

    assert_eq!(SCHEDULED_EVENT, "Scheduled Event");
    assert_eq!(MAX_ENTRIES, 10);
    assert_eq!(MAX_BATCH_BYTES, 262_144);

    // ScheduledTick is constructible and round-trips through serde.
    let tick = ScheduledTick {
        event_id: "id".to_string(),
        rule_arn: "arn".to_string(),
        time: time::macros::datetime!(2026-10-25 06:00:00 UTC),
        detail_type: SCHEDULED_EVENT.to_string(),
    };
    let encoded = serde_json::to_value(&tick).expect("serialise");
    let decoded: ScheduledTick = serde_json::from_value(encoded).expect("deserialise");
    assert_eq!(decoded, tick);
    assert_eq!(tick.clone(), tick);

    // EventEnvelope round-trips, including the optional fields.
    let envelope = EventEnvelope::from_value(serde_json::json!({
        "id": "e1",
        "source": "app",
        "detail-type": SCHEDULED_EVENT,
        "time": "2026-10-25T06:00:00Z",
        "resources": ["arn"],
        "detail": {"a": 1},
        "account": "1",
        "region": "eu-central-1",
        "event-bus-name": "default"
    }))
    .expect("envelope");
    assert_eq!(envelope.event_bus_name.as_deref(), Some("default"));
    assert_eq!(envelope.account.as_deref(), Some("1"));
    assert_eq!(envelope.region.as_deref(), Some("eu-central-1"));
    assert!(envelope.is_scheduled());
    let encoded = serde_json::to_value(&envelope).expect("serialise");
    assert_eq!(encoded["event-bus-name"], "default");
    let decoded: EventEnvelope = serde_json::from_value(encoded).expect("deserialise");
    assert_eq!(decoded, envelope);

    // An envelope without a time serialises without one (the `None` branch of
    // the custom serde helper).
    let timeless = EventEnvelope::from_value(serde_json::json!({"id": "x"})).expect("envelope");
    let encoded = serde_json::to_value(&timeless).expect("serialise");
    assert!(encoded["time"].is_null());
    assert_eq!(envelope.clone(), envelope);

    // TryFrom<Value> and parse both work.
    let from_value = worker_shared::eventbridge::EventBridgeScheduledEvent::try_from(
        serde_json::json!({"id": "e", "time": "2026-10-25T06:00:00Z", "resources": ["arn"]}),
    )
    .expect("try_from");
    assert_eq!(from_value.tick.event_id, "e");
    assert_eq!(from_value.clone(), from_value);
    let parsed = worker_shared::eventbridge::EventBridgeScheduledEvent::parse(
        r#"{"id":"e","time":"2026-10-25T06:00:00Z","resources":["arn"]}"#,
    )
    .expect("parse");
    assert_eq!(parsed, from_value);

    // Entry builders and WireEntry.
    let entry = Entry::new("app", "Ping", serde_json::json!({"n": 1}))
        .expect("entry")
        .with_resources(vec!["arn".to_string()])
        .with_event_bus_name("bus")
        .with_time(time::macros::datetime!(2026-10-25 06:00:00 UTC));
    let wire: WireEntry<'_> = entry.wire();
    assert_eq!(wire.source, "app");
    assert_eq!(wire.detail_type, "Ping");
    assert_eq!(wire.detail, "{\"n\":1}");
    assert_eq!(wire.resources, ["arn".to_string()]);
    assert_eq!(wire.event_bus_name, Some("bus"));
    assert!(wire.time.is_some());
    assert_eq!(entry.clone(), entry);
    assert_ne!(
        entry,
        Entry::new("app", "Ping", serde_json::json!({})).expect("other")
    );
    assert_eq!(entry.resources().len(), 1);
    assert_eq!(entry.event_bus_name(), Some("bus"));
    assert!(entry.time().is_some());
}

#[cfg(feature = "eventbridge")]
#[tokio::test]
async fn eventbridge_batch_surface_is_complete() {
    use worker_shared::eventbridge::{
        dispatch, plan, Batch, DeliveryReport, Dispatcher, Entry, EntryFailure, EntryResult,
        FnDispatcher, PutEventsResponse,
    };

    let entry = Entry::new("app", "Ping", serde_json::json!({"n": 1}))
        .expect("entry")
        .with_resources(vec!["arn".to_string()])
        .with_event_bus_name("bus");

    // Batch: default, empty, into_entries, TryFrom failure.
    assert!(Batch::default().is_empty());
    assert_eq!(Batch::new().len(), 0);
    assert_eq!(plan(Vec::new()).expect("empty plan").len(), 0);
    let batch = Batch::try_from(vec![entry.clone()]).expect("batch");
    assert_eq!(batch.clone(), batch);
    assert_eq!(batch.into_entries().len(), 1);
    assert_eq!(
        Batch::try_from(vec![
            Entry::new("a", "B", serde_json::json!({}))
                .expect("entry");
            11
        ])
        .expect_err("too many")
        .code(),
        "too_many_entries"
    );

    // DeliveryReport with failures.
    let report = DeliveryReport {
        accepted: 1,
        failures: vec![
            EntryFailure {
                index: 0,
                code: "ThrottlingException".to_string(),
                message: "slow".to_string(),
            },
            EntryFailure {
                index: 2,
                code: "MalformedDetail".to_string(),
                message: "bad".to_string(),
            },
        ],
    };
    assert_eq!(report.total(), 3);
    assert!(!report.is_complete());
    assert_eq!(report.failed_indexes().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(report.retryable_indexes().collect::<Vec<_>>(), vec![0]);
    assert_eq!(report.clone(), report);
    assert!(!format!("{report:?}").is_empty());
    assert!(!report.failures[1].is_retryable());

    // PutEventsResponse constructors.
    assert_eq!(PutEventsResponse::empty(), PutEventsResponse::default());
    assert_eq!(PutEventsResponse::all_accepted(2).results.len(), 2);
    assert_eq!(
        PutEventsResponse::from_results(vec![EntryResult::failed("X", "y")])
            .results
            .len(),
        1
    );

    // FnDispatcher behind Arc<dyn Dispatcher>: the blanket impl.
    let inner = FnDispatcher::new(|entries: &[Entry]| {
        std::future::ready(Ok::<PutEventsResponse, Error>(
            PutEventsResponse::all_accepted(entries.len()),
        ))
    });
    let shared: std::sync::Arc<dyn Dispatcher> = std::sync::Arc::new(inner);
    let batch = Batch::try_from(vec![entry]).expect("batch");
    assert_eq!(
        dispatch(&batch, &shared).await.expect("dispatch").accepted,
        1
    );
}

#[cfg(feature = "sqs")]
#[test]
fn sqs_surface_is_complete() {
    use worker_shared::sqs::{
        partial_batch_response, BatchItemFailures, SqsBatchEvent, SqsMessage,
    };

    let message = SqsMessage::new("id", "body");
    assert_eq!(message.clone(), message);
    assert!(!format!("{message:?}").is_empty());

    let event = SqsBatchEvent::parse(r#"{"Records":[{"messageId":"a"}]}"#).expect("parse");
    assert_eq!(event.clone(), event);
    assert_eq!(event.iter().count(), 1);
    let encoded = serde_json::to_value(&event).expect("serialise");
    let decoded: SqsBatchEvent = serde_json::from_value(encoded).expect("deserialise");
    assert_eq!(decoded, event);

    let mut failures = BatchItemFailures::new();
    failures.push("a");
    assert_eq!(
        partial_batch_response(&failures)["batchItemFailures"][0]["itemIdentifier"],
        "a"
    );
    assert_eq!(failures.to_json(), partial_batch_response(&failures));
    assert!(!format!("{failures:?}").is_empty());
}

#[cfg(feature = "ddb")]
#[test]
fn ddb_surface_is_complete() {
    use worker_shared::ddb_streams::{DdbStreamEvent, StreamRecord, StreamRecordKind};

    assert_eq!(StreamRecordKind::parse("insert"), StreamRecordKind::Insert);
    assert_eq!(StreamRecordKind::Modify.as_str(), "MODIFY");
    assert_eq!(StreamRecordKind::Remove.to_string(), "REMOVE");
    assert!(StreamRecordKind::Remove.is_known());
    assert!(!format!("{:?}", StreamRecordKind::Insert).is_empty());

    let record = StreamRecord::new("id", StreamRecordKind::Insert);
    assert_eq!(record.clone(), record);
    assert!(!format!("{record:?}").is_empty());

    let event = DdbStreamEvent::from_value(serde_json::json!({
        "Records": [{"eventID": "a", "eventName": "INSERT"}]
    }))
    .expect("parse");
    assert_eq!(event.clone(), event);
    assert_eq!(event.skipped, 0);
}

#[tokio::test]
async fn secret_surface_is_complete() {
    use worker_shared::secrets::{CacheConfig, CacheStats, SecretCache, SecretString};

    let config = CacheConfig::default();
    assert_eq!(config.clone(), config);
    assert_eq!(
        CacheConfig::new(Duration::from_secs(1)).ttl,
        Duration::from_secs(1)
    );
    assert!(config.lifetime(Duration::from_secs(1)) <= config.ttl);
    assert!(!format!("{config:?}").is_empty());
    assert_eq!(CacheStats::default().hits, 0);
    assert!(!format!("{:?}", CacheStats::default()).is_empty());

    let fetcher = common::CountingFetcher::new();
    let cache = SecretCache::new(std::sync::Arc::clone(&fetcher), config).expect("cache");
    cache.get("k").await.expect("value");
    assert_eq!(cache.evict_expired(), 0);
    assert_eq!(cache.invalidate_all(), 1);
    assert!(cache.is_empty());
    assert_eq!(cache.config().ttl, config.ttl);
    assert!(!format!("{cache:?}").is_empty());

    let secret = SecretString::new("v").expect("secret");
    assert_eq!(secret.clone().len(), 1);
    assert!(secret.ct_eq(&SecretString::new("v").expect("secret")));
}

#[test]
fn handler_debug_impls_exist() {
    use worker_shared::handler::{Cancellation, Deadline, Lazy};

    assert!(format!("{:?}", Lazy::<u8>::new()).starts_with("Lazy"));
    assert!(!format!("{:?}", Deadline::after(Duration::from_secs(1))).is_empty());
    assert!(!format!("{:?}", Cancellation::new()).is_empty());
    assert!(!format!("{:?}", Cancellation::new().token()).is_empty());
}
