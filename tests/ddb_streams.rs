//! DynamoDB Streams envelope parsing.
#![cfg(feature = "ddb")]

mod common;

use serde_json::json;

use worker_shared::ddb_streams::{table_from_arn, DdbStreamEvent, StreamRecord, StreamRecordKind};

const STREAM_ARN: &str =
    "arn:aws:dynamodb:eu-central-1:123456789012:table/orders/stream/2026-10-25T06:00:00.000";

#[test]
fn batch_parses_the_aws_wire_shape() {
    let raw = json!({"Records": [{
        "eventID": "f1",
        "eventName": "INSERT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": "eu-central-1",
        "dynamodb": {
            "ApproximateCreationDateTime": 1_792_908_000.5,
            "Keys": {"id": {"S": "42"}},
            "NewImage": {"id": {"S": "42"}, "total": {"N": "10"}},
            "SequenceNumber": "111",
            "SizeBytes": 26,
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        },
        "eventSourceARN": STREAM_ARN
    }]});

    let event = DdbStreamEvent::from_value(raw).expect("parse");
    assert_eq!(event.len(), 1);
    assert_eq!(event.skipped, 0);

    let record = &event.records[0];
    assert_eq!(record.event_id, "f1");
    assert_eq!(record.kind, StreamRecordKind::Insert);
    assert_eq!(record.table_name.as_deref(), Some("orders"));
    assert_eq!(record.new_image["total"]["N"], "10");
    assert_eq!(record.old_image, serde_json::Value::Null);
    assert_eq!(record.sequence_number.as_deref(), Some("111"));
    assert_eq!(record.created_at(), Some(1_792_908_000.5));
    assert_eq!(
        record.created_at_rfc3339().as_deref(),
        Some("2026-10-25T06:00:00.5Z")
    );
}

#[test]
fn snake_case_and_lowercase_event_names_are_tolerated() {
    let raw = json!({"records": [{
        "event_id": "f2",
        "event_name": "modify",
        "event_source_arn": STREAM_ARN,
        "dynamodb": {"new_image": {"a": {"S": "1"}}, "old_image": {"a": {"S": "0"}}}
    }]});

    let event = DdbStreamEvent::from_value(raw).expect("parse");
    assert_eq!(event.records[0].kind, StreamRecordKind::Modify);
    assert_eq!(event.records[0].new_image["a"]["S"], "1");
    assert_eq!(event.records[0].old_image["a"]["S"], "0");
    assert_eq!(event.records[0].table_name.as_deref(), Some("orders"));
}

#[test]
fn unknown_event_names_are_carried_not_dropped() {
    let raw = json!({"Records": [{"eventID": "f3", "eventName": "BACKFILL"}]});
    let event = DdbStreamEvent::from_value(raw).expect("parse");
    assert_eq!(
        event.records[0].kind,
        StreamRecordKind::Other("BACKFILL".to_string())
    );
    assert!(!event.records[0].kind.is_known());
    assert_eq!(event.records[0].kind.as_str(), "BACKFILL");
    assert_eq!(event.records[0].kind.to_string(), "BACKFILL");
}

#[test]
fn records_without_an_event_id_are_counted_as_skipped() {
    let raw = json!({"Records": [
        {"eventName": "INSERT"},
        {"eventID": "good", "eventName": "REMOVE"}
    ]});
    let event = DdbStreamEvent::from_value(raw).expect("one good record");
    assert_eq!(event.len(), 1);
    assert_eq!(event.skipped, 1);
    assert_eq!(event.records[0].kind, StreamRecordKind::Remove);
}

#[test]
fn a_batch_with_nothing_parseable_is_an_error() {
    let error = DdbStreamEvent::from_value(json!({"Records": [{"eventName": "INSERT"}]}))
        .expect_err("no records");
    assert_eq!(error.code(), "validation");
    assert!(error.to_string().contains("1 skipped"), "{error}");

    assert!(DdbStreamEvent::from_value(json!({"Records": []})).is_err());
    assert!(DdbStreamEvent::parse("[]").is_err());
    assert!(DdbStreamEvent::parse("not json").is_err());
}

#[test]
fn empty_event_names_become_the_other_variant() {
    let raw = json!({"Records": [{"eventID": "f4"}]});
    let event = DdbStreamEvent::from_value(raw).expect("parse");
    assert_eq!(
        event.records[0].kind,
        StreamRecordKind::Other(String::new())
    );
}

#[test]
fn images_default_to_null_instead_of_failing() {
    let raw = json!({"Records": [{"eventID": "f5", "eventName": "INSERT"}]});
    let event = DdbStreamEvent::from_value(raw).expect("parse");
    assert_eq!(event.records[0].new_image, serde_json::Value::Null);
    assert_eq!(event.records[0].old_image, serde_json::Value::Null);
    assert!(event.records[0].table_name.is_none());
    assert!(event.records[0].sequence_number.is_none());
    assert!(event.records[0].created_at().is_none());
    assert!(event.records[0].created_at_rfc3339().is_none());
}

#[test]
fn created_at_rfc3339_rejects_unrepresentable_values() {
    let mut record = StreamRecord::new("id", StreamRecordKind::Insert);
    record.created_at = Some(f64::NAN);
    assert!(record.created_at_rfc3339().is_none());

    record.created_at = Some(1e20);
    assert!(record.created_at_rfc3339().is_none());

    record.created_at = Some(-1.0);
    assert!(
        record.created_at_rfc3339().is_some(),
        "pre-epoch is representable"
    );

    record.created_at = Some(0.0);
    assert_eq!(
        record.created_at_rfc3339().as_deref(),
        Some("1970-01-01T00:00:00Z")
    );
}

#[test]
fn table_name_falls_back_to_a_table_prefixed_event_source() {
    let raw = json!({"Records": [{
        "eventID": "f6",
        "eventName": "INSERT",
        "eventSource": "table/legacy"
    }]});
    let event = DdbStreamEvent::from_value(raw).expect("parse");
    assert_eq!(event.records[0].table_name.as_deref(), Some("legacy"));
}

#[test]
fn table_from_arn_handles_the_edge_cases() {
    assert_eq!(table_from_arn(STREAM_ARN).as_deref(), Some("orders"));
    assert_eq!(table_from_arn("not-an-arn"), None);
    assert_eq!(table_from_arn(""), None);
    assert_eq!(
        table_from_arn("arn:aws:dynamodb:eu-central-1:1:table/"),
        None
    );
    assert_eq!(
        table_from_arn("arn:aws:dynamodb:eu-central-1:1:table/my.table/stream/x").as_deref(),
        Some("my.table")
    );
    assert_eq!(
        table_from_arn("arn:aws:dynamodb:eu-central-1:1:index/x"),
        None
    );
}

#[test]
fn accessors_and_builders_cover_the_surface() {
    let record = StreamRecord::new("id", StreamRecordKind::Modify)
        .with_new_image(json!({"a": {"N": "2"}}))
        .with_old_image(json!({"a": {"N": "1"}}))
        .with_table_name("orders");

    assert_eq!(record.new_image["a"]["N"], "2");
    assert_eq!(record.old_image["a"]["N"], "1");
    assert_eq!(record.table_name.as_deref(), Some("orders"));
    assert_eq!(record, record.clone());
    assert_ne!(record, StreamRecord::new("id", StreamRecordKind::Insert));
}

#[test]
fn record_kinds_round_trip_through_serde() {
    for kind in [
        StreamRecordKind::Insert,
        StreamRecordKind::Modify,
        StreamRecordKind::Remove,
        StreamRecordKind::Other("BACKFILL".to_string()),
    ] {
        let json = serde_json::to_string(&kind).expect("serialise");
        let back: StreamRecordKind = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(back, kind);
    }
    let parsed: StreamRecordKind = serde_json::from_str("\"insert\"").expect("lowercase");
    assert_eq!(parsed, StreamRecordKind::Insert);
    assert_eq!(
        StreamRecordKind::parse(""),
        StreamRecordKind::Other(String::new())
    );
}

#[test]
fn a_whole_event_round_trips_through_serde() {
    let raw = json!({"Records": [{"eventID": "f7", "eventName": "INSERT", "eventSourceARN": STREAM_ARN}]});
    let event = DdbStreamEvent::from_value(raw).expect("parse");
    let encoded = serde_json::to_value(&event).expect("serialise");
    let decoded: DdbStreamEvent = serde_json::from_value(encoded).expect("deserialise");
    assert_eq!(decoded, event);
}

#[test]
fn iterators_expose_records_and_ids_in_order() {
    let raw = json!({"Records": [
        {"eventID": "a", "eventName": "INSERT"},
        {"eventID": "b", "eventName": "MODIFY"}
    ]});
    let event = DdbStreamEvent::from_value(raw).expect("parse");
    assert_eq!(event.iter().count(), 2);
    assert_eq!(event.event_ids().collect::<Vec<_>>(), vec!["a", "b"]);
    assert_ne!(event.len(), 0);
    assert_eq!(DdbStreamEvent::default().len(), 0);
}
