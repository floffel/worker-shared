//! SQS batch parsing and partial-batch responses.
#![cfg(feature = "sqs")]

mod common;

use serde_json::json;

use worker_shared::sqs::{
    by_id, partial_batch_response, run_batch, BatchItemFailures, SqsBatchEvent, SqsMessage,
};
use worker_shared::Error;

#[test]
fn batch_parses_camel_case_and_keeps_every_field() {
    let raw = json!({"Records": [{
        "messageId": "m-1",
        "receiptHandle": "rh",
        "body": "{\"n\":1}",
        "attributes": {"ApproximateReceiveCount": "3", "SenderId": "AIDA"},
        "messageAttributes": {"kind": {"stringValue": "a", "dataType": "String"}},
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:eu-central-1:1:queue",
        "unknownField": true
    }]});

    let event = SqsBatchEvent::from_value(raw).expect("parse");
    assert_eq!(event.len(), 1);
    let message = &event.records[0];
    assert_eq!(message.message_id, "m-1");
    assert_eq!(message.receipt_handle.as_deref(), Some("rh"));
    assert_eq!(message.body, "{\"n\":1}");
    assert_eq!(message.receive_count(), 3);
    assert_eq!(message.event_source.as_deref(), Some("aws:sqs"));
    assert_eq!(
        message.event_source_arn.as_deref(),
        Some("arn:aws:sqs:eu-central-1:1:queue")
    );
    assert_eq!(message.message_attributes["kind"]["stringValue"], "a");
}

#[test]
fn snake_case_is_accepted_too() {
    let raw = json!({"records": [{
        "message_id": "m-2",
        "body": "hello",
        "receipt_handle": "rh2",
        "message_attributes": {},
        "event_source_arn": "arn"
    }]});

    let event = SqsBatchEvent::from_value(raw).expect("parse");
    assert_eq!(event.records[0].message_id, "m-2");
    assert_eq!(event.records[0].receipt_handle.as_deref(), Some("rh2"));
    assert_eq!(event.records[0].event_source_arn.as_deref(), Some("arn"));
}

#[test]
fn minimal_message_defaults_the_optional_fields() {
    let event = SqsBatchEvent::parse(r#"{"Records":[{"messageId":"only"}]}"#).expect("parse");
    let message = &event.records[0];
    assert!(message.body.is_empty());
    assert!(message.receipt_handle.is_none());
    assert!(message.attributes.is_empty());
    assert!(message.message_attributes.is_empty());
    assert!(message.event_source.is_none());
    assert!(message.event_source_arn.is_none());
    assert_eq!(message.receive_count(), 1, "never delivered before");
}

#[test]
fn a_record_without_message_id_is_an_error_naming_its_position() {
    let raw = json!({"Records": [
        {"messageId": "ok"},
        {"body": "no id"}
    ]});
    let error = SqsBatchEvent::from_value(raw).expect_err("missing id");
    assert_eq!(error.code(), "validation");
    assert!(error.to_string().contains("record 1"), "{error}");
}

#[test]
fn non_object_and_malformed_payloads_are_rejected() {
    for raw in ["[]", "null", "3", "\"x\""] {
        assert!(SqsBatchEvent::parse(raw).is_err(), "{raw}");
    }
    assert!(SqsBatchEvent::parse("{").is_err());
    // A missing `Records` key is an empty batch, not an error.
    assert!(SqsBatchEvent::parse("{}").expect("empty").is_empty());
}

#[test]
fn empty_batch_is_legal_and_produces_an_empty_report() {
    let event = SqsBatchEvent::default();
    assert_eq!(event.len(), 0);
    assert!(event.is_empty());
    assert_eq!(event.iter().count(), 0);
    assert!(event.message("x").is_none());

    let response = partial_batch_response(&BatchItemFailures::new());
    assert_eq!(response.to_string(), r#"{"batchItemFailures":[]}"#);
}

#[test]
fn failures_keep_input_order_and_ignore_duplicates() {
    let mut failures = BatchItemFailures::new();
    assert!(failures.push("c"));
    assert!(failures.push("a"));
    assert!(!failures.push("a"), "duplicate");
    assert!(!failures.push(""), "empty id would poison the report");
    assert_eq!(failures.len(), 2);
    assert_eq!(failures.as_slice(), &["c".to_string(), "a".to_string()]);
    assert!(!failures.is_empty());
    assert_eq!(
        failures.to_json_string().expect("json"),
        r#"{"batchItemFailures":[{"itemIdentifier":"c"},{"itemIdentifier":"a"}]}"#
    );
}

#[test]
fn failures_are_built_positionally_from_results() {
    let ids = ["a".to_string(), "b".to_string(), "c".to_string()];
    let failures = BatchItemFailures::from_results(&ids, &[false, true, false]).expect("report");
    assert_eq!(failures.as_slice(), &["a".to_string(), "c".to_string()]);

    let error = BatchItemFailures::from_results(&ids, &[true]).expect_err("length mismatch");
    assert_eq!(error.code(), "validation");
    assert!(error.to_string().contains("3 ids but 1 results"), "{error}");
}

#[test]
fn failures_from_outcomes_and_iterators() {
    let failures = BatchItemFailures::from_outcomes([
        ("a".to_string(), Ok::<(), &str>(())),
        ("b".to_string(), Err("boom")),
        ("c".to_string(), Err("boom")),
    ]);
    assert_eq!(failures.as_slice(), &["b".to_string(), "c".to_string()]);

    let collected: BatchItemFailures = ["x".to_string(), "x".to_string(), "y".to_string()]
        .into_iter()
        .collect();
    assert_eq!(collected.len(), 2);
    assert_eq!(collected, collected.clone());
    assert_eq!(BatchItemFailures::default(), BatchItemFailures::new());
}

#[tokio::test]
async fn run_batch_reports_failed_messages_in_batch_order() {
    let event = SqsBatchEvent::parse(
        r#"{"Records":[{"messageId":"a"},{"messageId":"b"},{"messageId":"c"}]}"#,
    )
    .expect("parse");

    let failures = run_batch(&event, |index, message| {
        assert_eq!(message.message_id.len(), 1);
        async move { index != 1 }
    })
    .await;
    assert_eq!(failures.as_slice(), &["b".to_string()]);
    assert_eq!(
        failures.to_json()["batchItemFailures"][0]["itemIdentifier"],
        "b"
    );
}

#[tokio::test]
async fn run_batch_handles_the_zero_message_case() {
    let event = SqsBatchEvent::default();
    let failures = run_batch(&event, |_, _| std::future::ready(false)).await;
    assert!(failures.is_empty());
}

#[tokio::test]
async fn unicode_and_control_characters_survive_the_round_trip() {
    let raw = json!({"Records": [{
        "messageId": "id-ü-🚀",
        "body": "Grüße\u{0}\n\t{\"n\":1}",
        "attributes": {"ApproximateReceiveCount": "not-a-number"}
    }]});
    let event = SqsBatchEvent::from_value(raw).expect("parse");
    assert_eq!(event.records[0].body, "Grüße\u{0}\n\t{\"n\":1}");
    assert_eq!(event.records[0].receive_count(), 1, "garbage falls back");

    let mut failures = BatchItemFailures::new();
    failures.push(event.records[0].message_id.clone());
    let json = failures.to_json().to_string();
    assert!(json.contains("ü") || json.contains("\\u"), "{json}");
}

#[test]
fn by_id_indexes_the_batch() {
    let event = SqsBatchEvent::parse(
        r#"{"Records":[{"messageId":"a","body":"1"},{"messageId":"b","body":"2"}]}"#,
    )
    .expect("parse");
    let index = by_id(&event);
    assert_eq!(index.len(), 2);
    assert_eq!(index["a"].body, "1");
    assert_eq!(index["b"].body, "2");
    assert!(!index.contains_key("c"));
}

#[test]
fn a_large_batch_keeps_every_failure() {
    let records: Vec<serde_json::Value> = (0..1_000)
        .map(|index| json!({"messageId": format!("m-{index}"), "body": "x"}))
        .collect();
    let event = SqsBatchEvent::from_value(json!({"Records": records})).expect("parse");
    assert_eq!(event.len(), 1_000);

    let mut failures = BatchItemFailures::new();
    for message in event.iter().step_by(2) {
        failures.push(message.message_id.clone());
    }
    assert_eq!(failures.len(), 500);
    assert_eq!(failures.as_slice()[0], "m-0");
    assert_eq!(failures.as_slice()[499], "m-998");
}

#[test]
fn sqs_message_new_builds_a_minimal_record() {
    let message = SqsMessage::new("id", "body");
    assert_eq!(message.message_id, "id");
    assert_eq!(message.body, "body");
    assert!(message.attributes.is_empty());
    assert_eq!(message, message.clone());
}

#[test]
fn error_code_of_a_bad_sqs_payload_is_validation() {
    let error = SqsBatchEvent::parse("[]").expect_err("not an object");
    assert!(matches!(error, Error::Validation(_)));
    assert!(error
        .to_string()
        .strip_prefix("invalid payload: ")
        .is_some());
}
