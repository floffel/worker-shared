//! SQS batch events and partial-batch responses.
//!
//! An SQS-triggered Lambda gets up to 10 (default) or 10 000 (max batching)
//! messages per invocation. By default a *single* failed message makes the
//! whole batch visible again, so nine good messages are reprocessed to retry
//! one — duplicate work, and duplicate side effects for anything not
//! idempotent. The fix is `ReportBatchItemFailures` on the event source
//! mapping: the function reports exactly which message ids failed and only
//! those return to the queue.
//!
//! [`BatchItemFailures`] builds that report. It is ordered by input position,
//! de-duplicated, and never contains an empty identifier (SQS rejects the
//! whole report if it does, which would silently turn a partial failure into a
//! full-batch retry).
//!
//! ```
//! use worker_shared::sqs::{run_batch, SqsBatchEvent};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), worker_shared::Error> {
//! let raw = r#"{"Records": [
//!     {"messageId": "a", "body": "{\"n\":1}"},
//!     {"messageId": "b", "body": "{\"n\":2}"}
//! ]}"#;
//!
//! let event = SqsBatchEvent::parse(raw)?;
//! let response = run_batch(&event, |index, _message| std::future::ready(index != 1)).await;
//!
//! assert_eq!(response.to_json().to_string(), r#"{"batchItemFailures":[{"itemIdentifier":"b"}]}"#);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::error::Error;

/// One SQS message.
///
/// Decoding is permissive: unknown fields are ignored, `snake_case` is accepted
/// alongside camelCase, and only `messageId` is required — without it the
/// message cannot be reported as failed, which is a hard error rather than a
/// silent full-batch retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsMessage {
    /// The SQS message id. This is what `batchItemFailures` refers to.
    #[serde(default, alias = "message_id")]
    pub message_id: String,
    /// The message body.
    #[serde(default)]
    pub body: String,
    /// The receipt handle, needed to delete the message out-of-band.
    #[serde(default, alias = "receipt_handle")]
    pub receipt_handle: Option<String>,
    /// System attributes (`ApproximateReceiveCount`, `SenderId`, ...).
    #[serde(default)]
    pub attributes: Map<String, Value>,
    /// Message attributes set by the producer.
    #[serde(default, alias = "message_attributes")]
    pub message_attributes: Map<String, Value>,
    /// `aws:sqs` for queue messages.
    #[serde(default, alias = "event_source")]
    pub event_source: Option<String>,
    /// The queue ARN the message came from.
    #[serde(default, alias = "eventSourceARN", alias = "event_source_arn")]
    pub event_source_arn: Option<String>,
}

impl SqsMessage {
    /// Build a message from an id and body (tests, replays, DLQ redrives).
    ///
    /// ```
    /// use worker_shared::sqs::SqsMessage;
    ///
    /// let message = SqsMessage::new("id-1", "{\"n\":1}");
    /// assert_eq!(message.message_id, "id-1");
    /// assert_eq!(message.body, "{\"n\":1}");
    /// assert!(message.receipt_handle.is_none());
    /// ```
    #[must_use]
    pub fn new(message_id: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            body: body.into(),
            receipt_handle: None,
            attributes: Map::new(),
            message_attributes: Map::new(),
            event_source: None,
            event_source_arn: None,
        }
    }

    /// How many times this message has been delivered, from
    /// `ApproximateReceiveCount`. `1` when the attribute is absent or
    /// unparseable — a message that has never been seen is on its first
    /// delivery.
    ///
    /// ```
    /// use worker_shared::sqs::SqsMessage;
    ///
    /// let message = SqsMessage::new("id", "body");
    /// assert_eq!(message.receive_count(), 1);
    /// ```
    #[must_use]
    pub fn receive_count(&self) -> u64 {
        self.attributes
            .get("ApproximateReceiveCount")
            .and_then(Value::as_str)
            .and_then(|count| count.parse().ok())
            .unwrap_or(1)
    }
}

/// A batch of SQS messages, decoded from the event source mapping.
///
/// ```
/// use worker_shared::sqs::SqsBatchEvent;
///
/// let event = SqsBatchEvent::parse(
///     r#"{"Records":[{"messageId":"a","body":"one"},{"messageId":"b","body":"two"}]}"#,
/// )?;
/// assert_eq!(event.len(), 2);
/// assert_eq!(event.records[0].body, "one");
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqsBatchEvent {
    /// The messages, in the order SQS delivered them.
    #[serde(default, rename = "Records", alias = "records")]
    pub records: Vec<SqsMessage>,
}

impl SqsBatchEvent {
    /// Parse JSON text into a batch.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the input is not JSON, is not an object with
    /// `Records`, or a record has no usable `messageId` (the error names the
    /// record's position).
    ///
    /// ```
    /// use worker_shared::sqs::SqsBatchEvent;
    ///
    /// assert!(SqsBatchEvent::parse("{}").is_ok()); // empty batch
    /// assert!(SqsBatchEvent::parse("[]").is_err());
    /// assert!(SqsBatchEvent::parse(r#"{"Records":[{"body":"no id"}]}"#).is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, Error> {
        let value: Value = serde_json::from_str(raw)?;
        Self::from_value(value)
    }

    /// Parse an already-decoded JSON value.
    ///
    /// # Errors
    ///
    /// As [`Self::parse`].
    ///
    /// ```
    /// use worker_shared::sqs::SqsBatchEvent;
    ///
    /// let event = SqsBatchEvent::from_value(serde_json::json!({"Records": []}))?;
    /// assert!(event.is_empty());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn from_value(value: Value) -> Result<Self, Error> {
        if !value.is_object() {
            return Err(Error::Validation(
                "SQS event must be a JSON object with `Records`".to_string(),
            ));
        }
        let event: Self = serde_json::from_value(value)?;
        for (index, record) in event.records.iter().enumerate() {
            if record.message_id.is_empty() {
                return Err(Error::Validation(format!(
                    "SQS record {index} has no `messageId`"
                )));
            }
        }
        Ok(event)
    }

    /// Number of messages.
    ///
    /// ```
    /// use worker_shared::sqs::SqsBatchEvent;
    ///
    /// assert_eq!(SqsBatchEvent::default().len(), 0);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the batch is empty. An empty batch is a real event: SQS can
    /// invoke with zero messages, and the handler must answer with an empty
    /// failure list, not an error.
    ///
    /// ```
    /// use worker_shared::sqs::SqsBatchEvent;
    ///
    /// assert!(SqsBatchEvent::default().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Iterate the messages.
    ///
    /// ```
    /// use worker_shared::sqs::SqsBatchEvent;
    ///
    /// let event = SqsBatchEvent::parse(r#"{"Records":[{"messageId":"a"}]}"#)?;
    /// assert_eq!(event.iter().count(), 1);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = &SqsMessage> {
        self.records.iter()
    }

    /// The message with `message_id`.
    ///
    /// ```
    /// use worker_shared::sqs::SqsBatchEvent;
    ///
    /// let event = SqsBatchEvent::parse(r#"{"Records":[{"messageId":"a","body":"x"}]}"#)?;
    /// assert_eq!(event.message("a").map(|m| m.body.as_str()), Some("x"));
    /// assert!(event.message("missing").is_none());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn message(&self, message_id: &str) -> Option<&SqsMessage> {
        self.records
            .iter()
            .find(|record| record.message_id == message_id)
    }
}

/// The `batchItemFailures` report.
///
/// Ids are kept in insertion order and de-duplicated; an empty id is refused
/// by [`BatchItemFailures::push`] because SQS rejects the whole report on one
/// malformed entry, which would escalate a partial failure into a full-batch
/// retry.
///
/// ```
/// use worker_shared::sqs::BatchItemFailures;
///
/// let mut failures = BatchItemFailures::new();
/// assert!(failures.push("a"));
/// assert!(!failures.push("a")); // duplicate, ignored
/// assert!(!failures.push(""));  // empty, refused
///
/// assert_eq!(failures.as_slice(), &["a".to_string()]);
/// assert_eq!(failures.len(), 1);
/// assert!(!failures.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchItemFailures {
    ids: Vec<String>,
}

impl BatchItemFailures {
    /// An empty report.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// assert!(BatchItemFailures::new().is_empty());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a failure. Returns whether it was added.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// let mut failures = BatchItemFailures::new();
    /// assert!(failures.push("id"));
    /// assert!(!failures.push("id"));
    /// ```
    pub fn push(&mut self, message_id: impl Into<String>) -> bool {
        let message_id = message_id.into();
        if message_id.is_empty() || self.ids.contains(&message_id) {
            return false;
        }
        self.ids.push(message_id);
        true
    }

    /// The failed ids, in the order they were recorded.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// let mut failures = BatchItemFailures::new();
    /// failures.push("b");
    /// failures.push("a");
    /// assert_eq!(failures.as_slice(), &["b".to_string(), "a".to_string()]);
    /// ```
    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.ids
    }

    /// Number of failures.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// let mut failures = BatchItemFailures::new();
    /// failures.push("x");
    /// assert_eq!(failures.len(), 1);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether every message succeeded.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// assert!(BatchItemFailures::new().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// The Lambda response body: `{"batchItemFailures":[{"itemIdentifier":"…"}]}`.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// let mut failures = BatchItemFailures::new();
    /// failures.push("a");
    /// assert_eq!(
    ///     failures.to_json().to_string(),
    ///     r#"{"batchItemFailures":[{"itemIdentifier":"a"}]}"#,
    /// );
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn to_json(&self) -> Value {
        partial_batch_response(self)
    }

    /// The response body as a JSON string, ready to return from the handler.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] if serialisation fails, which it cannot for this
    /// shape; the signature keeps the call site panic-free.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// assert_eq!(
    ///     BatchItemFailures::new().to_json_string()?,
    ///     r#"{"batchItemFailures":[]}"#,
    /// );
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn to_json_string(&self) -> Result<String, Error> {
        Ok(serde_json::to_string(&self.to_json())?)
    }

    /// Build a report from one outcome per message, positionally.
    ///
    /// `ids` and `results` must have the same length; a failure at position
    /// `i` marks `ids[i]`. This is the shape a `for` loop over the batch
    /// naturally produces, and it keeps ordering trivial to verify.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the two slices differ in length — silently
    /// mismatching them would report the wrong messages as failed.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// let ids = ["a".to_string(), "b".to_string()];
    /// let results = [true, false];
    /// assert_eq!(
    ///     BatchItemFailures::from_results(&ids, &results)?.as_slice(),
    ///     &["b".to_string()],
    /// );
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn from_results(ids: &[String], results: &[bool]) -> Result<Self, Error> {
        if ids.len() != results.len() {
            return Err(Error::Validation(format!(
                "{} ids but {} results",
                ids.len(),
                results.len()
            )));
        }
        let mut failures = Self::new();
        for (id, ok) in ids.iter().zip(results) {
            if !ok {
                failures.push(id);
            }
        }
        Ok(failures)
    }

    /// Build a report from an iterator of `(message_id, result)`, keeping
    /// input order.
    ///
    /// ```
    /// use worker_shared::sqs::BatchItemFailures;
    ///
    /// let failures: BatchItemFailures = BatchItemFailures::from_outcomes([
    ///     ("a".to_string(), Ok(())),
    ///     ("b".to_string(), Err("boom")),
    /// ]);
    /// assert_eq!(failures.as_slice(), &["b".to_string()]);
    /// ```
    pub fn from_outcomes<E>(outcomes: impl IntoIterator<Item = (String, Result<(), E>)>) -> Self {
        let mut failures = Self::new();
        for (id, outcome) in outcomes {
            if outcome.is_err() {
                failures.push(id);
            }
        }
        failures
    }
}

impl FromIterator<String> for BatchItemFailures {
    fn from_iter<I: IntoIterator<Item = String>>(iter: I) -> Self {
        let mut failures = Self::new();
        for id in iter {
            failures.push(id);
        }
        failures
    }
}

/// Wrap a report in the exact JSON SQS expects.
///
/// The field is always present, `[]` included: SQS treats a missing
/// `batchItemFailures` as "all succeeded", so an explicit empty list is both
/// correct and unambiguous.
///
/// ```
/// use worker_shared::sqs::{partial_batch_response, BatchItemFailures};
///
/// let mut failures = BatchItemFailures::new();
/// failures.push("a");
/// assert_eq!(
///     partial_batch_response(&failures).to_string(),
///     r#"{"batchItemFailures":[{"itemIdentifier":"a"}]}"#,
/// );
/// ```
#[must_use]
pub fn partial_batch_response(failures: &BatchItemFailures) -> Value {
    json!({
        "batchItemFailures": failures
            .as_slice()
            .iter()
            .map(|id| json!({ "itemIdentifier": id }))
            .collect::<Vec<_>>(),
    })
}

/// Run `handler` for every message and report the ones that returned `false`.
///
/// The handler receives `(index, message)` and returns a future of `bool`
/// (`true` = processed). Failures come back in batch order, which makes the
/// report deterministic even though the handler is free to be concurrent
/// inside a single message.
///
/// Runs sequentially on purpose: the Lambda's concurrency is the batch, one
/// message already maps to one unit of work, and a panic in one message should
/// not take the rest of the batch down with it.
///
/// ```
/// use worker_shared::sqs::{run_batch, SqsBatchEvent};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), worker_shared::Error> {
/// let event = SqsBatchEvent::parse(
///     r#"{"Records":[{"messageId":"a"},{"messageId":"b"}]}"#,
/// )?;
///
/// let response = run_batch(&event, |_index, message| {
///     let ok = message.message_id != "b";
///     async move { ok }
/// }).await;
///
/// assert_eq!(response.as_slice(), &["b".to_string()]);
/// # Ok(())
/// # }
/// ```
pub async fn run_batch<F, Fut>(event: &SqsBatchEvent, handler: F) -> BatchItemFailures
where
    F: Fn(usize, &SqsMessage) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let mut failures = BatchItemFailures::new();
    for (index, message) in event.records.iter().enumerate() {
        if !handler(index, message).await {
            failures.push(message.message_id.clone());
        }
    }
    failures
}

/// Index the messages by id, for handlers that resolve a failure back to its
/// payload.
///
/// ```
/// use worker_shared::sqs::{by_id, SqsBatchEvent};
///
/// let event = SqsBatchEvent::parse(r#"{"Records":[{"messageId":"a","body":"x"}]}"#)?;
/// let index = by_id(&event);
/// assert_eq!(index.get("a").map(|m| m.body.as_str()), Some("x"));
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[must_use]
pub fn by_id(event: &SqsBatchEvent) -> HashMap<&str, &SqsMessage> {
    event
        .records
        .iter()
        .map(|record| (record.message_id.as_str(), record))
        .collect()
}
