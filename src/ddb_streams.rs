//! DynamoDB Streams envelope parsing.
//!
//! A Streams event is a batch: up to `batchSize` records, each with an
//! `eventName` (`INSERT` / `MODIFY` / `REMOVE`), an `eventID`, and the old and
//! new item images as DynamoDB's typed `AttributeValue` JSON
//! (`{"S": "x"}`, `{"N": "1"}`, ...).
//!
//! Two deliberate choices:
//!
//! * **Images stay as [`serde_json::Value`].** Most handlers reload the row by
//!   key and only need to know *that* it changed. Decoding attribute values
//!   into a typed model is the consumer's schema decision, not the plumbing's.
//! * **Unknown event names are carried, not dropped.** [`StreamRecordKind`]
//!   keeps an [`StreamRecordKind::Other`] variant so a new AWS event name (or a
//!   custom one in a replay) shows up in the handler instead of silently
//!   disappearing.
//!
//! ```
//! use worker_shared::ddb_streams::{DdbStreamEvent, StreamRecordKind};
//!
//! let raw = r#"{"Records": [{
//!     "eventID": "f1",
//!     "eventName": "INSERT",
//!     "eventSourceARN": "arn:aws:dynamodb:eu-central-1:123456789012:table/orders/stream/2026-10-25T06:00:00.000",
//!     "dynamodb": {"NewImage": {"id": {"S": "42"}}}
//! }]}"#;
//!
//! let event = DdbStreamEvent::parse(raw)?;
//! assert_eq!(event.len(), 1);
//!
//! let record = &event.records[0];
//! assert_eq!(record.kind, StreamRecordKind::Insert);
//! assert_eq!(record.table_name.as_deref(), Some("orders"));
//! assert_eq!(record.new_image["id"]["S"], "42");
//! # Ok::<(), worker_shared::Error>(())
//! ```

use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Error;

/// The kind of change a stream record describes.
///
/// ```
/// use worker_shared::ddb_streams::StreamRecordKind;
///
/// assert_eq!(StreamRecordKind::parse("MODIFY"), StreamRecordKind::Modify);
/// assert_eq!(
///     StreamRecordKind::parse("BACKFILL"),
///     StreamRecordKind::Other("BACKFILL".to_string()),
/// );
/// assert_eq!(StreamRecordKind::Insert.as_str(), "INSERT");
/// assert_eq!(StreamRecordKind::Insert.to_string(), "INSERT");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StreamRecordKind {
    /// A new item.
    Insert,
    /// An existing item changed.
    Modify,
    /// An item was deleted (the old image is preserved).
    Remove,
    /// Any other event name, carried verbatim.
    Other(String),
}

impl StreamRecordKind {
    /// Parse the wire string. Never fails; unknown names become
    /// [`Self::Other`].
    ///
    /// ```
    /// use worker_shared::ddb_streams::StreamRecordKind;
    ///
    /// assert_eq!(StreamRecordKind::parse("REMOVE"), StreamRecordKind::Remove);
    /// assert_eq!(StreamRecordKind::parse(""), StreamRecordKind::Other(String::new()));
    /// ```
    #[must_use]
    pub fn parse(value: &str) -> Self {
        match value.to_ascii_uppercase().as_str() {
            "INSERT" => Self::Insert,
            "MODIFY" => Self::Modify,
            "REMOVE" => Self::Remove,
            _ => Self::Other(value.to_string()),
        }
    }

    /// The wire string.
    ///
    /// ```
    /// use worker_shared::ddb_streams::StreamRecordKind;
    ///
    /// assert_eq!(StreamRecordKind::Other("X".into()).as_str(), "X");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Insert => "INSERT",
            Self::Modify => "MODIFY",
            Self::Remove => "REMOVE",
            Self::Other(other) => other,
        }
    }

    /// Whether this is a create, update or delete.
    ///
    /// ```
    /// use worker_shared::ddb_streams::StreamRecordKind;
    ///
    /// assert!(StreamRecordKind::Insert.is_known());
    /// assert!(!StreamRecordKind::Other("X".into()).is_known());
    /// ```
    #[must_use]
    pub fn is_known(&self) -> bool {
        !matches!(self, Self::Other(_))
    }
}

impl std::fmt::Display for StreamRecordKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for StreamRecordKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for StreamRecordKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(Self::parse(&raw))
    }
}

/// One record of a stream batch.
///
/// ```
/// use worker_shared::ddb_streams::{StreamRecord, StreamRecordKind};
///
/// let record = StreamRecord::new("f1", StreamRecordKind::Insert);
/// assert_eq!(record.event_id, "f1");
/// assert!(record.new_image.is_null());
/// assert_eq!(record.created_at(), None);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamRecord {
    /// Stable per-record id. EventBridge and Streams reuse it when they retry
    /// a batch, which makes it the idempotency key.
    pub event_id: String,
    /// What happened.
    pub kind: StreamRecordKind,
    /// The item after the change (`Value::Null` for `REMOVE`).
    pub new_image: Value,
    /// The item before the change (`Value::Null` for `INSERT`).
    pub old_image: Value,
    /// The table the record belongs to, parsed from `eventSourceARN`.
    pub table_name: Option<String>,
    /// The DynamoDB sequence number, when present. Together with the shard it
    /// totally orders the stream; useful for "newer record wins" decisions.
    pub sequence_number: Option<String>,
    /// The `ApproximateCreationDateTime` as epoch seconds, when present.
    pub created_at: Option<f64>,
}

impl StreamRecord {
    /// A minimal record, for tests and replays.
    ///
    /// ```
    /// use worker_shared::ddb_streams::{StreamRecord, StreamRecordKind};
    ///
    /// let record = StreamRecord::new("id", StreamRecordKind::Modify)
    ///     .with_new_image(serde_json::json!({"id": {"S": "1"}}));
    /// assert_eq!(record.new_image["id"]["S"], "1");
    /// ```
    #[must_use]
    pub fn new(event_id: impl Into<String>, kind: StreamRecordKind) -> Self {
        Self {
            event_id: event_id.into(),
            kind,
            new_image: Value::Null,
            old_image: Value::Null,
            table_name: None,
            sequence_number: None,
            created_at: None,
        }
    }

    /// Set the new image.
    ///
    /// ```
    /// use worker_shared::ddb_streams::{StreamRecord, StreamRecordKind};
    ///
    /// let record = StreamRecord::new("id", StreamRecordKind::Insert)
    ///     .with_new_image(serde_json::json!({"a": {"N": "1"}}));
    /// assert_eq!(record.new_image["a"]["N"], "1");
    /// ```
    #[must_use]
    pub fn with_new_image(mut self, image: Value) -> Self {
        self.new_image = image;
        self
    }

    /// Set the old image.
    ///
    /// ```
    /// use worker_shared::ddb_streams::{StreamRecord, StreamRecordKind};
    ///
    /// let record = StreamRecord::new("id", StreamRecordKind::Remove)
    ///     .with_old_image(serde_json::json!({"a": {"N": "1"}}));
    /// assert_eq!(record.old_image["a"]["N"], "1");
    /// ```
    #[must_use]
    pub fn with_old_image(mut self, image: Value) -> Self {
        self.old_image = image;
        self
    }

    /// Set the table name.
    ///
    /// ```
    /// use worker_shared::ddb_streams::{StreamRecord, StreamRecordKind};
    ///
    /// let record = StreamRecord::new("id", StreamRecordKind::Insert).with_table_name("orders");
    /// assert_eq!(record.table_name.as_deref(), Some("orders"));
    /// ```
    #[must_use]
    pub fn with_table_name(mut self, table: impl Into<String>) -> Self {
        self.table_name = Some(table.into());
        self
    }

    /// The `ApproximateCreationDateTime` as an RFC 3339 string, when present
    /// and representable.
    ///
    /// ```
    /// use worker_shared::ddb_streams::{StreamRecord, StreamRecordKind};
    ///
    /// assert_eq!(StreamRecord::new("id", StreamRecordKind::Insert).created_at_rfc3339(), None);
    /// ```
    #[must_use]
    pub fn created_at_rfc3339(&self) -> Option<String> {
        let seconds = self.created_at?;
        // Fractional epoch seconds; `OffsetDateTime` wants whole seconds plus
        // nanoseconds.
        let whole = seconds.trunc();
        if !whole.is_finite() || whole.abs() > 1e15 {
            return None;
        }
        #[allow(
            clippy::cast_possible_truncation,
            reason = "bounded by the 1e15 magnitude check above, which keeps |seconds| far inside i64"
        )]
        let secs = whole as i64;
        #[allow(
            clippy::cast_possible_truncation,
            reason = "the fractional part of a checked finite f64 is in 0..1e9 nanoseconds"
        )]
        let signed_nanos = ((seconds - whole) * 1e9) as i64;
        let timestamp = time::OffsetDateTime::from_unix_timestamp(secs)
            .ok()?
            .saturating_add(time::Duration::nanoseconds(signed_nanos));
        timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .ok()
    }

    /// A convenience alias for [`Self::created_at`] that reads as a question.
    ///
    /// ```
    /// use worker_shared::ddb_streams::{StreamRecord, StreamRecordKind};
    ///
    /// assert_eq!(StreamRecord::new("id", StreamRecordKind::Insert).created_at(), None);
    /// ```
    #[must_use]
    pub fn created_at(&self) -> Option<f64> {
        self.created_at
    }
}

/// A stream batch.
///
/// ```
/// use worker_shared::ddb_streams::DdbStreamEvent;
///
/// let event = DdbStreamEvent::parse(r#"{"Records":[{"eventID":"a","eventName":"REMOVE"}]}"#)?;
/// assert_eq!(event.len(), 1);
/// assert_eq!(event.skipped, 0);
/// assert_eq!(event.event_ids().collect::<Vec<_>>(), vec!["a"]);
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DdbStreamEvent {
    /// The records that parsed.
    pub records: Vec<StreamRecord>,
    /// How many records were dropped because they had no `eventID`. Non-zero
    /// is worth a log line: it means the batch was not fully understood.
    #[serde(default)]
    pub skipped: usize,
}

impl DdbStreamEvent {
    /// Parse JSON text.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the input is not JSON, when `Records` is not
    /// an array, or when nothing in the batch parsed — an event with no usable
    /// records cannot be handled, and returning an empty success would drop
    /// data.
    ///
    /// ```
    /// use worker_shared::ddb_streams::DdbStreamEvent;
    ///
    /// assert!(DdbStreamEvent::parse("nope").is_err());
    /// assert!(DdbStreamEvent::parse(r#"{"Records":[]}"#).is_err());
    /// assert!(DdbStreamEvent::parse(r#"{"Records":[{"eventName":"INSERT"}]}"#).is_err());
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
    /// use worker_shared::ddb_streams::DdbStreamEvent;
    ///
    /// let event = DdbStreamEvent::from_value(serde_json::json!({
    ///     "Records": [{"eventID": "a", "eventName": "insert"}]
    /// }))?;
    /// assert_eq!(event.records[0].kind.as_str(), "INSERT");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn from_value(value: Value) -> Result<Self, Error> {
        if !value.is_object() {
            return Err(Error::Validation(
                "DDB Streams event must be a JSON object with `Records`".to_string(),
            ));
        }
        let envelope: Envelope = serde_json::from_value(value)?;
        let mut records = Vec::with_capacity(envelope.records.len());
        let mut skipped = 0;
        for raw in envelope.records {
            match raw.into_record() {
                Some(record) => records.push(record),
                None => skipped += 1,
            }
        }
        if records.is_empty() {
            return Err(Error::Validation(format!(
                "DDB Streams batch had no parseable records ({skipped} skipped)"
            )));
        }
        Ok(Self { records, skipped })
    }

    /// Number of parsed records.
    ///
    /// ```
    /// use worker_shared::ddb_streams::DdbStreamEvent;
    ///
    /// assert_eq!(DdbStreamEvent::default().len(), 0);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the batch holds no records.
    ///
    /// ```
    /// use worker_shared::ddb_streams::DdbStreamEvent;
    ///
    /// assert!(DdbStreamEvent::default().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Iterate the records.
    ///
    /// ```
    /// use worker_shared::ddb_streams::DdbStreamEvent;
    ///
    /// let event = DdbStreamEvent::parse(r#"{"Records":[{"eventID":"a"}]}"#)?;
    /// assert_eq!(event.iter().count(), 1);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = &StreamRecord> {
        self.records.iter()
    }

    /// Iterate the event ids, in batch order.
    ///
    /// ```
    /// use worker_shared::ddb_streams::DdbStreamEvent;
    ///
    /// let event = DdbStreamEvent::parse(r#"{"Records":[{"eventID":"a"},{"eventID":"b"}]}"#)?;
    /// assert_eq!(event.event_ids().collect::<Vec<_>>(), vec!["a", "b"]);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn event_ids(&self) -> impl Iterator<Item = &str> {
        self.records.iter().map(|record| record.event_id.as_str())
    }
}

/// The wire envelope. Private: callers only see [`DdbStreamEvent`].
#[derive(Debug, Clone, Deserialize)]
struct Envelope {
    #[serde(default, rename = "Records", alias = "records")]
    records: Vec<RawRecord>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawRecord {
    #[serde(default, rename = "eventID", alias = "eventId", alias = "event_id")]
    event_id: String,
    #[serde(default, alias = "event_name")]
    event_name: String,
    #[serde(default)]
    dynamodb: Option<RawDynamo>,
    #[serde(
        default,
        rename = "eventSourceARN",
        alias = "eventSourceArn",
        alias = "event_source_arn"
    )]
    event_source_arn: Option<String>,
    #[serde(default)]
    event_source: Option<String>,
    #[serde(default)]
    sequence_number: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawDynamo {
    #[serde(default, rename = "NewImage", alias = "new_image")]
    new_image: Option<Value>,
    #[serde(default, rename = "OldImage", alias = "old_image")]
    old_image: Option<Value>,
    #[serde(
        default,
        rename = "ApproximateCreationDateTime",
        alias = "approximate_creation_date_time"
    )]
    approximate_creation_date_time: Option<f64>,
    #[serde(default, rename = "SequenceNumber", alias = "sequence_number")]
    sequence_number: Option<String>,
}

impl RawRecord {
    fn into_record(self) -> Option<StreamRecord> {
        if self.event_id.is_empty() {
            return None;
        }
        let dynamo = self.dynamodb.unwrap_or(RawDynamo {
            new_image: None,
            old_image: None,
            approximate_creation_date_time: None,
            sequence_number: None,
        });
        // The table name lives in the stream ARN, not in `eventSource` (which
        // is just `aws:dynamodb`).
        let table_name = self
            .event_source_arn
            .as_deref()
            .and_then(table_from_arn)
            .or_else(|| {
                self.event_source
                    .as_deref()
                    .and_then(|source| source.strip_prefix("table/"))
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
            });
        Some(StreamRecord {
            kind: StreamRecordKind::parse(&self.event_name),
            new_image: dynamo.new_image.unwrap_or(Value::Null),
            old_image: dynamo.old_image.unwrap_or(Value::Null),
            table_name,
            sequence_number: dynamo.sequence_number.or(self.sequence_number),
            created_at: dynamo.approximate_creation_date_time,
            event_id: self.event_id,
        })
    }
}

/// Extract the table name from a DynamoDB stream ARN.
///
/// `arn:aws:dynamodb:eu-central-1:123456789012:table/orders/stream/2026-10-25T06:00:00.000`
/// → `orders`.
///
/// ```
/// use worker_shared::ddb_streams::table_from_arn;
///
/// let arn = "arn:aws:dynamodb:eu-central-1:123456789012:table/orders/stream/2026-10-25T06:00:00.000";
/// assert_eq!(table_from_arn(arn).as_deref(), Some("orders"));
/// assert_eq!(table_from_arn("not-an-arn"), None);
/// assert_eq!(table_from_arn("arn:aws:dynamodb:eu-central-1:1:table/"), None);
/// ```
#[must_use]
pub fn table_from_arn(arn: &str) -> Option<String> {
    let resource = arn.split(':').nth(5)?;
    let rest = resource.strip_prefix("table/")?;
    let name = rest.split('/').next()?;
    (!name.is_empty()).then(|| name.to_string())
}
