//! EventBridge envelopes and `PutEvents` batching.
//!
//! Two jobs, both pure logic:
//!
//! * **Inbound** — parse the event Lambda receives. The AWS envelope is
//!   `{"version","id","detail-type","source","account","time","region",
//!   "resources","detail"}`; scheduled rules send `detail-type: "Scheduled
//!   Event"`, `source: "aws.events"` and nothing useful in `detail`.
//!   [`EventBridgeScheduledEvent`] reduces that to the four fields a cron
//!   handler needs. Unknown fields are ignored and unknown `detail-type`s are
//!   carried through, because a rule you did not write may still target your
//!   function.
//! * **Outbound** — build a `PutEvents` call that the service will accept.
//!   The limits are 10 entries and 256 KiB *per call*; exceeding either fails
//!   the whole call with a 400, so [`Batch`] refuses to exist in an
//!   unsendable state and [`plan`] splits a long list before anything is sent.
//!
//! Partial failure is normal for `PutEvents`: the call succeeds and individual
//! entries are rejected. [`dispatch`] maps results back to the entries by
//! position (the only correlation the API offers) and reports exactly which
//! ones to retry.
//!
//! ```
//! use worker_shared::eventbridge::{dispatch, Batch, Entry, FnDispatcher, PutEventsResponse};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), worker_shared::Error> {
//! let mut batch = Batch::new();
//! batch.push(Entry::new("app.orders", "OrderPlaced", serde_json::json!({"id": 7}))?)?;
//!
//! let dispatcher = FnDispatcher::new(|entries: &[Entry]| {
//!     std::future::ready(Ok(PutEventsResponse::all_accepted(entries.len())))
//! });
//!
//! let report = dispatch(&batch, &dispatcher).await?;
//! assert_eq!(report.accepted, 1);
//! assert!(report.is_complete());
//! # Ok(())
//! # }
//! ```

use std::future::Future;

use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::Error;

/// Maximum entries in one `PutEvents` call.
pub const MAX_ENTRIES: usize = 10;

/// Maximum serialised size of one `PutEvents` call: 256 KiB.
pub const MAX_BATCH_BYTES: usize = 256 * 1024;

/// `detail-type` of a scheduled rule.
pub const SCHEDULED_EVENT: &str = "Scheduled Event";

/// A parsed EventBridge envelope.
///
/// Field names follow the wire format (`detail-type`), the Rust names are
/// `snake_case`. Anything unrecognised is ignored; anything optional that is
/// missing stays `None` instead of failing the parse.
///
/// ```
/// use worker_shared::eventbridge::EventEnvelope;
///
/// let raw = r#"{
///   "version": "0",
///   "id": "1a2b",
///   "detail-type": "OrderPlaced",
///   "source": "app.orders",
///   "account": "123456789012",
///   "time": "2026-10-25T06:00:00Z",
///   "region": "eu-central-1",
///   "resources": ["arn:aws:events:eu-central-1:123456789012:rule/nightly"],
///   "detail": {"id": 7},
///   "unexpected": true
/// }"#;
///
/// let envelope = EventEnvelope::parse(raw)?;
/// assert_eq!(envelope.id, "1a2b");
/// assert_eq!(envelope.detail_type, "OrderPlaced");
/// assert_eq!(envelope.detail["id"], 7);
/// assert_eq!(envelope.resources.len(), 1);
/// assert!(envelope.time.is_some());
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Unique id of this event. EventBridge reuses it on retries, which makes
    /// it the natural idempotency key.
    #[serde(default)]
    pub id: String,
    /// The service that emitted the event (`aws.events` for scheduled rules).
    #[serde(default)]
    pub source: String,
    /// The event's type, e.g. `Scheduled Event` or `OrderPlaced`.
    #[serde(default, rename = "detail-type")]
    pub detail_type: String,
    /// When the event was emitted, when it parses as RFC 3339. An unparseable
    /// timestamp is treated as absent rather than failing the whole event: the
    /// envelope is still usable, and the scheduled-event check reports the
    /// missing time itself.
    #[serde(
        default,
        deserialize_with = "de_optional_time",
        serialize_with = "ser_optional_time"
    )]
    pub time: Option<OffsetDateTime>,
    /// Resource ARNs, e.g. the rule that fired.
    #[serde(default)]
    pub resources: Vec<String>,
    /// The event payload.
    #[serde(default)]
    pub detail: Value,
    /// The AWS account the event belongs to.
    #[serde(default)]
    pub account: Option<String>,
    /// The region the event was emitted in.
    #[serde(default)]
    pub region: Option<String>,
    /// The event bus name, when the target is a custom bus.
    #[serde(default, rename = "event-bus-name")]
    pub event_bus_name: Option<String>,
}

impl EventEnvelope {
    /// Parse an envelope from JSON text.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the input is not JSON or an envelope field
    /// has the wrong type.
    ///
    /// ```
    /// use worker_shared::eventbridge::EventEnvelope;
    ///
    /// assert!(EventEnvelope::parse("not json").is_err());
    /// let envelope = EventEnvelope::parse(r#"{"id":"x","source":"s","detail-type":"t"}"#)?;
    /// assert_eq!(envelope.id, "x");
    /// assert_eq!(envelope.detail, serde_json::Value::Null);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn parse(raw: &str) -> Result<Self, Error> {
        Self::from_value(serde_json::from_str(raw)?)
    }

    /// Convert an already-parsed JSON value.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the value is not an envelope.
    ///
    /// ```
    /// use worker_shared::eventbridge::EventEnvelope;
    ///
    /// let envelope = EventEnvelope::from_value(serde_json::json!({"id": "x"}))?;
    /// assert_eq!(envelope.source, "");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn from_value(value: Value) -> Result<Self, Error> {
        if !value.is_object() {
            return Err(Error::Validation(
                "EventBridge event must be a JSON object".to_string(),
            ));
        }
        Ok(serde_json::from_value(value)?)
    }

    /// Whether this envelope was emitted by a scheduled rule
    /// (`detail-type: "Scheduled Event"`).
    ///
    /// ```
    /// use worker_shared::eventbridge::EventEnvelope;
    ///
    /// let scheduled = EventEnvelope::from_value(serde_json::json!({"detail-type": "Scheduled Event"}))?;
    /// assert!(scheduled.is_scheduled());
    ///
    /// let custom = EventEnvelope::from_value(serde_json::json!({"detail-type": "OrderPlaced"}))?;
    /// assert!(!custom.is_scheduled());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn is_scheduled(&self) -> bool {
        self.detail_type == SCHEDULED_EVENT
    }
}

fn de_optional_time<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    Ok(raw.and_then(|value| OffsetDateTime::parse(&value, &Rfc3339).ok()))
}

#[allow(
    clippy::ref_option,
    reason = "serde's `serialize_with` hands the field over as `&Option<T>`; the reference is not ours to choose"
)]
fn ser_optional_time<S>(time: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match time {
        Some(time) => {
            let formatted = time.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&formatted)
        }
        None => serializer.serialize_none(),
    }
}

/// The fields of a scheduled tick that a cron handler needs.
///
/// ```
/// use worker_shared::eventbridge::ScheduledTick;
///
/// let tick = ScheduledTick {
///     event_id: "1a2b".to_string(),
///     rule_arn: "arn:aws:events:eu-central-1:1:rule/nightly".to_string(),
///     time: time::macros::datetime!(2026-10-25 06:00:00 UTC),
///     detail_type: "Scheduled Event".to_string(),
/// };
/// assert_eq!(tick.time.hour(), 6);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledTick {
    /// Stable per-tick id. EventBridge reuses it when it retries the
    /// invocation, so it is the idempotency key.
    pub event_id: String,
    /// ARN of the rule that fired (`resources[0]`).
    pub rule_arn: String,
    /// When the rule fired.
    pub time: OffsetDateTime,
    /// The `detail-type`, preserved so a manual invocation can be told from a
    /// scheduled one.
    pub detail_type: String,
}

/// A scheduled-tick invocation: the envelope reduced to one [`ScheduledTick`].
///
/// ```
/// use worker_shared::eventbridge::EventBridgeScheduledEvent;
///
/// let raw = r#"{
///   "version": "0",
///   "id": "tick-1",
///   "detail-type": "Scheduled Event",
///   "source": "aws.events",
///   "time": "2026-10-25T06:00:00Z",
///   "resources": ["arn:aws:events:eu-central-1:1:rule/nightly"]
/// }"#;
///
/// let event = EventBridgeScheduledEvent::parse(raw)?;
/// assert_eq!(event.tick.event_id, "tick-1");
/// assert_eq!(event.tick.time.unix_timestamp(), 1_792_908_000);
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventBridgeScheduledEvent {
    /// The tick.
    pub tick: ScheduledTick,
}

impl EventBridgeScheduledEvent {
    /// Parse the raw JSON value the Lambda runtime forwarded.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when `id` is empty, when `time` is missing or not
    /// RFC 3339, when `resources[0]` is missing, or when `detail-type` is set
    /// and is not `Scheduled Event`.
    ///
    /// ```
    /// use worker_shared::eventbridge::EventBridgeScheduledEvent;
    ///
    /// let incomplete = serde_json::json!({"id": "x", "resources": ["arn"], "time": "2026-10-25T06:00:00Z"});
    /// // detail-type is optional…
    /// assert!(EventBridgeScheduledEvent::from_value(incomplete).is_ok());
    ///
    /// let wrong_type = serde_json::json!({
    ///     "id": "x",
    ///     "detail-type": "OrderPlaced",
    ///     "resources": ["arn"],
    ///     "time": "2026-10-25T06:00:00Z"
    /// });
    /// assert!(EventBridgeScheduledEvent::from_value(wrong_type).is_err());
    /// ```
    pub fn from_value(value: Value) -> Result<Self, Error> {
        let envelope = EventEnvelope::from_value(value)?;
        Self::from_envelope(envelope)
    }

    /// Parse JSON text.
    ///
    /// # Errors
    ///
    /// As [`Self::from_value`], plus [`Error::Validation`] for malformed JSON.
    ///
    /// ```
    /// use worker_shared::eventbridge::EventBridgeScheduledEvent;
    ///
    /// let event = EventBridgeScheduledEvent::parse(
    ///     r#"{"id":"x","time":"2026-10-25T06:00:00Z","resources":["arn"]}"#,
    /// )?;
    /// assert_eq!(event.tick.rule_arn, "arn");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn parse(raw: &str) -> Result<Self, Error> {
        Self::from_value(serde_json::from_str(raw)?)
    }

    /// Reduce a parsed envelope to a tick, validating the scheduled-event
    /// invariants.
    ///
    /// # Errors
    ///
    /// As [`Self::from_value`].
    ///
    /// ```
    /// use worker_shared::eventbridge::{EventBridgeScheduledEvent, EventEnvelope};
    ///
    /// let envelope = EventEnvelope::parse(
    ///     r#"{"id":"x","time":"2026-10-25T06:00:00Z","resources":["arn"]}"#,
    /// )?;
    /// assert_eq!(EventBridgeScheduledEvent::from_envelope(envelope)?.tick.event_id, "x");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn from_envelope(envelope: EventEnvelope) -> Result<Self, Error> {
        if envelope.id.is_empty() {
            return Err(Error::Validation(
                "EventBridge envelope has no `id`".to_string(),
            ));
        }
        let time = envelope.time.ok_or_else(|| {
            Error::Validation("EventBridge envelope has no RFC 3339 `time`".to_string())
        })?;
        let rule_arn = envelope.resources.first().cloned().ok_or_else(|| {
            Error::Validation("EventBridge envelope has no `resources[0]` rule ARN".to_string())
        })?;
        if !envelope.detail_type.is_empty() && !envelope.is_scheduled() {
            return Err(Error::Validation(format!(
                "EventBridge `detail-type` is `{}`, expected `{SCHEDULED_EVENT}`",
                envelope.detail_type
            )));
        }
        Ok(Self {
            tick: ScheduledTick {
                event_id: envelope.id,
                rule_arn,
                time,
                detail_type: envelope.detail_type,
            },
        })
    }
}

impl TryFrom<Value> for EventBridgeScheduledEvent {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::from_value(value)
    }
}

/// One entry of a `PutEvents` call.
///
/// The detail is kept as JSON *text*: `PutEvents` takes a string, and keeping
/// the caller's exact serialisation is what makes [`Entry::size_bytes`] match
/// what the service counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    source: String,
    detail_type: String,
    detail: String,
    resources: Vec<String>,
    time: Option<OffsetDateTime>,
    event_bus_name: Option<String>,
}

impl Entry {
    /// Build an entry from any serialisable detail.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when `source` or `detail_type` is empty (the
    /// service rejects both) or when the detail cannot be serialised.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// let entry = Entry::new("app.orders", "OrderPlaced", serde_json::json!({"id": 7}))?;
    /// assert_eq!(entry.source(), "app.orders");
    /// assert_eq!(entry.detail(), r#"{"id":7}"#);
    ///
    /// assert!(Entry::new("", "OrderPlaced", serde_json::json!({})).is_err());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn new(
        source: impl Into<String>,
        detail_type: impl Into<String>,
        detail: impl Serialize,
    ) -> Result<Self, Error> {
        Self::with_detail_json(
            source,
            detail_type,
            serde_json::to_string(&detail).map_err(|err| Error::Validation(err.to_string()))?,
        )
    }

    /// Build an entry from detail that is already JSON text.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the detail is not valid JSON, or when
    /// `source`/`detail_type` is empty.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// let entry = Entry::with_detail_json("app", "Ping", "{\"n\":1}")?;
    /// assert_eq!(entry.detail(), "{\"n\":1}");
    ///
    /// assert!(Entry::with_detail_json("app", "Ping", "not json").is_err());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn with_detail_json(
        source: impl Into<String>,
        detail_type: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<Self, Error> {
        let source = source.into();
        let detail_type = detail_type.into();
        let detail = detail.into();
        if source.is_empty() {
            return Err(Error::Validation("entry `source` is empty".to_string()));
        }
        if detail_type.is_empty() {
            return Err(Error::Validation(
                "entry `detail-type` is empty".to_string(),
            ));
        }
        // Validate here so a bad detail fails at build time, not with a
        // service-side 400 in the middle of a batch.
        serde_json::from_str::<Value>(&detail)?;
        Ok(Self {
            source,
            detail_type,
            detail,
            resources: Vec::new(),
            time: None,
            event_bus_name: None,
        })
    }

    /// Attach resource ARNs.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// let entry = Entry::new("app", "Ping", serde_json::json!({}))?
    ///     .with_resources(vec!["arn:aws:events:eu-central-1:1:event-bus/default".to_string()]);
    /// assert_eq!(entry.resources().len(), 1);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn with_resources(mut self, resources: Vec<String>) -> Self {
        self.resources = resources;
        self
    }

    /// Attach an explicit event time (defaults to "now" service-side).
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// let entry = Entry::new("app", "Ping", serde_json::json!({}))?
    ///     .with_time(time::macros::datetime!(2026-10-25 06:00:00 UTC));
    /// assert_eq!(entry.time().map(|t| t.year()), Some(2026));
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn with_time(mut self, time: OffsetDateTime) -> Self {
        self.time = Some(time);
        self
    }

    /// Send to a named event bus instead of `default`.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// let entry = Entry::new("app", "Ping", serde_json::json!({}))?
    ///     .with_event_bus_name("orders");
    /// assert_eq!(entry.event_bus_name(), Some("orders"));
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn with_event_bus_name(mut self, name: impl Into<String>) -> Self {
        self.event_bus_name = Some(name.into());
        self
    }

    /// The `source` field.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// assert_eq!(Entry::new("app", "Ping", serde_json::json!({}))?.source(), "app");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The `detail-type` field.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// assert_eq!(Entry::new("app", "Ping", serde_json::json!({}))?.detail_type(), "Ping");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn detail_type(&self) -> &str {
        &self.detail_type
    }

    /// The detail as JSON text.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// assert_eq!(Entry::new("a", "B", serde_json::json!([1,2]))?.detail(), "[1,2]");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// The resource ARNs.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// assert!(Entry::new("a", "B", serde_json::json!({}))?.resources().is_empty());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn resources(&self) -> &[String] {
        &self.resources
    }

    /// The explicit event time, if set.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// assert!(Entry::new("a", "B", serde_json::json!({}))?.time().is_none());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn time(&self) -> Option<OffsetDateTime> {
        self.time
    }

    /// The event-bus name, if set.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// assert_eq!(Entry::new("a", "B", serde_json::json!({}))?.event_bus_name(), None);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn event_bus_name(&self) -> Option<&str> {
        self.event_bus_name.as_deref()
    }

    /// Serialised size in bytes, as the service counts it.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// let entry = Entry::new("app", "Ping", serde_json::json!({"n": 1}))?;
    /// assert!(entry.size_bytes() > 20);
    /// assert!(entry.size_bytes() < 200);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        serde_json::to_vec(&self.wire()).map_or(usize::MAX, |encoded| encoded.len())
    }

    /// The `PutEvents` wire shape.
    ///
    /// ```
    /// use worker_shared::eventbridge::Entry;
    ///
    /// let entry = Entry::new("app", "Ping", serde_json::json!({"n":1}))?;
    /// let wire = entry.wire();
    /// assert_eq!(wire.source, "app");
    /// assert_eq!(wire.detail_type, "Ping");
    /// assert_eq!(wire.detail, "{\"n\":1}");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn wire(&self) -> WireEntry<'_> {
        WireEntry {
            source: &self.source,
            detail_type: &self.detail_type,
            detail: &self.detail,
            resources: &self.resources,
            time: self
                .time
                .as_ref()
                .and_then(|time| time.format(&Rfc3339).ok()),
            event_bus_name: self.event_bus_name.as_deref(),
        }
    }
}

fn is_empty_slice<T>(slice: &[T]) -> bool {
    slice.is_empty()
}

/// The serde shape of one `PutEvents` entry. Produced by [`Entry::wire`].
///
/// ```
/// use worker_shared::eventbridge::Entry;
///
/// let entry = Entry::new("app", "Ping", serde_json::json!({}))?;
/// let json = serde_json::to_value(entry.wire())?;
/// assert_eq!(json["detail-type"], "Ping");
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct WireEntry<'a> {
    /// The `source` field.
    pub source: &'a str,
    /// The `detail-type` field.
    #[serde(rename = "detail-type")]
    pub detail_type: &'a str,
    /// The detail JSON text.
    pub detail: &'a str,
    /// Resource ARNs; omitted when empty.
    #[serde(skip_serializing_if = "is_empty_slice")]
    pub resources: &'a [String],
    /// RFC 3339 time; omitted when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    /// Target bus; omitted when unset.
    #[serde(skip_serializing_if = "Option::is_none", rename = "event-bus-name")]
    pub event_bus_name: Option<&'a str>,
}

/// A `PutEvents` batch that cannot hold more than the service accepts.
///
/// [`Batch::push`] validates entry size, entry count and total size, so a
/// `Batch` that exists is always sendable.
///
/// ```
/// use worker_shared::eventbridge::{Batch, Entry};
///
/// let mut batch = Batch::new();
/// batch.push(Entry::new("app", "Ping", serde_json::json!({"n": 1}))?)?;
/// assert_eq!(batch.len(), 1);
/// assert!(!batch.is_empty());
/// assert!(batch.total_bytes() > 0);
/// assert!(batch.total_bytes() <= worker_shared::eventbridge::MAX_BATCH_BYTES);
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
    entries: Vec<Entry>,
    bytes: usize,
}

impl Batch {
    /// An empty batch.
    ///
    /// ```
    /// use worker_shared::eventbridge::Batch;
    ///
    /// assert!(Batch::new().is_empty());
    /// assert_eq!(Batch::default().len(), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an entry, enforcing both limits.
    ///
    /// # Errors
    ///
    /// [`Error::EntryTooLarge`] when the entry alone exceeds
    /// [`MAX_BATCH_BYTES`], [`Error::TooManyEntries`] when the batch would hold
    /// more than [`MAX_ENTRIES`], [`Error::BatchTooLarge`] when the serialised
    /// total would exceed [`MAX_BATCH_BYTES`]. A rejected entry is not added.
    ///
    /// ```
    /// use worker_shared::eventbridge::{Batch, Entry, MAX_ENTRIES};
    ///
    /// let mut batch = Batch::new();
    /// for index in 0..MAX_ENTRIES {
    ///     batch.push(Entry::new("app", "Ping", serde_json::json!({"i": index}))?)?;
    /// }
    /// let eleventh = batch.push(Entry::new("app", "Ping", serde_json::json!({}))?);
    /// assert!(eleventh.is_err());
    /// assert_eq!(batch.len(), MAX_ENTRIES);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn push(&mut self, entry: Entry) -> Result<(), Error> {
        let index = self.entries.len();
        let size = entry.size_bytes();
        if size > MAX_BATCH_BYTES {
            return Err(Error::EntryTooLarge {
                index,
                bytes: size,
                limit: MAX_BATCH_BYTES,
            });
        }
        if self.entries.len() >= MAX_ENTRIES {
            return Err(Error::TooManyEntries {
                entries: self.entries.len() + 1,
                limit: MAX_ENTRIES,
            });
        }
        let total = self.bytes.saturating_add(size);
        if total > MAX_BATCH_BYTES {
            return Err(Error::BatchTooLarge {
                bytes: total,
                limit: MAX_BATCH_BYTES,
            });
        }
        self.entries.push(entry);
        self.bytes = total;
        Ok(())
    }

    /// Number of entries.
    ///
    /// ```
    /// use worker_shared::eventbridge::{Batch, Entry};
    ///
    /// let mut batch = Batch::new();
    /// batch.push(Entry::new("a", "B", serde_json::json!({}))?)?;
    /// assert_eq!(batch.len(), 1);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the batch holds no entries.
    ///
    /// ```
    /// use worker_shared::eventbridge::Batch;
    ///
    /// assert!(Batch::new().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialised size of all entries, in bytes.
    ///
    /// ```
    /// use worker_shared::eventbridge::{Batch, Entry};
    ///
    /// let mut batch = Batch::new();
    /// batch.push(Entry::new("a", "B", serde_json::json!({}))?)?;
    /// assert_eq!(batch.total_bytes(), batch.entries()[0].size_bytes());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.bytes
    }

    /// The entries.
    ///
    /// ```
    /// use worker_shared::eventbridge::{Batch, Entry};
    ///
    /// let mut batch = Batch::new();
    /// batch.push(Entry::new("a", "B", serde_json::json!({}))?)?;
    /// assert_eq!(batch.entries()[0].source(), "a");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Consume the batch.
    ///
    /// ```
    /// use worker_shared::eventbridge::{Batch, Entry};
    ///
    /// let mut batch = Batch::new();
    /// batch.push(Entry::new("a", "B", serde_json::json!({}))?)?;
    /// assert_eq!(batch.into_entries().len(), 1);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn into_entries(self) -> Vec<Entry> {
        self.entries
    }
}

impl TryFrom<Vec<Entry>> for Batch {
    type Error = Error;

    fn try_from(entries: Vec<Entry>) -> Result<Self, Self::Error> {
        let mut batch = Self::new();
        for entry in entries {
            batch.push(entry)?;
        }
        Ok(batch)
    }
}

/// Split entries into service-sized batches.
///
/// # Errors
///
/// [`Error::EntryTooLarge`] when a single entry cannot fit in any batch (no
/// split can fix that).
///
/// ```
/// use worker_shared::eventbridge::{plan, Entry, MAX_ENTRIES};
///
/// let entries: Vec<Entry> = (0..MAX_ENTRIES + 5)
///     .map(|i| Entry::new("app", "Ping", serde_json::json!({"i": i})))
///     .collect::<Result<_, _>>()?;
///
/// let batches = plan(entries)?;
/// assert_eq!(batches.len(), 2);
/// assert_eq!(batches[0].len(), MAX_ENTRIES);
/// assert_eq!(batches[1].len(), 5);
/// # Ok::<(), worker_shared::Error>(())
/// ```
pub fn plan(entries: impl IntoIterator<Item = Entry>) -> Result<Vec<Batch>, Error> {
    let mut batches = Vec::new();
    let mut current = Batch::new();

    for (index, entry) in entries.into_iter().enumerate() {
        let size = entry.size_bytes();
        // A single entry that cannot fit is fatal, whatever the split.
        if size > MAX_BATCH_BYTES {
            return Err(Error::EntryTooLarge {
                index,
                bytes: size,
                limit: MAX_BATCH_BYTES,
            });
        }
        let full = current.len() >= MAX_ENTRIES
            || current.total_bytes().saturating_add(size) > MAX_BATCH_BYTES;
        if full {
            batches.push(std::mem::replace(&mut current, Batch::new()));
        }
        current.push(entry)?;
    }

    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

/// The outcome of one entry in a `PutEvents` response.
///
/// ```
/// use worker_shared::eventbridge::EntryResult;
///
/// let accepted = EntryResult::accepted("evt-1");
/// assert!(accepted.is_accepted());
///
/// let failed = EntryResult::failed("ThrottlingException", "slow down");
/// assert_eq!(failed.code(), "ThrottlingException");
/// assert!(failed.is_retryable());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryResult {
    /// The service accepted the entry.
    Accepted {
        /// The event id the service assigned, when it sends one.
        event_id: Option<String>,
    },
    /// The service rejected the entry.
    Failed {
        /// The service's error code.
        code: String,
        /// The service's message.
        message: String,
    },
}

impl EntryResult {
    /// An accepted result.
    ///
    /// ```
    /// use worker_shared::eventbridge::EntryResult;
    ///
    /// assert_eq!(EntryResult::accepted("id").event_id(), Some("id"));
    /// ```
    #[must_use]
    pub fn accepted(event_id: impl Into<String>) -> Self {
        Self::Accepted {
            event_id: Some(event_id.into()),
        }
    }

    /// A rejected result.
    ///
    /// ```
    /// use worker_shared::eventbridge::EntryResult;
    ///
    /// assert!(!EntryResult::failed("InternalFailure", "boom").is_accepted());
    /// ```
    #[must_use]
    pub fn failed(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Whether the entry was accepted.
    ///
    /// ```
    /// use worker_shared::eventbridge::EntryResult;
    ///
    /// assert!(EntryResult::Accepted { event_id: None }.is_accepted());
    /// ```
    #[must_use]
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    /// The event id, when accepted with one.
    ///
    /// ```
    /// use worker_shared::eventbridge::EntryResult;
    ///
    /// assert_eq!(EntryResult::Accepted { event_id: None }.event_id(), None);
    /// ```
    #[must_use]
    pub fn event_id(&self) -> Option<&str> {
        match self {
            Self::Accepted { event_id } => event_id.as_deref(),
            Self::Failed { .. } => None,
        }
    }

    /// The error code, empty for accepted entries.
    ///
    /// ```
    /// use worker_shared::eventbridge::EntryResult;
    ///
    /// assert_eq!(EntryResult::accepted("id").code(), "");
    /// ```
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::Accepted { .. } => "",
            Self::Failed { code, .. } => code,
        }
    }

    /// Whether this rejection is worth retrying.
    ///
    /// EventBridge signals throttling and transient backend trouble with a
    /// small, documented set of codes; everything else is a permanent problem
    /// with the entry itself and will fail again.
    ///
    /// ```
    /// use worker_shared::eventbridge::EntryResult;
    ///
    /// assert!(EntryResult::failed("ThrottlingException", "").is_retryable());
    /// assert!(!EntryResult::failed("MalformedDetail", "").is_retryable());
    /// ```
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Failed { code, .. } if code_is_retryable(code))
    }
}

/// The response of one `PutEvents` call.
///
/// ```
/// use worker_shared::eventbridge::PutEventsResponse;
///
/// let response = PutEventsResponse::all_accepted(3);
/// assert_eq!(response.results.len(), 3);
/// assert!(response.results.iter().all(worker_shared::eventbridge::EntryResult::is_accepted));
///
/// let empty = PutEventsResponse::empty();
/// assert!(empty.results.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutEventsResponse {
    /// One result per sent entry, in the order they were sent.
    pub results: Vec<EntryResult>,
}

impl PutEventsResponse {
    /// A response with no results (the reply to an empty batch).
    ///
    /// ```
    /// use worker_shared::eventbridge::PutEventsResponse;
    ///
    /// assert!(PutEventsResponse::empty().results.is_empty());
    /// ```
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// `count` accepted results without event ids.
    ///
    /// ```
    /// use worker_shared::eventbridge::PutEventsResponse;
    ///
    /// assert_eq!(PutEventsResponse::all_accepted(2).results.len(), 2);
    /// ```
    #[must_use]
    pub fn all_accepted(count: usize) -> Self {
        Self {
            results: (0..count)
                .map(|_| EntryResult::Accepted { event_id: None })
                .collect(),
        }
    }

    /// Wrap explicit results.
    ///
    /// ```
    /// use worker_shared::eventbridge::{EntryResult, PutEventsResponse};
    ///
    /// let response = PutEventsResponse::from_results(vec![EntryResult::accepted("a")]);
    /// assert_eq!(response.results[0].event_id(), Some("a"));
    /// ```
    #[must_use]
    pub fn from_results(results: Vec<EntryResult>) -> Self {
        Self { results }
    }
}

/// Send `PutEvents` calls.
///
/// Implement this over your SDK client; [`FnDispatcher`] covers tests and
/// simple cases.
///
/// ```
/// use worker_shared::eventbridge::{Dispatcher, Entry, FnDispatcher, PutEventsResponse};
/// use worker_shared::Error;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let dispatcher = FnDispatcher::new(|entries: &[Entry]| {
///     std::future::ready(Ok::<PutEventsResponse, Error>(
///         PutEventsResponse::all_accepted(entries.len()),
///     ))
/// });
/// let entry = Entry::new("a", "B", serde_json::json!({}))?;
/// let reply = dispatcher.put_events(&[entry]).await?;
/// assert_eq!(reply.results.len(), 1);
/// # Ok(())
/// # }
/// ```
#[async_trait::async_trait]
pub trait Dispatcher: Send + Sync {
    /// Send one batch. `entries` never exceeds the service limits.
    ///
    /// # Errors
    ///
    /// [`Error::Throttled`] for a rate limit, [`Error::Upstream`] for anything
    /// else the service reports at call level. Entry-level rejections belong
    /// in the returned [`PutEventsResponse`], not here.
    async fn put_events(&self, entries: &[Entry]) -> Result<PutEventsResponse, Error>;
}

/// An `Arc` of any dispatcher is itself a dispatcher.
///
/// ```
/// use std::sync::Arc;
/// use worker_shared::eventbridge::{Dispatcher, Entry, FnDispatcher, PutEventsResponse};
/// use worker_shared::Error;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let inner = FnDispatcher::new(|entries: &[Entry]| {
///     std::future::ready(Ok::<PutEventsResponse, Error>(
///         PutEventsResponse::all_accepted(entries.len()),
///     ))
/// });
/// let shared: Arc<dyn Dispatcher> = Arc::new(inner);
/// let entry = Entry::new("a", "B", serde_json::json!({}))?;
/// assert_eq!(shared.put_events(&[entry]).await?.results.len(), 1);
/// # Ok(())
/// # }
/// ```
#[async_trait::async_trait]
impl<T: Dispatcher + ?Sized> Dispatcher for std::sync::Arc<T> {
    async fn put_events(&self, entries: &[Entry]) -> Result<PutEventsResponse, Error> {
        (**self).put_events(entries).await
    }
}

/// A [`Dispatcher`] from an async closure.
///
/// ```
/// use worker_shared::eventbridge::{Dispatcher, Entry, FnDispatcher, PutEventsResponse};
/// use worker_shared::Error;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let dispatcher = FnDispatcher::new(|entries: &[Entry]| {
///     std::future::ready(Ok::<PutEventsResponse, Error>(
///         PutEventsResponse::all_accepted(entries.len()),
///     ))
/// });
/// let reply = dispatcher.put_events(&[]).await?;
/// assert!(reply.results.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct FnDispatcher<F> {
    inner: F,
}

impl<F> FnDispatcher<F> {
    /// Wrap a closure.
    ///
    /// ```
    /// use worker_shared::eventbridge::{Entry, FnDispatcher, PutEventsResponse};
    /// use worker_shared::Error;
    ///
    /// let dispatcher = FnDispatcher::new(|entries: &[Entry]| {
    ///     std::future::ready(Ok::<PutEventsResponse, Error>(
    ///         PutEventsResponse::all_accepted(entries.len()),
    ///     ))
    /// });
    /// assert!(std::ptr::eq(&dispatcher, &dispatcher));
    /// ```
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<F, Fut> Dispatcher for FnDispatcher<F>
where
    F: Fn(&[Entry]) -> Fut + Send + Sync,
    Fut: Future<Output = Result<PutEventsResponse, Error>> + Send,
{
    async fn put_events(&self, entries: &[Entry]) -> Result<PutEventsResponse, Error> {
        (self.inner)(entries).await
    }
}

/// One entry that the service rejected.
///
/// ```
/// use worker_shared::eventbridge::EntryFailure;
///
/// let failure = EntryFailure {
///     index: 2,
///     code: "ThrottlingException".to_string(),
///     message: "rate exceeded".to_string(),
/// };
/// assert!(failure.is_retryable());
/// assert_eq!(failure.to_string(), "entry 2 failed with ThrottlingException: rate exceeded");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryFailure {
    /// Position of the entry in the batch that was sent.
    pub index: usize,
    /// The service's error code.
    pub code: String,
    /// The service's message.
    pub message: String,
}

impl EntryFailure {
    /// Whether retrying this entry can succeed.
    ///
    /// ```
    /// use worker_shared::eventbridge::EntryFailure;
    ///
    /// assert!(!EntryFailure { index: 0, code: "MalformedDetail".into(), message: String::new() }.is_retryable());
    /// ```
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        code_is_retryable(&self.code)
    }
}

impl std::fmt::Display for EntryFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "entry {} failed with {}: {}",
            self.index, self.code, self.message
        )
    }
}

/// Which entries of a dispatched batch made it.
///
/// ```
/// use worker_shared::eventbridge::DeliveryReport;
///
/// let report = DeliveryReport::default();
/// assert!(report.is_complete());
/// assert_eq!(report.total(), 0);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliveryReport {
    /// Entries the service accepted.
    pub accepted: usize,
    /// Entries the service rejected, in batch order.
    pub failures: Vec<EntryFailure>,
}

impl DeliveryReport {
    /// Whether every entry was accepted.
    ///
    /// ```
    /// use worker_shared::eventbridge::{DeliveryReport, EntryFailure};
    ///
    /// let report = DeliveryReport { accepted: 1, failures: vec![] };
    /// assert!(report.is_complete());
    ///
    /// let partial = DeliveryReport {
    ///     accepted: 1,
    ///     failures: vec![EntryFailure { index: 1, code: "x".into(), message: String::new() }],
    /// };
    /// assert!(!partial.is_complete());
    /// ```
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    /// Accepted plus rejected.
    ///
    /// ```
    /// use worker_shared::eventbridge::{DeliveryReport, EntryFailure};
    ///
    /// let report = DeliveryReport {
    ///     accepted: 2,
    ///     failures: vec![EntryFailure { index: 0, code: "x".into(), message: String::new() }],
    /// };
    /// assert_eq!(report.total(), 3);
    /// ```
    #[must_use]
    pub fn total(&self) -> usize {
        self.accepted + self.failures.len()
    }

    /// Batch positions of the rejected entries, ascending.
    ///
    /// ```
    /// use worker_shared::eventbridge::{DeliveryReport, EntryFailure};
    ///
    /// let report = DeliveryReport {
    ///     accepted: 1,
    ///     failures: vec![
    ///         EntryFailure { index: 2, code: "x".into(), message: String::new() },
    ///         EntryFailure { index: 0, code: "x".into(), message: String::new() },
    ///     ],
    /// };
    /// let mut indexes: Vec<usize> = report.failed_indexes().collect();
    /// indexes.sort_unstable();
    /// assert_eq!(indexes, vec![0, 2]);
    /// ```
    pub fn failed_indexes(&self) -> impl Iterator<Item = usize> + '_ {
        self.failures.iter().map(|failure| failure.index)
    }

    /// The rejected entries that are worth retrying.
    ///
    /// ```
    /// use worker_shared::eventbridge::{DeliveryReport, EntryFailure};
    ///
    /// let report = DeliveryReport {
    ///     accepted: 0,
    ///     failures: vec![
    ///         EntryFailure { index: 0, code: "ThrottlingException".into(), message: String::new() },
    ///         EntryFailure { index: 1, code: "MalformedDetail".into(), message: String::new() },
    ///     ],
    /// };
    /// let retryable: Vec<usize> = report.retryable_indexes().collect();
    /// assert_eq!(retryable, vec![0]);
    /// ```
    pub fn retryable_indexes(&self) -> impl Iterator<Item = usize> + '_ {
        self.failures
            .iter()
            .filter(|failure| failure.is_retryable())
            .map(|failure| failure.index)
    }
}

/// Send one batch and report per-entry outcomes.
///
/// Nothing is sent for an empty batch, and the batch is re-validated first, so
/// the service limit can never be exceeded by a caller that built a [`Batch`]
/// by hand.
///
/// # Errors
///
/// [`Error::TooManyEntries`] / [`Error::BatchTooLarge`] when the batch is not
/// sendable, [`Error::ResponseLengthMismatch`] when the service answers with
/// the wrong number of results (partial failures cannot then be attributed),
/// and whatever the dispatcher returned.
///
/// ```
/// use worker_shared::eventbridge::{
///     dispatch, Batch, DeliveryReport, Entry, EntryResult, FnDispatcher, PutEventsResponse,
/// };
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), worker_shared::Error> {
/// let mut batch = Batch::new();
/// batch.push(Entry::new("app", "Ping", serde_json::json!({"n": 1}))?)?;
/// batch.push(Entry::new("app", "Ping", serde_json::json!({"n": 2}))?)?;
///
/// // The second entry is throttled; the first is fine.
/// let dispatcher = FnDispatcher::new(|_entries: &[Entry]| {
///     std::future::ready(Ok(PutEventsResponse::from_results(vec![
///         EntryResult::accepted("evt-1"),
///         EntryResult::failed("ThrottlingException", "slow down"),
///     ])))
/// });
///
/// let report: DeliveryReport = dispatch(&batch, &dispatcher).await?;
/// assert_eq!(report.accepted, 1);
/// assert_eq!(report.retryable_indexes().collect::<Vec<_>>(), vec![1]);
/// # Ok(())
/// # }
/// ```
pub async fn dispatch<D: Dispatcher + ?Sized>(
    batch: &Batch,
    dispatcher: &D,
) -> Result<DeliveryReport, Error> {
    // No size check here on purpose: a `Batch` cannot exist outside the
    // service limits (see `Batch::push` and `plan`), so re-checking would be
    // an unreachable branch pretending to be defence in depth.
    if batch.is_empty() {
        return Ok(DeliveryReport::default());
    }

    let response = dispatcher.put_events(batch.entries()).await?;
    if response.results.len() != batch.len() {
        return Err(Error::ResponseLengthMismatch {
            service: "eventbridge",
            expected: batch.len(),
            actual: response.results.len(),
        });
    }

    let mut report = DeliveryReport::default();
    for (index, result) in response.results.into_iter().enumerate() {
        match result {
            EntryResult::Accepted { .. } => report.accepted += 1,
            EntryResult::Failed { code, message } => report.failures.push(EntryFailure {
                index,
                code,
                message,
            }),
        }
    }
    Ok(report)
}

/// Whether an EventBridge error code is transient.
///
/// ```
/// use worker_shared::eventbridge::code_is_retryable;
///
/// assert!(code_is_retryable("ThrottlingException"));
/// assert!(code_is_retryable("InternalFailure"));
/// assert!(!code_is_retryable("MalformedDetail"));
/// ```
#[must_use]
pub fn code_is_retryable(code: &str) -> bool {
    matches!(
        code,
        "ThrottlingException"
            | "InternalFailure"
            | "InternalException"
            | "ProvisionedThroughputExceededException"
            | "ServiceUnavailable"
            | "ServiceUnavailableException"
    )
}
