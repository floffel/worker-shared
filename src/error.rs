//! The crate's single error type.
//!
//! One enum for the whole surface, because a Lambda handler has exactly one
//! place to put a failure: the invocation result. Callers inspect
//! [`Error::is_retryable`] to decide "let the event source retry" versus
//! "send it to the dead-letter queue", and [`Error::is_ambiguous`] to decide
//! whether a retry could duplicate a side effect.
//!
//! No variant carries secret material: secret failures name the *id* and a
//! message, never the value (see the security notes in the crate docs).

use std::time::Duration;

use thiserror::Error;

/// A configuration problem, produced by [`crate::config`].
///
/// Messages name the variable and the reason, never the value: environment
/// variables routinely hold credentials.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A variable the caller marked as required is absent.
    #[error("missing required environment variable `{key}`")]
    Missing {
        /// The variable name.
        key: String,
    },
    /// A variable is present but empty. Treating `""` as a value is how a
    /// missing secret becomes an empty secret, so this fails closed.
    #[error("environment variable `{key}` is empty")]
    Empty {
        /// The variable name.
        key: String,
    },
    /// A variable is present but does not parse as the requested type.
    #[error("environment variable `{key}` is invalid: {message}")]
    Invalid {
        /// The variable name.
        key: String,
        /// Why the value was rejected (never the value itself).
        message: String,
    },
}

impl ConfigError {
    /// The variable name the error is about.
    ///
    /// ```
    /// use worker_shared::ConfigError;
    ///
    /// let err = ConfigError::Missing { key: "QUEUE_URL".into() };
    /// assert_eq!(err.key(), "QUEUE_URL");
    /// ```
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Missing { key } | Self::Empty { key } | Self::Invalid { key, .. } => key,
        }
    }
}

/// Everything that can go wrong in this crate.
///
/// ```
/// use worker_shared::Error;
///
/// let err = Error::Throttled { service: "secretsmanager", retry_after: None };
/// assert!(err.is_retryable());
/// assert!(!err.is_ambiguous());
/// assert_eq!(err.code(), "throttled");
/// assert_eq!(err.to_string(), "upstream secretsmanager rejected the call as throttled");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid configuration; see [`ConfigError`].
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    /// A payload did not have the shape the AWS event contract promises.
    #[error("invalid payload: {0}")]
    Validation(String),

    /// A secret could not be loaded.
    #[error("secret `{id}`: {message}")]
    Secret {
        /// The secret id (name or ARN) — never the value.
        id: String,
        /// What went wrong.
        message: String,
    },

    /// The secret exists but is empty. Empty credentials are a
    /// misconfiguration, not a valid value, so the cache fails closed.
    #[error("secret `{id}` is empty")]
    EmptySecret {
        /// The secret id (name or ARN) — never the value.
        id: String,
    },

    /// A batch carries more entries than the service accepts in one call.
    #[error("batch of {entries} entries exceeds the {limit}-entry limit")]
    TooManyEntries {
        /// How many entries the batch holds.
        entries: usize,
        /// The service limit.
        limit: usize,
    },

    /// A batch exceeds the byte limit for one call.
    #[error("batch of {bytes} bytes exceeds the {limit}-byte limit")]
    BatchTooLarge {
        /// Serialised batch size.
        bytes: usize,
        /// The service limit.
        limit: usize,
    },

    /// A single entry exceeds the byte limit, so no split can fix it.
    #[error("entry {index} of {bytes} bytes exceeds the {limit}-byte limit")]
    EntryTooLarge {
        /// Position of the offending entry in the batch.
        index: usize,
        /// Serialised entry size.
        bytes: usize,
        /// The service limit.
        limit: usize,
    },

    /// The upstream service throttled the call. The request was rejected
    /// before it was processed, so a retry cannot duplicate anything.
    #[error("upstream {service} rejected the call as throttled")]
    Throttled {
        /// The service that throttled (`eventbridge`, `sqs`, ...).
        service: &'static str,
        /// Server-provided backoff hint, when the service sends one.
        retry_after: Option<Duration>,
    },

    /// The upstream call failed. May or may not have been processed — see
    /// [`Self::is_ambiguous`].
    #[error("upstream {service} failed: {message}")]
    Upstream {
        /// The service that failed.
        service: &'static str,
        /// The service's message.
        message: String,
    },

    /// The service answered with a different number of results than entries
    /// were sent. Partial failures cannot be attributed, so the batch is
    /// reported as failed rather than guessed at.
    #[error("upstream {service} returned {actual} results for {expected} entries")]
    ResponseLengthMismatch {
        /// The service that answered.
        service: &'static str,
        /// Entries sent.
        expected: usize,
        /// Results received.
        actual: usize,
    },

    /// The handler's deadline passed while the work was in flight.
    #[error("deadline exceeded")]
    DeadlineExceeded,

    /// The handler's cancellation token fired while the work was in flight.
    #[error("cancelled")]
    Cancelled,

    /// A bug: a state the crate's invariants should have made impossible.
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// A stable, log-friendly code per variant.
    ///
    /// Stable strings are what dashboards group on; the `Display` text may
    /// change with wording, this does not.
    ///
    /// ```
    /// use worker_shared::{ConfigError, Error};
    ///
    /// assert_eq!(Error::Cancelled.code(), "cancelled");
    /// assert_eq!(Error::from(ConfigError::Missing { key: "X".into() }).code(), "config");
    /// ```
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Validation(_) => "validation",
            Self::Secret { .. } => "secret",
            Self::EmptySecret { .. } => "empty_secret",
            Self::TooManyEntries { .. } => "too_many_entries",
            Self::BatchTooLarge { .. } => "batch_too_large",
            Self::EntryTooLarge { .. } => "entry_too_large",
            Self::Throttled { .. } => "throttled",
            Self::Upstream { .. } => "upstream",
            Self::ResponseLengthMismatch { .. } => "response_length_mismatch",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Cancelled => "cancelled",
            Self::Internal(_) => "internal",
        }
    }

    /// Whether retrying the same work can succeed.
    ///
    /// Throttling and transient upstream failures are retryable. Validation
    /// errors, oversized batches and internal errors are terminal. A deadline
    /// or cancellation is terminal *for this invocation*: the event source
    /// re-delivers the batch on its own schedule, so returning
    /// `is_retryable() == false` only says "do not loop inside this
    /// invocation".
    ///
    /// ```
    /// use worker_shared::Error;
    ///
    /// assert!(Error::Upstream { service: "sqs", message: "reset".into() }.is_retryable());
    /// assert!(!Error::Validation("no records".into()).is_retryable());
    /// ```
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Secret { .. } | Self::Throttled { .. } | Self::Upstream { .. }
        )
    }

    /// Whether the request may already have been processed upstream.
    ///
    /// An ambiguous failure is *not* retryable for a non-idempotent call: the
    /// message may have been accepted, and a retry duplicates it. A throttle
    /// is rejected before processing, so it is never ambiguous.
    ///
    /// ```
    /// use worker_shared::Error;
    ///
    /// assert!(Error::DeadlineExceeded.is_ambiguous());
    /// assert!(!Error::Throttled { service: "sqs", retry_after: None }.is_ambiguous());
    /// ```
    #[must_use]
    pub fn is_ambiguous(&self) -> bool {
        matches!(self, Self::DeadlineExceeded | Self::Upstream { .. })
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Validation(format!("JSON: {err}"))
    }
}
