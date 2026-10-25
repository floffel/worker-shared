//! Lambda plumbing that every Rust AWS worker ends up rewriting.
//!
//! A Lambda estate accumulates the same six helpers: a secret cache that does
//! not stampede, EventBridge `PutEvents` batching that respects the 10-entry /
//! 256 KiB limits and reports partial failure, SQS partial-batch responses,
//! envelope parsing for the event sources, one-time initialisation that stays
//! retryable, and env-var config that returns an error instead of panicking.
//! This crate is those helpers, with the product domain stripped out.
//!
//! ```
//! use std::time::Duration;
//! use worker_shared::config::{Config, MapSource};
//! use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
//! use worker_shared::Error;
//!
//! // Secrets: cached, single-flight, refresh-ahead.
//! struct Fetcher;
//!
//! #[async_trait::async_trait]
//! impl SecretFetcher for Fetcher {
//!     async fn fetch(&self, id: &str) -> Result<String, Error> {
//!         Ok(format!("value-of-{id}"))
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! // Config: typed, no panics, no values echoed on error.
//! let config = Config::new(MapSource::from([("SECRET_ID", "prod/api-key")]));
//! let secret_id = config.required("SECRET_ID")?;
//!
//! let cache = SecretCache::new(Fetcher, CacheConfig::default())?;
//! assert_eq!(cache.get(&secret_id).await?.expose_secret(), "value-of-prod/api-key");
//! # Ok(())
//! # }
//! ```
//!
//! # Feature matrix
//!
//! | Feature | Default | What it adds |
//! |---|---|---|
//! | `eventbridge` | yes | envelope parsing, [`eventbridge::Batch`], [`eventbridge::dispatch`] |
//! | `sqs` | yes | [`sqs::SqsBatchEvent`], [`sqs::BatchItemFailures`] |
//! | `ddb` | no | [`ddb_streams`] envelope parsing |
//! | `secrets-aws` | no | [`secrets_aws::SecretsManagerFetcher`] (AWS SDK dependency) |
//! | `tracing` | no | warn-level lines on fetch failure and partial dispatch (never values) |
//!
//! The secret cache, config and handler glue are always compiled: they are the
//! crate's reason to exist and add no heavy dependency.
//!
//! # Security notes
//!
//! * [`SecretString`] has a redacting `Debug`, no `Display`, and no
//!   `PartialEq` — comparisons go through [`secrets::SecretString::ct_eq`], which uses
//!   `subtle` for constant time.
//! * Error messages name the secret *id*, never its value, and
//!   [`ConfigError`] never echoes a variable's contents.
//! * Empty secrets fail closed: [`SecretString::new`] rejects `""`, so a
//!   missing credential is a load-time error instead of a downstream 401.
//! * Failed fetches are never cached, so a throttle cannot be remembered for a
//!   whole TTL.
//! * Values are not zeroised on drop (`#![forbid(unsafe_code)]` rules out a
//!   hand-written wipe); enable `zeroize` in the code that owns the value if
//!   you need that guarantee.
//!
//! # Concurrency
//!
//! Every type is `Send + Sync`, all methods take `&self`, and the only shared
//! state is behind a `Mutex`, an `AtomicU64` or a `tokio::sync` primitive. No
//! lock is held across an `await` except the per-key single-flight guard in
//! [`secrets::SecretCache::get`], which is the point of that guard.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod config;
pub mod error;
pub mod handler;
pub mod secrets;

#[cfg(feature = "ddb")]
pub mod ddb_streams;
#[cfg(feature = "eventbridge")]
pub mod eventbridge;
#[cfg(feature = "secrets-aws")]
pub mod secrets_aws;
#[cfg(feature = "sqs")]
pub mod sqs;

pub use error::{ConfigError, Error};
pub use secrets::SecretString;

/// The imports a Lambda `main.rs` usually needs.
///
/// ```
/// use worker_shared::prelude::*;
///
/// let error = Error::Validation("payload".to_string());
/// assert_eq!(error.code(), "validation");
/// ```
pub mod prelude {
    pub use crate::config::{Config, Env, MapSource, Source};
    pub use crate::error::{ConfigError, Error};
    pub use crate::handler::{Cancellation, CancellationToken, Deadline, Lazy};
    pub use crate::secrets::{
        CacheConfig, CacheStats, Clock, SecretCache, SecretFetcher, SecretString,
    };

    #[cfg(feature = "ddb")]
    pub use crate::ddb_streams::{DdbStreamEvent, StreamRecord, StreamRecordKind};
    #[cfg(feature = "eventbridge")]
    pub use crate::eventbridge::{
        dispatch, Batch, DeliveryReport, Entry, EventBridgeScheduledEvent, EventEnvelope,
        PutEventsResponse, ScheduledTick,
    };
    #[cfg(feature = "secrets-aws")]
    pub use crate::secrets_aws::SecretsManagerFetcher;
    #[cfg(feature = "sqs")]
    pub use crate::sqs::{BatchItemFailures, SqsBatchEvent, SqsMessage};
}
