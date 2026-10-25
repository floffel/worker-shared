//! A cached secret store for Lambda cold-start-sensitive code paths.
//!
//! Every invocation that calls Secrets Manager pays 20–80 ms and eats into the
//! account's request quota, so secrets are cached. A naive cache has three
//! failure modes, and this module handles all three:
//!
//! 1. **Stampede.** A cold start with 32 concurrent invocations of the same
//!    SDK call would issue 32 `GetSecretValue` requests for one secret. The
//!    cache is *single-flight*: the first caller fetches, the rest wait on the
//!    same in-flight future and share its result. Concurrent callers for
//!    *different* secrets do not block each other.
//! 2. **Late refresh.** A secret rotated upstream keeps being served until the
//!    TTL expires, so a rotation can break auth for minutes. [`CacheConfig`]
//!    has a `refresh_margin`: the entry is refreshed that long *before* the
//!    hard deadline, and the TTL is jittered *downwards* per entry so a fleet
//!    that restarted together does not refresh together.
//! 3. **Invalidation races.** A fetch takes time; if an invalidation lands
//!    while it is in flight, storing the result would resurrect the very value
//!    that was just invalidated (secret rotation mid-flight). The cache keeps
//!    a generation counter and drops results that began before the last
//!    invalidation.
//!
//! ```
//! use std::sync::Arc;
//! use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
//! use worker_shared::Error;
//!
//! struct Static(&'static str);
//!
//! #[async_trait::async_trait]
//! impl SecretFetcher for Static {
//!     async fn fetch(&self, _id: &str) -> Result<String, Error> {
//!         Ok(self.0.to_string())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let cache = SecretCache::new(Static("s3cr3t"), CacheConfig::default())?;
//! let first = cache.get("db/password").await?;
//! assert_eq!(first.expose_secret(), "s3cr3t");
//!
//! // Second read is a cache hit — no fetch.
//! let second = cache.get("db/password").await?;
//! assert!(Arc::ptr_eq(&first, &second));
//!
//! cache.invalidate("db/password");
//! assert_eq!(cache.stats().fetches, 1);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use subtle::ConstantTimeEq;

use crate::error::{ConfigError, Error};

/// A secret value that does not leak through `Debug` or `Display`.
///
/// Comparing two secrets with `==` runs in variable time, which leaks their
/// prefix through a timing side channel; use [`SecretString::ct_eq`]. This
/// type deliberately implements neither `PartialEq` nor `Display`.
///
/// The value is **not** zeroed on drop: `#![forbid(unsafe_code)]` rules out a
/// hand-written wipe, and a `zeroize` dependency is a decision the consumer
/// should make. See the security notes in the README.
///
/// ```
/// use worker_shared::SecretString;
///
/// let secret = SecretString::new("hunter2")?;
/// assert_eq!(secret.expose_secret(), "hunter2");
/// assert_eq!(secret.len(), 7);
///
/// // Redacted: a stray `{:?}` in a log line is harmless.
/// assert_eq!(format!("{secret:?}"), "SecretString(\"[redacted]\")");
/// assert!(!format!("{secret:?}").contains("hunter2"));
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Clone)]
pub struct SecretString {
    value: String,
}

impl SecretString {
    /// Wrap a value, rejecting the empty string.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the value is empty: an empty secret is a
    /// misconfiguration, and serving it makes the failure surface later, in a
    /// downstream 401 instead of at load time.
    ///
    /// ```
    /// use worker_shared::SecretString;
    ///
    /// assert!(SecretString::new("value").is_ok());
    /// assert!(SecretString::new("").is_err());
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::Validation("secret value is empty".to_string()));
        }
        Ok(Self { value })
    }

    /// Read the secret.
    ///
    /// The name says "expose" on purpose: every call site is a place where the
    /// value can end up in a log, an error message or a response body.
    ///
    /// ```
    /// use worker_shared::SecretString;
    ///
    /// let secret = SecretString::new("abc")?;
    /// assert_eq!(secret.expose_secret(), "abc");
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.value
    }

    /// Length in bytes.
    ///
    /// ```
    /// use worker_shared::SecretString;
    ///
    /// assert_eq!(SecretString::new("äöü")?.len(), 6);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.value.len()
    }

    /// Whether the value is empty. Always `false`: the constructor rejects
    /// empty values. Provided so clippy's `len_without_is_empty` stays happy
    /// and callers need no special case.
    ///
    /// ```
    /// use worker_shared::SecretString;
    ///
    /// assert!(!SecretString::new("x")?.is_empty());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Constant-time equality.
    ///
    /// Runs in time independent of *where* two values differ, so an API key
    /// comparison cannot be used as a byte-by-byte oracle. Length is compared
    /// first — lengths are not secret.
    ///
    /// ```
    /// use worker_shared::SecretString;
    ///
    /// let a = SecretString::new("key")?;
    /// let b = SecretString::new("key")?;
    /// let c = SecretString::new("kez")?;
    /// assert!(a.ct_eq(&b));
    /// assert!(!a.ct_eq(&c));
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    #[must_use]
    pub fn ct_eq(&self, other: &Self) -> bool {
        let (left, right) = (self.value.as_bytes(), other.value.as_bytes());
        left.len() == right.len() && bool::from(left.ct_eq(right))
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString(\"[redacted]\")")
    }
}

/// Where secrets come from.
///
/// Implement this for your provider; [`crate::secrets_aws`] ships one for AWS
/// Secrets Manager. The trait is object-safe, so `Arc<dyn SecretFetcher>` works
/// as the cache's fetcher.
///
/// ```
/// use worker_shared::secrets::SecretFetcher;
/// use worker_shared::Error;
///
/// struct Fixed;
///
/// #[async_trait::async_trait]
/// impl SecretFetcher for Fixed {
///     async fn fetch(&self, id: &str) -> Result<String, Error> {
///         if id == "known" { Ok("value".to_string()) } else {
///             Err(Error::Secret { id: id.to_string(), message: "unknown".to_string() })
///         }
///     }
/// }
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// assert_eq!(Fixed.fetch("known").await?, "value");
/// assert!(Fixed.fetch("other").await.is_err());
/// # Ok(())
/// # }
/// ```
#[async_trait::async_trait]
pub trait SecretFetcher: Send + Sync {
    /// Fetch the current value of `id`.
    ///
    /// # Errors
    ///
    /// Return [`Error::Throttled`] for a rate limit (retryable) and
    /// [`Error::Secret`] for anything else. The returned message must not
    /// contain the secret value.
    async fn fetch(&self, id: &str) -> Result<String, Error>;
}

/// An `Arc` of any fetcher is itself a fetcher, so a cache can share one
/// backend across tasks without a wrapper type.
///
/// ```
/// use std::sync::Arc;
/// use worker_shared::secrets::SecretFetcher;
/// use worker_shared::Error;
///
/// struct F;
/// # #[async_trait::async_trait]
/// # impl SecretFetcher for F {
/// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
/// # }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let shared: Arc<dyn SecretFetcher> = Arc::new(F);
/// assert_eq!(shared.fetch("x").await?, "v");
/// # Ok(())
/// # }
/// ```
#[async_trait::async_trait]
impl<T: SecretFetcher + ?Sized> SecretFetcher for Arc<T> {
    async fn fetch(&self, id: &str) -> Result<String, Error> {
        (**self).fetch(id).await
    }
}

/// Time and jitter, injectable so cache behaviour is testable without sleeping.
///
/// ```
/// use std::time::{Duration, Instant};
/// use worker_shared::secrets::{Clock, SystemClock};
///
/// let clock = SystemClock::new();
/// let first = clock.now();
/// assert!(clock.now() >= first);
/// // Jitter is always within the requested bound.
/// assert!(clock.jitter(Duration::from_secs(5)) <= Duration::from_secs(5));
/// ```
pub trait Clock: fmt::Debug + Send + Sync {
    /// A monotonic timestamp.
    fn now(&self) -> Instant;

    /// A random duration in `0..=max`, sampled per fetch to spread refreshes.
    fn jitter(&self, max: Duration) -> Duration;
}

/// The production [`Clock`]: monotonic time plus a xorshift jitter source.
///
/// ```
/// use std::time::Duration;
/// use worker_shared::secrets::{Clock, SystemClock};
///
/// let clock = SystemClock::new();
/// assert_eq!(clock.jitter(Duration::ZERO), Duration::ZERO);
/// ```
#[derive(Debug)]
pub struct SystemClock {
    seed: AtomicU64,
}

impl SystemClock {
    /// A clock seeded from the wall clock.
    ///
    /// ```
    /// use worker_shared::secrets::SystemClock;
    ///
    /// let clock = SystemClock::new();
    /// assert!(format!("{clock:?}").starts_with("SystemClock"));
    /// ```
    #[must_use]
    pub fn new() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0x9E37_79B9_7F4A_7C15, |elapsed| {
                u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
            })
            | 1;
        Self {
            seed: AtomicU64::new(seed),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn jitter(&self, max: Duration) -> Duration {
        if max.is_zero() {
            return Duration::ZERO;
        }
        // xorshift64*: cheap, seedable, and not used for anything security
        // relevant — only to desynchronise refresh times across a fleet.
        let mut state = self.seed.load(Ordering::Relaxed);
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        self.seed.store(state, Ordering::Relaxed);
        let value = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        let max_nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX).max(1);
        Duration::from_nanos(value % max_nanos)
    }
}

/// Cache lifetime policy.
///
/// * `ttl` — hard ceiling on how old a served value may be.
/// * `refresh_margin` — refresh this long before that ceiling, so a value is
///   never served right at the edge of a rotation.
/// * `jitter` — each entry's lifetime is shortened by a random amount in
///   `0..=jitter`, which de-synchronises a fleet that started together. Jitter
///   only ever *shortens* an entry's life, so it can never push a served value
///   past `ttl`.
///
/// Effective lifetime per entry:
/// `ttl - refresh_margin - jitter_sample`, saturating at zero. A zero lifetime
/// means "always refetch" — correct, just slow.
///
/// ```
/// use std::time::Duration;
/// use worker_shared::secrets::CacheConfig;
///
/// let config = CacheConfig::new(Duration::from_secs(600))
///     .with_refresh_margin(Duration::from_secs(60))
///     .with_jitter(Duration::from_secs(10));
/// assert_eq!(config.ttl, Duration::from_secs(600));
/// assert_eq!(config.refresh_margin, Duration::from_secs(60));
/// assert_eq!(config.jitter, Duration::from_secs(10));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheConfig {
    /// Hard ceiling on the age of a served value.
    pub ttl: Duration,
    /// Refresh this long before `ttl` expires.
    pub refresh_margin: Duration,
    /// Maximum random shortening of an entry's lifetime.
    pub jitter: Duration,
}

impl CacheConfig {
    /// Five minutes TTL, 30 s refresh margin, 5 s jitter.
    ///
    /// ```
    /// use worker_shared::secrets::CacheConfig;
    ///
    /// assert_eq!(CacheConfig::default().ttl, std::time::Duration::from_secs(300));
    /// ```
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            ..Self::default()
        }
    }

    /// Set the refresh margin.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::secrets::CacheConfig;
    ///
    /// let config = CacheConfig::new(Duration::from_secs(60)).with_refresh_margin(Duration::ZERO);
    /// assert_eq!(config.refresh_margin, Duration::ZERO);
    /// ```
    #[must_use]
    pub fn with_refresh_margin(mut self, margin: Duration) -> Self {
        self.refresh_margin = margin;
        self
    }

    /// Set the maximum jitter.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::secrets::CacheConfig;
    ///
    /// let config = CacheConfig::new(Duration::from_secs(60)).with_jitter(Duration::ZERO);
    /// assert_eq!(config.jitter, Duration::ZERO);
    /// ```
    #[must_use]
    pub fn with_jitter(mut self, jitter: Duration) -> Self {
        self.jitter = jitter;
        self
    }

    /// The lifetime of an entry fetched with `jitter_sample`.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::secrets::CacheConfig;
    ///
    /// let config = CacheConfig::new(Duration::from_secs(100)).with_refresh_margin(Duration::from_secs(20));
    /// assert_eq!(config.lifetime(Duration::from_secs(5)), Duration::from_secs(75));
    /// // Never negative, and never longer than `ttl`.
    /// assert_eq!(config.lifetime(Duration::from_secs(500)), Duration::ZERO);
    /// assert!(config.lifetime(Duration::ZERO) <= config.ttl);
    /// ```
    #[must_use]
    pub fn lifetime(&self, jitter_sample: Duration) -> Duration {
        self.ttl
            .saturating_sub(self.refresh_margin)
            .saturating_sub(jitter_sample)
    }

    fn validate(&self) -> Result<(), Error> {
        if !self.ttl.is_zero() && self.refresh_margin >= self.ttl {
            return Err(Error::Config(ConfigError::Invalid {
                key: "cache.refresh_margin".to_string(),
                message: "must be shorter than the TTL (or TTL must be zero to disable caching)"
                    .to_string(),
            }));
        }
        Ok(())
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            refresh_margin: Duration::from_secs(30),
            jitter: Duration::from_secs(5),
        }
    }
}

#[cfg(feature = "tracing")]
fn log_fetch_failure(id: &str, error: &Error) {
    // The id, never the value: a cache log line must be safe to ship to a
    // third-party log pipeline.
    tracing::warn!(secret_id = %id, error = %error, "secret fetch failed");
}

#[cfg(not(feature = "tracing"))]
fn log_fetch_failure(_id: &str, _error: &Error) {}

/// Counters, for dashboards and for proving cache behaviour in tests.
///
/// ```
/// use worker_shared::secrets::{CacheConfig, CacheStats, SecretCache, SecretFetcher};
/// use worker_shared::Error;
///
/// struct F;
/// # #[async_trait::async_trait]
/// # impl SecretFetcher for F {
/// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
/// # }
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let cache = SecretCache::new(F, CacheConfig::default())?;
/// cache.get("a").await?;
/// cache.get("a").await?;
/// assert_eq!(
///     cache.stats(),
///     CacheStats { hits: 1, coalesced: 0, misses: 1, fetches: 1, failures: 0 },
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Reads served from a fresh entry without waiting.
    pub hits: u64,
    /// Reads that missed but were served by an in-flight fetch for the same
    /// id — the single-flight saving. Each one is a fetch that did not happen.
    pub coalesced: u64,
    /// Reads that had no fresh entry and had to fetch (or wait and fail).
    pub misses: u64,
    /// Actual fetches issued to the backend.
    pub fetches: u64,
    /// Fetches that failed.
    pub failures: u64,
}

#[derive(Debug, Clone)]
struct Entry {
    secret: Arc<SecretString>,
    stored_at: Instant,
    lifetime: Duration,
}

impl Entry {
    fn is_fresh(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.stored_at) < self.lifetime
    }
}

/// A single-flight, TTL- and jitter-aware secret cache.
///
/// `F` is any [`SecretFetcher`]. Every method takes `&self`, the cache is
/// `Send + Sync`, and it is meant to be shared as an `Arc` across tasks.
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
/// use worker_shared::Error;
///
/// struct Counting(std::sync::atomic::AtomicU64);
///
/// #[async_trait::async_trait]
/// impl SecretFetcher for Counting {
///     async fn fetch(&self, id: &str) -> Result<String, Error> {
///         self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
///         Ok(format!("value-for-{id}"))
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let cache = Arc::new(SecretCache::new(
///     Counting(std::sync::atomic::AtomicU64::new(0)),
///     CacheConfig::new(Duration::from_secs(60)).with_jitter(Duration::ZERO),
/// )?);
///
/// let secret = cache.get("api-key").await?;
/// assert_eq!(secret.expose_secret(), "value-for-api-key");
///
/// // 32 concurrent readers, one fetch.
/// let mut handles = Vec::new();
/// for _ in 0..32 {
///     let cache = Arc::clone(&cache);
///     handles.push(tokio::spawn(async move { cache.get("api-key").await.is_ok() }));
/// }
/// for handle in handles {
///     assert!(handle.await.unwrap_or(false));
/// }
/// assert_eq!(cache.stats().fetches, 1);
/// # Ok(())
/// # }
/// ```
pub struct SecretCache<F: SecretFetcher> {
    fetcher: F,
    config: CacheConfig,
    clock: Arc<dyn Clock>,
    entries: Mutex<HashMap<String, Entry>>,
    key_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    generation: AtomicU64,
    hits: AtomicU64,
    coalesced: AtomicU64,
    misses: AtomicU64,
    fetches: AtomicU64,
    failures: AtomicU64,
}

#[allow(
    clippy::missing_fields_in_debug,
    reason = "the fetcher and clock are opaque collaborators; entries are summarised by count"
)]
impl<F: SecretFetcher> fmt::Debug for SecretCache<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretCache")
            .field("config", &self.config)
            .field("entries", &self.len())
            .field("stats", &self.stats())
            .finish()
    }
}

impl<F: SecretFetcher> SecretCache<F> {
    /// Build a cache.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when `refresh_margin >= ttl` — that combination would
    /// mean "never serve from cache", which is far more likely a units mistake
    /// than an intent.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// let bad = CacheConfig::new(Duration::from_secs(10))
    ///     .with_refresh_margin(Duration::from_secs(10));
    /// assert!(SecretCache::new(F, bad).is_err());
    /// assert!(SecretCache::new(F, CacheConfig::default()).is_ok());
    /// ```
    pub fn new(fetcher: F, config: CacheConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self::with_default_clock(fetcher, config))
    }

    fn with_default_clock(fetcher: F, config: CacheConfig) -> Self {
        Self {
            fetcher,
            config,
            clock: Arc::new(SystemClock::new()),
            entries: Mutex::new(HashMap::new()),
            key_locks: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetches: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        }
    }

    /// Replace the clock. Tests use this to jump time without sleeping; in
    /// production the default [`SystemClock`] is right.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::{Duration, Instant};
    /// use worker_shared::secrets::{CacheConfig, Clock, SecretCache, SecretFetcher, SystemClock};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// let cache = SecretCache::new(F, CacheConfig::default())?
    ///     .with_clock(Arc::new(SystemClock::new()));
    /// assert_eq!(cache.len(), 0);
    /// # Ok::<(), Error>(())
    /// ```
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The active policy.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// let cache = SecretCache::new(F, CacheConfig::default())?;
    /// assert_eq!(cache.config().ttl.as_secs(), 300);
    /// # Ok::<(), Error>(())
    /// ```
    #[must_use]
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Fetch `id`, or return the cached value.
    ///
    /// Concurrent callers for the same id share a single fetch; callers for
    /// different ids never wait on each other. Failed fetches are never
    /// cached, so a throttle is retried by the next caller instead of being
    /// remembered for the whole TTL.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] for an empty id, [`Error::EmptySecret`] when the
    /// backend returns an empty value, and whatever the fetcher returned
    /// otherwise.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// let cache = SecretCache::new(F, CacheConfig::default())?;
    /// assert_eq!(cache.get("db").await?.expose_secret(), "v");
    /// assert!(cache.get("  ").await.is_ok()); // whitespace is a legal id
    /// assert!(cache.get("").await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, id: &str) -> Result<Arc<SecretString>, Error> {
        if id.is_empty() {
            return Err(Error::Validation("secret id is empty".to_string()));
        }
        if let Some(hit) = self.lookup(id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(hit);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let lock = self.key_lock(id);
        // Held across the fetch: that is the single-flight guarantee. It is a
        // per-key lock, so unrelated secrets still load in parallel.
        let guard = lock.lock().await;

        // Someone may have filled the cache while this task waited. That is
        // the single-flight win: count it separately from a plain hit.
        if let Some(hit) = self.lookup(id) {
            self.coalesced.fetch_add(1, Ordering::Relaxed);
            drop(guard);
            return Ok(hit);
        }

        let generation = self.generation.load(Ordering::Acquire);
        let secret = match self.fetcher.fetch(id).await {
            Ok(raw) => {
                self.fetches.fetch_add(1, Ordering::Relaxed);
                if let Ok(secret) = SecretString::new(raw) {
                    Arc::new(secret)
                } else {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    drop(guard);
                    return Err(Error::EmptySecret { id: id.to_string() });
                }
            }
            Err(err) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                log_fetch_failure(id, &err);
                drop(guard);
                return Err(err);
            }
        };

        // An invalidation during the fetch means this value may already be
        // stale (rotation mid-flight); store nothing and let the next caller
        // decide.
        if self.generation.load(Ordering::Acquire) == generation {
            let jitter = self
                .clock
                .jitter(self.config.jitter)
                .min(self.config.jitter);
            let entry = Entry {
                secret: Arc::clone(&secret),
                stored_at: self.clock.now(),
                lifetime: self.config.lifetime(jitter),
            };
            self.entries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(id.to_string(), entry);
        }

        drop(guard);
        Ok(secret)
    }

    /// The cached value for `id`, without fetching and without counting a hit.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// let cache = SecretCache::new(F, CacheConfig::default())?;
    /// assert!(cache.peek("db").is_none());
    /// cache.get("db").await?;
    /// assert!(cache.peek("db").is_some());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn peek(&self, id: &str) -> Option<Arc<SecretString>> {
        self.lookup(id)
    }

    /// Drop one entry. Returns whether anything was cached.
    ///
    /// Also bumps the generation, so a fetch that started before this call
    /// will not store its now-suspect result.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// let cache = SecretCache::new(F, CacheConfig::default())?;
    /// cache.get("db").await?;
    /// assert!(cache.invalidate("db"));
    /// assert!(!cache.invalidate("db"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn invalidate(&self, id: &str) -> bool {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id)
            .is_some()
    }

    /// Drop every entry (rotation of several secrets, or a 401 storm).
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// let cache = SecretCache::new(F, CacheConfig::default())?;
    /// cache.get("a").await?;
    /// cache.get("b").await?;
    /// assert_eq!(cache.invalidate_all(), 2);
    /// assert_eq!(cache.len(), 0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn invalidate_all(&self) -> usize {
        self.generation.fetch_add(1, Ordering::AcqRel);
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let count = entries.len();
        entries.clear();
        count
    }

    /// Number of cached entries, including ones that have expired but have not
    /// been touched since.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    /// use std::time::Duration;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// let cache = SecretCache::new(F, CacheConfig::new(Duration::from_secs(60)).with_jitter(Duration::ZERO))?;
    /// cache.get("a").await?;
    /// assert_eq!(cache.len(), 1);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether the cache holds no entries.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// assert!(SecretCache::new(F, CacheConfig::default())?.is_empty());
    /// # Ok::<(), Error>(())
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every expired entry. Returns how many were removed.
    ///
    /// Expiry is already enforced on read; this exists so a long-lived process
    /// that has seen many distinct secret ids can bound memory.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// let cache = SecretCache::new(F, CacheConfig::default())?;
    /// cache.get("a").await?;
    /// assert_eq!(cache.evict_expired(), 0); // still fresh
    /// # Ok(())
    /// # }
    /// ```
    pub fn evict_expired(&self) -> usize {
        let now = self.clock.now();
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let before = entries.len();
        entries.retain(|_, entry| entry.is_fresh(now));
        before - entries.len()
    }

    /// The counters.
    ///
    /// ```
    /// use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher};
    /// use worker_shared::Error;
    ///
    /// struct F;
    /// # #[async_trait::async_trait]
    /// # impl SecretFetcher for F {
    /// #     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("v".into()) }
    /// # }
    ///
    /// let cache = SecretCache::new(F, CacheConfig::default())?;
    /// assert_eq!(cache.stats().fetches, 0);
    /// # Ok::<(), Error>(())
    /// ```
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            coalesced: self.coalesced.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            fetches: self.fetches.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
        }
    }

    fn lookup(&self, id: &str) -> Option<Arc<SecretString>> {
        let now = self.clock.now();
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        match entries.get(id) {
            Some(entry) if entry.is_fresh(now) => Some(Arc::clone(&entry.secret)),
            Some(_) => {
                // Remove on read: a stale entry can never be served again, and
                // keeping it would only grow the map.
                entries.remove(id);
                None
            }
            None => None,
        }
    }

    fn key_lock(&self, id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .key_locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            locks
                .entry(id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }
}
