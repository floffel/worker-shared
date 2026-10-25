//! Configuration from environment variables, with typed errors instead of
//! panics.
//!
//! Lambda configuration arrives as environment variables, and the usual
//! `std::env::var("X").unwrap()` turns a typo at deploy time into a panic on
//! the first cold start — inside a retry loop nobody is watching. This module
//! returns [`ConfigError`] instead, and names the offending variable without
//! ever echoing its value.
//!
//! Reading goes through the [`Source`] trait, so tests configure a
//! [`MapSource`] and never touch the process environment (which is global,
//! unsynchronised state — the exact thing a parallel test suite must not
//! share).
//!
//! ```
//! use worker_shared::config::{Config, MapSource};
//!
//! let config = Config::new(MapSource::from([
//!     ("QUEUE_URL", "https://sqs.eu-central-1.amazonaws.com/1/q"),
//!     ("BATCH_SIZE", "10"),
//!     ("GRACE", "5s"),
//! ]));
//!
//! assert_eq!(config.required("QUEUE_URL")?, "https://sqs.eu-central-1.amazonaws.com/1/q");
//! assert_eq!(config.required_parse::<usize>("BATCH_SIZE")?, 10);
//! assert_eq!(config.duration("GRACE")?, Some(std::time::Duration::from_secs(5)));
//! assert_eq!(config.optional("MISSING"), None);
//! # Ok::<(), worker_shared::Error>(())
//! ```

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use crate::error::ConfigError;

/// Where configuration values come from.
///
/// ```
/// use worker_shared::config::Source;
///
/// struct One(&'static str);
///
/// impl Source for One {
///     fn get(&self, key: &str) -> Option<String> {
///         (key == "ONLY").then(|| "yes".to_string())
///     }
/// }
///
/// assert_eq!(One("ONLY").get("ONLY").as_deref(), Some("yes"));
/// assert_eq!(One("ONLY").get("OTHER"), None);
/// ```
pub trait Source: Send + Sync {
    /// Return the value for `key`, or `None` when the variable is absent.
    ///
    /// An empty value should be returned as-is; [`Config`] decides whether
    /// empty counts as missing (it does, for required keys).
    fn get(&self, key: &str) -> Option<String>;
}

/// The process environment.
///
/// ```
/// use worker_shared::config::Env;
/// use worker_shared::config::Source;
///
/// // Never panics: a non-Unicode value is simply absent.
/// let _ = Env.get("PATH");
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct Env;

impl Source for Env {
    fn get(&self, key: &str) -> Option<String> {
        // A non-Unicode value is unusable as configuration; reporting it as
        // absent turns it into a typed error downstream rather than a panic.
        std::env::var(key).ok()
    }
}

/// An in-memory [`Source`], for tests and for embedding configuration in a
/// binary.
///
/// ```
/// use worker_shared::config::{Config, MapSource};
///
/// let config = Config::new(MapSource::from([("MODE", "fast")]));
/// assert_eq!(config.required("MODE")?, "fast");
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct MapSource {
    values: HashMap<String, String>,
}

impl Source for MapSource {
    fn get(&self, key: &str) -> Option<String> {
        self.values.get(key).cloned()
    }
}

impl MapSource {
    /// An empty source.
    ///
    /// ```
    /// use worker_shared::config::{MapSource, Source};
    ///
    /// assert_eq!(MapSource::new().get("ANY"), None);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a value.
    ///
    /// ```
    /// use worker_shared::config::{MapSource, Source};
    ///
    /// let source = MapSource::new().with("KEY", "value");
    /// assert_eq!(source.get("KEY").as_deref(), Some("value"));
    /// ```
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.values.insert(key.into(), value.into());
        self
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for MapSource {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut source = Self::new();
        for (key, value) in iter {
            source = source.with(key, value);
        }
        source
    }
}

impl<K: Into<String>, V: Into<String>, const N: usize> From<[(K, V); N]> for MapSource {
    fn from(pairs: [(K, V); N]) -> Self {
        pairs.into_iter().collect()
    }
}

/// A typed view over a [`Source`].
///
/// ```
/// use worker_shared::config::{Config, MapSource};
///
/// let config = Config::new(MapSource::from([
///     ("TIMEOUT", "30s"),
///     ("RETRIES", "3"),
///     ("DEBUG", "on"),
/// ]));
///
/// assert_eq!(config.duration("TIMEOUT")?, Some(std::time::Duration::from_secs(30)));
/// assert_eq!(config.u64("RETRIES")?, Some(3));
/// assert!(config.flag("DEBUG", false)?);
/// assert!(config.flag("VERBOSE", true)?); // default
/// # Ok::<(), worker_shared::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct Config<S: Source = Env> {
    source: S,
}

impl Config<Env> {
    /// Read the process environment.
    ///
    /// ```
    /// use worker_shared::config::Config;
    ///
    /// let config = Config::from_env();
    /// // `PATH` is set in every sane environment; `optional` never fails.
    /// assert!(config.optional("PATH").is_some());
    /// ```
    #[must_use]
    pub fn from_env() -> Self {
        Self { source: Env }
    }
}

impl<S: Source> Config<S> {
    /// Wrap a source.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::default());
    /// assert_eq!(config.optional("NOPE"), None);
    /// ```
    pub fn new(source: S) -> Self {
        Self { source }
    }

    /// The wrapped source.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource, Source};
    ///
    /// let config = Config::new(MapSource::from([("K", "v")]));
    /// assert_eq!(config.source().get("K").as_deref(), Some("v"));
    /// ```
    pub fn source(&self) -> &S {
        &self.source
    }

    /// The raw value, or `None` when absent or empty.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::from([("EMPTY", "")]));
    /// assert_eq!(config.optional("EMPTY"), None);
    /// ```
    #[must_use]
    pub fn optional(&self, key: &str) -> Option<String> {
        self.source.get(key).filter(|value| !value.is_empty())
    }

    /// The raw value, failing when it is absent or empty.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Missing`] when the variable is absent,
    /// [`ConfigError::Empty`] when it is present but empty.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::from([("EMPTY", "")]));
    /// assert_eq!(config.required("EMPTY").unwrap_err().key(), "EMPTY");
    /// assert!(config.required("ABSENT").is_err());
    /// ```
    pub fn required(&self, key: &str) -> Result<String, ConfigError> {
        match self.source.get(key) {
            None => Err(ConfigError::Missing {
                key: key.to_string(),
            }),
            Some(value) if value.is_empty() => Err(ConfigError::Empty {
                key: key.to_string(),
            }),
            Some(value) => Ok(value),
        }
    }

    /// Parse an optional value with [`FromStr`].
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] when the value does not parse. The error never
    /// contains the value, only the parser's complaint.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::from([("PORT", "abc")]));
    /// let err = config.parse::<u16>("PORT").unwrap_err();
    /// assert_eq!(err.key(), "PORT");
    /// assert_eq!(config.parse::<u16>("UNSET")?, None);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn parse<T>(&self, key: &str) -> Result<Option<T>, ConfigError>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        match self.optional(key) {
            None => Ok(None),
            Some(value) => value
                .parse::<T>()
                .map(Some)
                .map_err(|err| ConfigError::Invalid {
                    key: key.to_string(),
                    message: err.to_string(),
                }),
        }
    }

    /// Parse a required value with [`FromStr`].
    ///
    /// # Errors
    ///
    /// [`ConfigError::Missing`], [`ConfigError::Empty`], or
    /// [`ConfigError::Invalid`], as for [`Self::required`] and
    /// [`Self::parse`].
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::from([("N", "42")]));
    /// assert_eq!(config.required_parse::<u8>("N")?, 42);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn required_parse<T>(&self, key: &str) -> Result<T, ConfigError>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        let raw = self.required(key)?;
        raw.parse::<T>().map_err(|err| ConfigError::Invalid {
            key: key.to_string(),
            message: err.to_string(),
        })
    }

    /// Parse an optional unsigned integer.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] when the value is not a `u64`.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::from([("BATCH", "10")]));
    /// assert_eq!(config.u64("BATCH")?, Some(10));
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn u64(&self, key: &str) -> Result<Option<u64>, ConfigError> {
        self.parse(key)
    }

    /// Parse an optional boolean.
    ///
    /// Accepts `1/0`, `true/false`, `yes/no`, `on/off`, any case.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] for anything else — deliberately strict, so
    /// `TRACE=enabled` does not silently read as `false`.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::from([("A", "YES"), ("B", "0")]));
    /// assert_eq!(config.bool("A")?, Some(true));
    /// assert_eq!(config.bool("B")?, Some(false));
    /// assert!(config.bool("C")?.is_none());
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn bool(&self, key: &str) -> Result<Option<bool>, ConfigError> {
        match self.optional(key) {
            None => Ok(None),
            Some(value) => parse_bool(&value)
                .map(Some)
                .ok_or_else(|| ConfigError::Invalid {
                    key: key.to_string(),
                    message: "expected one of 1/0, true/false, yes/no, on/off".to_string(),
                }),
        }
    }

    /// Parse an optional boolean, falling back to `default`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] when the value is not a recognised boolean.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::default());
    /// assert!(config.flag("VERBOSE", true)?);
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn flag(&self, key: &str, default: bool) -> Result<bool, ConfigError> {
        Ok(self.bool(key)?.unwrap_or(default))
    }

    /// Parse an optional duration. Bare integers are seconds; suffixes `ms`,
    /// `s`, `m`, `h`, `d` are accepted, as are combinations like `1h30m`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] when the value is not a duration.
    ///
    /// ```
    /// use worker_shared::config::{Config, MapSource};
    ///
    /// let config = Config::new(MapSource::from([("T", "1h30m"), ("R", "250ms")]));
    /// assert_eq!(config.duration("T")?, Some(std::time::Duration::from_secs(5400)));
    /// assert_eq!(config.duration("R")?, Some(std::time::Duration::from_millis(250)));
    /// # Ok::<(), worker_shared::Error>(())
    /// ```
    pub fn duration(&self, key: &str) -> Result<Option<Duration>, ConfigError> {
        match self.optional(key) {
            None => Ok(None),
            Some(value) => parse_duration(&value)
                .map(Some)
                .ok_or_else(|| ConfigError::Invalid {
                    key: key.to_string(),
                    message: "expected a duration such as `30`, `30s`, `250ms` or `1h30m`"
                        .to_string(),
                }),
        }
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Parse `30`, `250ms`, `1h30m` into a [`Duration`].
///
/// Returns `None` for empty input, a bare suffix, an unknown unit, a
/// fractional number or a value that overflows. Never panics.
///
/// ```
/// use worker_shared::config::parse_duration;
///
/// assert_eq!(parse_duration("90"), Some(std::time::Duration::from_secs(90)));
/// assert_eq!(parse_duration("1h30m"), Some(std::time::Duration::from_secs(5400)));
/// assert_eq!(parse_duration("2x"), None);
/// assert_eq!(parse_duration(""), None);
/// ```
#[must_use]
pub fn parse_duration(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut total = Duration::ZERO;
    let mut rest = trimmed;
    let mut consumed_any = false;

    while !rest.is_empty() {
        let digits_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        if digits_end == 0 {
            return None;
        }
        let amount: u64 = rest.get(..digits_end)?.parse().ok()?;
        rest = rest.get(digits_end..)?;

        let unit_end = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        let unit = rest.get(..unit_end)?;
        rest = rest.get(unit_end..)?;

        // A bare `30` is seconds; a suffix must be a known unit.
        let scale_ms: u64 = match unit {
            "" | "s" => 1_000,
            "ms" => 1,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => return None,
        };

        let millis = amount.checked_mul(scale_ms)?;
        total = total.checked_add(Duration::from_millis(millis))?;
        consumed_any = true;
    }

    consumed_any.then_some(total)
}
