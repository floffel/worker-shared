//! Shared test doubles: a controllable clock, a counting fetcher, a gated
//! fetcher for in-flight races, and a recording dispatcher.
//!
//! Nothing here uses the network or sleeps for real wall-clock time beyond a
//! few milliseconds, so the suite is deterministic on CI.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

#[cfg(feature = "eventbridge")]
use worker_shared::eventbridge::{Dispatcher, Entry, PutEventsResponse};
use worker_shared::secrets::{Clock, SecretFetcher};
use worker_shared::Error;

/// A clock the test moves by hand.
#[derive(Debug, Clone)]
pub struct TestClock {
    now: Arc<Mutex<Instant>>,
    jitter: Duration,
}

impl TestClock {
    /// A clock starting now, with no jitter.
    pub fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
            jitter: Duration::ZERO,
        }
    }

    /// A clock that always returns exactly `jitter` when asked for jitter.
    pub fn with_jitter(jitter: Duration) -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
            jitter,
        }
    }

    /// Move time forward.
    pub fn advance(&self, by: Duration) {
        let mut now = self.now.lock().expect("clock lock");
        *now += by;
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        *self.now.lock().expect("clock lock")
    }

    fn jitter(&self, max: Duration) -> Duration {
        self.jitter.min(max)
    }
}

/// A fetcher that counts calls per key and can be told to fail or return an
/// empty value.
#[derive(Debug, Default)]
pub struct CountingFetcher {
    pub calls: AtomicU64,
    pub per_key: Mutex<HashMap<String, u64>>,
    pub delay: Duration,
    pub failure: Mutex<Option<Error>>,
    pub empty: bool,
}

impl CountingFetcher {
    /// A fetcher that answers `value-for-{id}` immediately.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Total calls across all keys.
    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }

    /// Calls for one key.
    pub fn calls_for(&self, id: &str) -> u64 {
        self.per_key
            .lock()
            .expect("per-key lock")
            .get(id)
            .copied()
            .unwrap_or(0)
    }

    /// Make every subsequent call fail with `error`.
    pub fn fail_with(&self, error: Error) {
        *self.failure.lock().expect("failure lock") = Some(error);
    }

    /// Stop failing.
    pub fn recover(&self) {
        *self.failure.lock().expect("failure lock") = None;
    }
}

#[async_trait::async_trait]
impl SecretFetcher for CountingFetcher {
    async fn fetch(&self, id: &str) -> Result<String, Error> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self
            .per_key
            .lock()
            .expect("per-key lock")
            .entry(id.to_string())
            .or_insert(0) += 1;

        if let Some(error) = self.failure.lock().expect("failure lock").clone() {
            return Err(error);
        }
        if self.empty {
            return Ok(String::new());
        }
        Ok(format!("value-for-{id}"))
    }
}

/// A fetcher that blocks inside `fetch` until the test releases it — the only
/// way to test "invalidated while the fetch was in flight" deterministically.
#[derive(Debug)]
pub struct GatedFetcher {
    pub calls: Arc<AtomicU64>,
    entered: watch::Sender<u64>,
    release: watch::Receiver<bool>,
}

/// The handles a test needs to drive a [`GatedFetcher`].
pub struct Gated {
    pub fetcher: Arc<GatedFetcher>,
    pub release: watch::Sender<bool>,
    pub entered: watch::Receiver<u64>,
}

/// Build a gated fetcher and the channels that drive it.
pub fn gated() -> Gated {
    let (entered_tx, entered) = watch::channel(0u64);
    let (release_tx, release) = watch::channel(false);
    Gated {
        fetcher: Arc::new(GatedFetcher {
            calls: Arc::new(AtomicU64::new(0)),
            entered: entered_tx,
            release,
        }),
        release: release_tx,
        entered,
    }
}

#[async_trait::async_trait]
impl SecretFetcher for GatedFetcher {
    async fn fetch(&self, id: &str) -> Result<String, Error> {
        let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.entered.send_replace(count);
        let mut release = self.release.clone();
        while !*release.borrow() {
            if release.changed().await.is_err() {
                break;
            }
        }
        Ok(format!("value-{count}-for-{id}"))
    }
}

/// A dispatcher that records what it was asked to send.
#[cfg(feature = "eventbridge")]
#[derive(Debug, Default, Clone)]
pub struct RecordingDispatcher {
    pub batches: Arc<Mutex<Vec<Vec<Entry>>>>,
    pub fail_with: Arc<Mutex<Option<Error>>>,
    pub reject_indexes: Arc<Mutex<Vec<usize>>>,
}

#[cfg(feature = "eventbridge")]
impl RecordingDispatcher {
    /// A dispatcher that accepts everything.
    pub fn accepting() -> Self {
        Self::default()
    }

    /// A dispatcher whose call-level result is an error.
    pub fn failing(error: Error) -> Self {
        let dispatcher = Self::default();
        *dispatcher.fail_with.lock().expect("fail lock") = Some(error);
        dispatcher
    }

    /// A dispatcher that rejects the given positions.
    pub fn rejecting(indexes: &[usize]) -> Self {
        let dispatcher = Self::default();
        *dispatcher.reject_indexes.lock().expect("reject lock") = indexes.to_vec();
        dispatcher
    }

    /// Every batch that was sent.
    pub fn sent(&self) -> Vec<Vec<Entry>> {
        self.batches.lock().expect("batches lock").clone()
    }

    /// How many calls were made.
    pub fn call_count(&self) -> usize {
        self.batches.lock().expect("batches lock").len()
    }
}

#[cfg(feature = "eventbridge")]
#[async_trait::async_trait]
impl Dispatcher for RecordingDispatcher {
    async fn put_events(&self, entries: &[Entry]) -> Result<PutEventsResponse, Error> {
        self.batches
            .lock()
            .expect("batches lock")
            .push(entries.to_vec());

        if let Some(error) = self.fail_with.lock().expect("fail lock").clone() {
            return Err(error);
        }

        let reject = self.reject_indexes.lock().expect("reject lock").clone();
        let results = entries
            .iter()
            .enumerate()
            .map(|(index, _)| {
                if reject.contains(&index) {
                    worker_shared::eventbridge::EntryResult::failed(
                        "ThrottlingException",
                        "rejected by test",
                    )
                } else {
                    worker_shared::eventbridge::EntryResult::accepted(format!("event-{index}"))
                }
            })
            .collect();
        Ok(PutEventsResponse::from_results(results))
    }
}
