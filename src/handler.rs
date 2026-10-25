//! Handler glue: initialise once, stop before the platform kills you, notice a
//! shutdown.
//!
//! A Lambda function object is reused across invocations, so expensive setup
//! (SDK clients, secret caches, HTTP pools) belongs in a `OnceCell`-style
//! one-time init, not in the handler body. But "one-time" init that fails must
//! be retryable — a cold start that met a throttled Secrets Manager call
//! should try again on the next invocation instead of caching the failure for
//! the lifetime of the container.
//!
//! [`Lazy`] wraps `tokio::sync::OnceCell` with exactly that behaviour: at most
//! one successful initialisation (concurrent callers wait for it), and a failed
//! initialisation leaves the cell empty so the next caller retries.
//!
//! [`Deadline`] and [`Cancellation`] cover the other two platform realities:
//! the invocation is killed at the timeout (so finish work before that, and
//! report honestly if you cannot), and `SIGTERM` arrives before the container
//! freezes (so a background flusher can stop instead of being cut off).
//!
//! ```
//! use std::time::Duration;
//! use worker_shared::handler::{Cancellation, Deadline, Lazy};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), worker_shared::Error> {
//! let state: Lazy<String> = Lazy::new();
//!
//! // First caller initialises; every later caller reuses it.
//! let first = state.get_or_try_init(|| async { Ok::<_, worker_shared::Error>("ready".to_string()) }).await?;
//! assert_eq!(first, "ready");
//! assert!(state.is_initialized());
//!
//! // Work that must not overrun the platform's timeout.
//! let deadline = Deadline::after(Duration::from_secs(5));
//! let value = worker_shared::handler::with_deadline(deadline, async { 41 + 1 }).await?;
//! assert_eq!(value, 42);
//!
//! // A background task that reacts to SIGTERM.
//! let cancellation = Cancellation::new();
//! let token = cancellation.token();
//! cancellation.cancel();
//! assert!(token.is_cancelled());
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::time::{Duration, Instant};

use tokio::sync::{watch, OnceCell};

use crate::error::Error;

/// One-time initialisation that stays retryable after a failure.
///
/// ```
/// use worker_shared::handler::Lazy;
/// use worker_shared::Error;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let lazy: Lazy<u32> = Lazy::new();
/// assert!(lazy.get().is_none());
///
/// let value = lazy.get_or_init(|| async { 7 }).await;
/// assert_eq!(*value, 7);
/// assert_eq!(lazy.get().copied(), Some(7));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default)]
pub struct Lazy<T> {
    cell: OnceCell<T>,
}

impl<T> Lazy<T> {
    /// An uninitialised cell.
    ///
    /// ```
    /// use worker_shared::handler::Lazy;
    ///
    /// let lazy: Lazy<String> = Lazy::new();
    /// assert!(!lazy.is_initialized());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            cell: OnceCell::new(),
        }
    }

    /// The value, if initialisation has already succeeded.
    ///
    /// ```
    /// use worker_shared::handler::Lazy;
    ///
    /// let lazy: Lazy<u8> = Lazy::new();
    /// assert_eq!(lazy.get(), None);
    /// ```
    #[must_use]
    pub fn get(&self) -> Option<&T> {
        self.cell.get()
    }

    /// Whether initialisation has succeeded.
    ///
    /// ```
    /// use worker_shared::handler::Lazy;
    ///
    /// assert!(!Lazy::<u8>::new().is_initialized());
    /// ```
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.cell.get().is_some()
    }

    /// Initialise on first call, reusing the value afterwards.
    ///
    /// Concurrent first callers all wait for the single in-flight
    /// initialisation, so the closure runs once, not once per task.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::Arc;
    /// use worker_shared::handler::Lazy;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let lazy = Arc::new(Lazy::<usize>::new());
    /// let calls = Arc::new(AtomicUsize::new(0));
    ///
    /// let mut handles = Vec::new();
    /// for _ in 0..32 {
    ///     let lazy = Arc::clone(&lazy);
    ///     let calls = Arc::clone(&calls);
    ///     handles.push(tokio::spawn(async move {
    ///         *lazy.get_or_init(|| async {
    ///             calls.fetch_add(1, Ordering::SeqCst);
    ///             9
    ///         }).await
    ///     }));
    /// }
    /// for handle in handles {
    ///     assert_eq!(handle.await.unwrap_or(0), 9);
    /// }
    /// assert_eq!(calls.load(Ordering::SeqCst), 1);
    /// # }
    /// ```
    pub async fn get_or_init<F, Fut>(&self, init: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        self.cell.get_or_init(init).await
    }

    /// Initialise on first call, retrying if the initialisation failed.
    ///
    /// # Errors
    ///
    /// Whatever `init` returned. The cell stays empty, so the next caller
    /// tries again — a throttle during cold start is not remembered.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use worker_shared::handler::Lazy;
    /// use worker_shared::Error;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// let lazy: Lazy<u8> = Lazy::new();
    /// let attempts = AtomicUsize::new(0);
    ///
    /// let first = lazy.get_or_try_init(|| async {
    ///     attempts.fetch_add(1, Ordering::SeqCst);
    ///     Err::<u8, Error>(Error::Throttled { service: "secretsmanager", retry_after: None })
    /// }).await;
    /// assert!(first.is_err());
    ///
    /// // The failure was not cached: this attempt can succeed.
    /// let second = lazy.get_or_try_init(|| async { Ok::<_, Error>(3) }).await?;
    /// assert_eq!(*second, 3);
    /// assert_eq!(attempts.load(Ordering::SeqCst), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_try_init<F, Fut, E>(&self, init: F) -> Result<&T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.cell.get_or_try_init(init).await
    }
}

/// A point in time by which the invocation must be done.
///
/// ```
/// use std::time::Duration;
/// use worker_shared::handler::Deadline;
///
/// let deadline = Deadline::after(Duration::from_secs(60));
/// assert!(!deadline.expired());
/// assert!(deadline.remaining() <= Duration::from_secs(60));
/// assert!(deadline.remaining() > Duration::from_secs(55));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    at: Instant,
}

impl Deadline {
    /// A deadline `budget` from now.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::handler::Deadline;
    ///
    /// assert!(Deadline::after(Duration::ZERO).expired());
    /// ```
    #[must_use]
    pub fn after(budget: Duration) -> Self {
        Self {
            at: Instant::now() + budget,
        }
    }

    /// A deadline from the Lambda context's `remaining_time`, minus a safety
    /// margin, so the handler can return a clean error before the platform
    /// kills it mid-write.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::handler::Deadline;
    ///
    /// // 30 s of runtime left, keep 2 s for reporting.
    /// let deadline = Deadline::from_remaining(Duration::from_secs(30), Duration::from_secs(2));
    /// assert!(deadline.remaining() <= Duration::from_secs(28));
    ///
    /// // A margin larger than the budget collapses to "already expired".
    /// assert!(Deadline::from_remaining(Duration::from_secs(1), Duration::from_secs(5)).expired());
    /// ```
    #[must_use]
    pub fn from_remaining(remaining: Duration, margin: Duration) -> Self {
        Self::after(remaining.saturating_sub(margin))
    }

    /// Time left, zero once the deadline has passed.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::handler::Deadline;
    ///
    /// assert_eq!(Deadline::after(Duration::ZERO).remaining(), Duration::ZERO);
    /// ```
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }

    /// Whether the deadline has passed.
    ///
    /// ```
    /// use std::time::Duration;
    /// use worker_shared::handler::Deadline;
    ///
    /// assert!(!Deadline::after(Duration::from_secs(1)).expired());
    /// ```
    #[must_use]
    pub fn expired(&self) -> bool {
        self.remaining().is_zero()
    }
}

impl fmt::Display for Deadline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deadline in {:?}", self.remaining())
    }
}

/// Run a future, failing with [`Error::DeadlineExceeded`] if it overruns.
///
/// The future is dropped at the deadline, which cancels it at its next await
/// point. A dropped write is *ambiguous*: check [`Error::is_ambiguous`] before
/// retrying anything non-idempotent.
///
/// # Errors
///
/// [`Error::DeadlineExceeded`] when the deadline passes first.
///
/// ```
/// use std::time::Duration;
/// use worker_shared::handler::{with_deadline, Deadline};
///
/// # #[tokio::main]
/// # async fn main() {
/// let deadline = Deadline::after(Duration::from_millis(50));
/// let result = with_deadline(deadline, async {
///     tokio::time::sleep(Duration::from_secs(10)).await;
///     1
/// }).await;
/// assert_eq!(result, Err(worker_shared::Error::DeadlineExceeded));
/// # }
/// ```
pub async fn with_deadline<F: Future>(deadline: Deadline, future: F) -> Result<F::Output, Error> {
    tokio::time::timeout(deadline.remaining(), future)
        .await
        .map_err(|_| Error::DeadlineExceeded)
}

/// Run a future with a time budget.
///
/// # Errors
///
/// [`Error::DeadlineExceeded`] when the budget runs out.
///
/// ```
/// use std::time::Duration;
/// use worker_shared::handler::with_timeout;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), worker_shared::Error> {
/// assert_eq!(with_timeout(Duration::from_secs(1), async { 5 }).await?, 5);
/// assert!(with_timeout(Duration::from_millis(5), async {
///     tokio::time::sleep(Duration::from_secs(1)).await;
/// }).await.is_err());
/// # Ok(())
/// # }
/// ```
pub async fn with_timeout<F: Future>(budget: Duration, future: F) -> Result<F::Output, Error> {
    with_deadline(Deadline::after(budget), future).await
}

/// The signal side of a graceful shutdown.
///
/// Hand a [`CancellationToken`] to the worker, call [`Cancellation::cancel`]
/// from the shutdown path (Lambda `SIGTERM`, a signal handler, a test).
///
/// ```
/// use worker_shared::handler::Cancellation;
///
/// let cancellation = Cancellation::new();
/// assert!(!cancellation.is_cancelled());
/// let token = cancellation.token();
/// assert!(!token.is_cancelled());
///
/// cancellation.cancel();
/// assert!(token.is_cancelled());
/// // Cancelling twice is a no-op, not an error.
/// assert!(!cancellation.cancel());
/// ```
#[derive(Debug, Clone)]
pub struct Cancellation {
    sender: watch::Sender<bool>,
}

impl Cancellation {
    /// A token that has not been cancelled.
    ///
    /// ```
    /// use worker_shared::handler::Cancellation;
    ///
    /// assert!(!Cancellation::new().is_cancelled());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self { sender }
    }

    /// A handle for the work to watch.
    ///
    /// ```
    /// use worker_shared::handler::Cancellation;
    ///
    /// let cancellation = Cancellation::new();
    /// let token = cancellation.token();
    /// cancellation.cancel();
    /// assert!(token.is_cancelled());
    /// ```
    #[must_use]
    pub fn token(&self) -> CancellationToken {
        CancellationToken {
            receiver: self.sender.subscribe(),
        }
    }

    /// Cancel, waking every waiter. Returns whether this call was the one that
    /// flipped the state.
    ///
    /// ```
    /// use worker_shared::handler::Cancellation;
    ///
    /// let cancellation = Cancellation::new();
    /// assert!(cancellation.cancel());
    /// assert!(!cancellation.cancel());
    /// ```
    #[must_use]
    pub fn cancel(&self) -> bool {
        if *self.sender.borrow() {
            return false;
        }
        self.sender.send_replace(true);
        true
    }

    /// Whether cancellation has been requested.
    ///
    /// ```
    /// use worker_shared::handler::Cancellation;
    ///
    /// let cancellation = Cancellation::new();
    /// cancellation.cancel();
    /// assert!(cancellation.is_cancelled());
    /// ```
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }
}

impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// The receiving side of a [`Cancellation`].
///
/// ```
/// use worker_shared::handler::Cancellation;
///
/// # #[tokio::main]
/// # async fn main() {
/// let cancellation = Cancellation::new();
/// let token = cancellation.token();
/// assert!(!token.is_cancelled());
/// cancellation.cancel();
/// assert!(token.is_cancelled());
/// token.cancelled().await;
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CancellationToken {
    receiver: watch::Receiver<bool>,
}

impl CancellationToken {
    /// Whether cancellation has been requested.
    ///
    /// ```
    /// use worker_shared::handler::Cancellation;
    ///
    /// let cancellation = Cancellation::new();
    /// assert!(!cancellation.token().is_cancelled());
    /// ```
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Wait until cancellation is requested. Returns immediately if it already
    /// was, so there is no lost-wakeup race with a `cancel()` that happened
    /// before this call.
    ///
    /// ```
    /// use worker_shared::handler::Cancellation;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let cancellation = Cancellation::new();
    /// let token = cancellation.token();
    /// cancellation.cancel();
    /// token.cancelled().await; // returns at once
    /// # }
    /// ```
    pub async fn cancelled(&self) {
        let mut receiver = self.receiver.clone();
        if *receiver.borrow_and_update() {
            return;
        }
        // Wait for a change, then re-check the flag: `changed()` may fire for a
        // value written by another sender. A closed channel means the
        // `Cancellation` was dropped without cancelling, which is terminal too
        // — waiting forever for a signal that can no longer arrive would hang
        // the handler.
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                // The `Cancellation` was dropped without cancelling; no further
                // signal can arrive, so stop waiting instead of hanging.
                return;
            }
        }
    }
}

/// Run a future until it finishes or cancellation is requested.
///
/// # Errors
///
/// [`Error::Cancelled`] when the token fires first. The future is dropped, so
/// in-flight writes are ambiguous — the caller decides whether re-delivery is
/// safe (see [`Error::is_ambiguous`]).
///
/// ```
/// use std::time::Duration;
/// use worker_shared::handler::{run_until_cancelled, Cancellation};
///
/// # #[tokio::main]
/// # async fn main() {
/// let cancellation = Cancellation::new();
/// let token = cancellation.token();
///
/// assert_eq!(run_until_cancelled(&token, async { 3 }).await, Ok(3));
///
/// cancellation.cancel();
/// let cancelled = run_until_cancelled(&token, async {
///     tokio::time::sleep(Duration::from_secs(10)).await;
/// }).await;
/// assert_eq!(cancelled, Err(worker_shared::Error::Cancelled));
/// # }
/// ```
pub async fn run_until_cancelled<F: Future>(
    token: &CancellationToken,
    future: F,
) -> Result<F::Output, Error> {
    tokio::select! {
        biased;
        () = token.cancelled() => Err(Error::Cancelled),
        output = future => Ok(output),
    }
}

/// Run a future under both a deadline and a cancellation token — the two
/// platform limits a handler actually has.
///
/// # Errors
///
/// [`Error::Cancelled`] or [`Error::DeadlineExceeded`].
///
/// ```
/// use std::time::Duration;
/// use worker_shared::handler::{run_bounded, Cancellation, Deadline};
///
/// # #[tokio::main]
/// # async fn main() {
/// let cancellation = Cancellation::new();
/// let result = run_bounded(
///     cancellation.token(),
///     Deadline::after(Duration::from_secs(1)),
///     async { "done" },
/// ).await;
/// assert_eq!(result, Ok("done"));
/// # }
/// ```
pub async fn run_bounded<F: Future>(
    token: CancellationToken,
    deadline: Deadline,
    future: F,
) -> Result<F::Output, Error> {
    tokio::select! {
        biased;
        () = token.cancelled() => Err(Error::Cancelled),
        () = tokio::time::sleep(deadline.remaining()) => Err(Error::DeadlineExceeded),
        output = future => Ok(output),
    }
}
