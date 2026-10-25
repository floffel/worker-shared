//! Handler glue: one-time init, deadlines, cancellation.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use worker_shared::handler::{
    run_bounded, run_until_cancelled, with_deadline, with_timeout, Cancellation, CancellationToken,
    Deadline, Lazy,
};
use worker_shared::Error;

#[tokio::test]
async fn lazy_initialises_exactly_once_under_32_tasks() {
    let lazy = Arc::new(Lazy::<usize>::new());
    let calls = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..32 {
        let lazy = Arc::clone(&lazy);
        let calls = Arc::clone(&calls);
        handles.push(tokio::spawn(async move {
            *lazy
                .get_or_init(|| async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    99
                })
                .await
        }));
    }

    for handle in handles {
        assert_eq!(handle.await.expect("join"), 99);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(lazy.is_initialized());
    assert_eq!(lazy.get().copied(), Some(99));
}

#[tokio::test]
async fn lazy_allows_a_retry_after_a_failed_init() {
    let lazy = Lazy::<String>::new();
    let attempts = AtomicUsize::new(0);

    let first: Result<&String, Error> = lazy
        .get_or_try_init(|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(Error::Throttled {
                service: "secretsmanager",
                retry_after: None,
            })
        })
        .await;
    assert!(first.is_err());
    assert!(!lazy.is_initialized());
    assert!(lazy.get().is_none());

    let second = lazy
        .get_or_try_init(|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>("ready".to_string())
        })
        .await
        .expect("second attempt succeeds");
    assert_eq!(second, "ready");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    // Once initialised, the closure is never run again.
    let third = lazy
        .get_or_try_init(|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>("ignored".to_string())
        })
        .await
        .expect("cached");
    assert_eq!(third, "ready");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn lazy_default_is_empty() {
    let lazy: Lazy<u8> = Lazy::default();
    assert!(!lazy.is_initialized());
}

#[test]
fn deadline_reports_remaining_time_and_expiry() {
    let deadline = Deadline::after(Duration::from_secs(60));
    assert!(!deadline.expired());
    assert!(deadline.remaining() <= Duration::from_secs(60));
    assert!(deadline.remaining() > Duration::from_secs(50));
    assert!(format!("{deadline}").starts_with("deadline in "));

    assert!(Deadline::after(Duration::ZERO).expired());
    assert_eq!(Deadline::after(Duration::ZERO).remaining(), Duration::ZERO);
    assert_eq!(deadline, deadline);

    let trimmed = Deadline::from_remaining(Duration::from_secs(30), Duration::from_secs(2));
    assert!(trimmed.remaining() <= Duration::from_secs(28));
    assert!(Deadline::from_remaining(Duration::from_secs(1), Duration::from_secs(5)).expired());
}

#[tokio::test]
async fn with_deadline_returns_the_value_when_fast_enough() {
    let result = with_deadline(Deadline::after(Duration::from_secs(5)), async { 7 }).await;
    assert_eq!(result.expect("within deadline"), 7);
}

#[tokio::test]
async fn with_deadline_yields_a_typed_timeout_error() {
    let error = with_deadline(Deadline::after(Duration::from_millis(20)), async {
        tokio::time::sleep(Duration::from_secs(10)).await;
        0
    })
    .await
    .expect_err("must time out");

    assert_eq!(error, Error::DeadlineExceeded);
    assert_eq!(error.code(), "deadline_exceeded");
    assert!(error.is_ambiguous(), "a dropped write may have landed");
    assert!(!error.is_retryable(), "the event source owns re-delivery");
}

#[tokio::test]
async fn with_timeout_mirrors_with_deadline() {
    assert_eq!(
        with_timeout(Duration::from_secs(1), async { "ok" })
            .await
            .expect("fast"),
        "ok"
    );
    assert!(with_timeout(Duration::from_millis(10), async {
        tokio::time::sleep(Duration::from_secs(5)).await;
    })
    .await
    .is_err());
}

#[tokio::test]
async fn cancellation_is_idempotent_and_reaches_every_token() {
    let cancellation = Cancellation::new();
    assert!(!cancellation.is_cancelled());
    let first = cancellation.token();
    let second = cancellation.token();
    assert!(!first.is_cancelled());

    assert!(cancellation.cancel());
    assert!(!cancellation.cancel());
    assert!(cancellation.is_cancelled());
    assert!(first.is_cancelled());
    assert!(second.is_cancelled());

    // Both return immediately; there is no lost wakeup.
    first.cancelled().await;
    second.cancelled().await;

    let defaulted = Cancellation::default();
    assert!(!defaulted.is_cancelled());
    assert!(format!("{defaulted:?}").starts_with("Cancellation"));
}

#[tokio::test]
async fn a_token_completes_when_cancelled_from_another_task() {
    let cancellation = Cancellation::new();
    let token = cancellation.token();

    let waiter = tokio::spawn(async move {
        token.cancelled().await;
        token.is_cancelled()
    });

    let _ = cancellation.cancel();
    assert!(waiter.await.expect("join"));
}

#[tokio::test]
async fn a_waiter_already_parked_on_cancelled_is_woken() {
    let cancellation = Cancellation::new();
    let token = cancellation.token();

    let waiter = tokio::spawn(async move {
        token.cancelled().await;
        token.is_cancelled()
    });

    // Give the waiter time to park inside `changed()` so the wake path — not
    // the fast path — is the one exercised.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(cancellation.cancel());
    assert!(waiter.await.expect("join"));
}

#[tokio::test]
async fn a_token_whose_cancellation_was_dropped_stops_waiting() {
    let cancellation = Cancellation::new();
    let token = cancellation.token();

    let waiter = tokio::spawn(async move {
        token.cancelled().await;
        token.is_cancelled()
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(cancellation);

    // The waiter returns (not cancelled, but no longer able to wait) instead
    // of hanging forever.
    assert!(!waiter.await.expect("join"));
}

#[tokio::test]
async fn run_until_cancelled_prefers_the_result_when_it_wins() {
    let cancellation = Cancellation::new();
    let token = cancellation.token();
    let result = run_until_cancelled(&token, async { "done" }).await;
    assert_eq!(result.expect("finished first"), "done");
}

#[tokio::test]
async fn run_until_cancelled_reports_cancellation() {
    let cancellation = Cancellation::new();
    let token: CancellationToken = cancellation.token();
    let _ = cancellation.cancel();
    let error = run_until_cancelled(&token, async {
        tokio::time::sleep(Duration::from_secs(10)).await;
    })
    .await
    .expect_err("already cancelled");
    assert_eq!(error, Error::Cancelled);
    assert_eq!(error.code(), "cancelled");
    assert!(!error.is_retryable());
    assert!(!error.is_ambiguous());
}

#[tokio::test]
async fn run_bounded_covers_success_cancellation_and_deadline() {
    let cancellation = Cancellation::new();
    let value = run_bounded(
        cancellation.token(),
        Deadline::after(Duration::from_secs(1)),
        async { 1 },
    )
    .await;
    assert_eq!(value.expect("fast path"), 1);

    let cancelled = Cancellation::new();
    let _ = cancelled.cancel();
    let error = run_bounded(
        cancelled.token(),
        Deadline::after(Duration::from_secs(1)),
        async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        },
    )
    .await
    .expect_err("cancelled");
    assert_eq!(error, Error::Cancelled);

    let error = run_bounded(
        Cancellation::new().token(),
        Deadline::after(Duration::from_millis(10)),
        async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        },
    )
    .await
    .expect_err("deadline");
    assert_eq!(error, Error::DeadlineExceeded);
}

#[tokio::test]
async fn handler_types_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Lazy<u8>>();
    assert_send_sync::<Deadline>();
    assert_send_sync::<Cancellation>();
    assert_send_sync::<CancellationToken>();
    assert_send_sync::<Arc<Lazy<String>>>();
}
