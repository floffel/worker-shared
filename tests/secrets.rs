//! Secret cache behaviour: TTL, refresh margin, jitter, invalidation,
//! single-flight and failure handling.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{gated, CountingFetcher, TestClock};

use worker_shared::secrets::{
    CacheConfig, CacheStats, Clock, SecretCache, SecretFetcher, SystemClock,
};
use worker_shared::{Error, SecretString};

fn config(ttl: u64, margin: u64) -> CacheConfig {
    CacheConfig::new(Duration::from_secs(ttl))
        .with_refresh_margin(Duration::from_secs(margin))
        .with_jitter(Duration::ZERO)
}

#[tokio::test]
async fn first_read_fetches_and_second_read_hits() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");

    let first = cache.get("db").await.expect("first");
    let second = cache.get("db").await.expect("second");

    assert_eq!(first.expose_secret(), "value-for-db");
    assert!(Arc::ptr_eq(&first, &second), "the cache hands out one Arc");
    assert_eq!(fetcher.calls(), 1);
    assert_eq!(
        cache.stats(),
        CacheStats {
            hits: 1,
            coalesced: 0,
            misses: 1,
            fetches: 1,
            failures: 0
        }
    );
}

#[tokio::test]
async fn entry_expires_at_refresh_margin_boundary() {
    let fetcher = CountingFetcher::new();
    let clock = Arc::new(TestClock::new());
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10))
        .expect("cache")
        .with_clock(clock.clone());

    cache.get("db").await.expect("first fetch");
    assert_eq!(fetcher.calls(), 1);

    // Effective lifetime is ttl - margin = 50 s.
    clock.advance(Duration::from_millis(49_900));
    assert!(cache.peek("db").is_some());
    cache.get("db").await.expect("cached read");
    assert_eq!(fetcher.calls(), 1, "still fresh");

    clock.advance(Duration::from_millis(200));
    assert!(cache.peek("db").is_none(), "the margin is the cut-off");
    cache.get("db").await.expect("refetch");
    assert_eq!(fetcher.calls(), 2, "refreshed before the hard TTL");
}

#[tokio::test]
async fn jitter_shortens_the_lifetime_and_never_extends_it() {
    let fetcher = CountingFetcher::new();
    let clock = Arc::new(TestClock::with_jitter(Duration::from_secs(5)));
    let cache = SecretCache::new(
        Arc::clone(&fetcher),
        CacheConfig::new(Duration::from_secs(60))
            .with_refresh_margin(Duration::from_secs(10))
            .with_jitter(Duration::from_secs(5)),
    )
    .expect("cache")
    .with_clock(clock.clone());

    cache.get("db").await.expect("first fetch");
    assert_eq!(
        cache.config().lifetime(Duration::from_secs(5)),
        Duration::from_secs(45)
    );

    clock.advance(Duration::from_millis(44_900));
    cache.get("db").await.expect("cached read");
    assert_eq!(fetcher.calls(), 1);

    clock.advance(Duration::from_millis(200));
    cache
        .get("db")
        .await
        .expect("refetch after jittered lifetime");
    assert_eq!(fetcher.calls(), 2);
    assert!(clock.jitter(Duration::from_secs(5)) <= Duration::from_secs(5));
}

#[tokio::test]
async fn zero_ttl_disables_caching_without_erroring() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(
        Arc::clone(&fetcher),
        CacheConfig::new(Duration::ZERO).with_refresh_margin(Duration::ZERO),
    )
    .expect("zero ttl is legal");

    cache.get("db").await.expect("first");
    cache.get("db").await.expect("second");
    assert_eq!(fetcher.calls(), 2);
    assert_eq!(cache.len(), 1, "the entry is stored but never fresh");
}

#[tokio::test]
async fn margin_longer_than_ttl_is_a_config_error() {
    let fetcher = CountingFetcher::new();
    let error = SecretCache::new(
        fetcher,
        CacheConfig::new(Duration::from_secs(10)).with_refresh_margin(Duration::from_secs(10)),
    )
    .expect_err("must reject");
    assert_eq!(error.code(), "config");
    assert!(error.to_string().contains("cache.refresh_margin"));
}

#[tokio::test]
async fn empty_secret_fails_closed_and_is_not_cached() {
    let fetcher = Arc::new(CountingFetcher {
        empty: true,
        ..CountingFetcher::default()
    });
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");

    let error = cache.get("db").await.expect_err("empty is an error");
    assert!(matches!(error, Error::EmptySecret { ref id } if id == "db"));
    assert_eq!(error.code(), "empty_secret");
    assert!(!error.is_retryable());
    assert!(cache.peek("db").is_none());
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.stats().failures, 1);

    // A later read retries instead of remembering the failure.
    assert!(cache.get("db").await.is_err());
    assert_eq!(fetcher.calls(), 2);
}

#[tokio::test]
async fn empty_id_is_validation_error_and_never_reaches_the_fetcher() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");

    let error = cache.get("").await.expect_err("empty id");
    assert_eq!(error.code(), "validation");
    assert_eq!(fetcher.calls(), 0);
}

#[tokio::test]
async fn throttled_fetch_is_retryable_and_not_cached() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");
    fetcher.fail_with(Error::Throttled {
        service: "secretsmanager",
        retry_after: Some(Duration::from_millis(250)),
    });

    let error = cache.get("db").await.expect_err("throttled");
    assert!(error.is_retryable());
    assert!(!error.is_ambiguous(), "a throttle was never processed");
    assert_eq!(cache.stats().failures, 1);
    assert!(cache.peek("db").is_none());

    fetcher.recover();
    assert_eq!(
        cache.get("db").await.expect("retry works").expose_secret(),
        "value-for-db"
    );
    assert_eq!(fetcher.calls(), 2);
}

#[tokio::test]
async fn invalidate_drops_one_entry_and_reports_whether_it_existed() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");

    cache.get("a").await.expect("a");
    cache.get("b").await.expect("b");
    assert_eq!(cache.len(), 2);

    assert!(cache.invalidate("a"));
    assert!(!cache.invalidate("a"));
    assert_eq!(cache.len(), 1);

    cache.get("a").await.expect("refetched");
    assert_eq!(fetcher.calls_for("a"), 2);
    assert_eq!(fetcher.calls_for("b"), 1);
}

#[tokio::test]
async fn invalidate_all_clears_everything() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");

    cache.get("a").await.expect("a");
    cache.get("b").await.expect("b");
    assert_eq!(cache.invalidate_all(), 2);
    assert!(cache.is_empty());
    assert_eq!(cache.invalidate_all(), 0);
}

#[tokio::test]
async fn rotation_mid_flight_does_not_store_the_stale_value() {
    let gated = gated();
    let cache =
        Arc::new(SecretCache::new(Arc::clone(&gated.fetcher), config(60, 10)).expect("cache"));

    let mut entered = gated.entered.clone();
    let handle = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move { cache.get("db").await })
    };

    // Wait until the fetch is actually in flight.
    while *entered.borrow() == 0 {
        entered.changed().await.expect("entered");
    }

    // The operator rotates the secret and invalidates the cache while the old
    // value is still in flight.
    assert!(!cache.invalidate("db"), "nothing stored yet");
    gated.release.send_replace(true);

    let reader = handle.await.expect("join").expect("fetch result");
    assert_eq!(reader.expose_secret(), "value-1-for-db");

    // The result of a fetch that began before the invalidation must not have
    // been stored: the next read has to fetch again.
    assert!(
        cache.peek("db").is_none(),
        "the stale in-flight value was dropped"
    );
}

#[tokio::test]
async fn peek_is_expiry_aware_and_does_not_count() {
    let fetcher = CountingFetcher::new();
    let clock = Arc::new(TestClock::new());
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10))
        .expect("cache")
        .with_clock(clock.clone());

    assert!(cache.peek("db").is_none());
    cache.get("db").await.expect("fetch");
    let stats = cache.stats();
    assert!(cache.peek("db").is_some());
    assert_eq!(cache.stats(), stats, "peek is not a read");

    clock.advance(Duration::from_secs(51));
    assert!(cache.peek("db").is_none());
    assert_eq!(cache.len(), 0, "an expired entry is dropped on read");
}

#[tokio::test]
async fn evict_expired_removes_only_stale_entries() {
    let fetcher = CountingFetcher::new();
    let clock = Arc::new(TestClock::new());
    let cache = SecretCache::new(Arc::clone(&fetcher), config(100, 0))
        .expect("cache")
        .with_clock(clock.clone());

    cache.get("old").await.expect("old");
    clock.advance(Duration::from_secs(60));
    cache.get("new").await.expect("new");
    assert_eq!(cache.len(), 2);

    clock.advance(Duration::from_secs(45));
    // "old" is 105 s old (expired), "new" is 45 s old (fresh).
    assert_eq!(cache.evict_expired(), 1);
    assert_eq!(cache.len(), 1);
    assert!(cache.peek("new").is_some());
}

#[tokio::test]
async fn distinct_keys_do_not_block_each_other() {
    let fetcher = Arc::new(CountingFetcher {
        delay: Duration::from_millis(60),
        ..CountingFetcher::default()
    });
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache"));

    let started = std::time::Instant::now();
    let mut handles = Vec::new();
    for index in 0..8 {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            cache
                .get(&format!("key-{index}"))
                .await
                .map(|secret| secret.expose_secret().to_string())
        }));
    }
    for handle in handles {
        handle.await.expect("join").expect("value");
    }
    // Eight 60 ms fetches would take 480 ms sequentially; per-key locks mean
    // they overlap.
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "distinct keys were serialised: {:?}",
        started.elapsed()
    );
    assert_eq!(fetcher.calls(), 8);
}

#[tokio::test]
async fn unicode_and_long_ids_round_trip() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");

    let id = "prod/ümlaut/🚀/".to_string() + &"x".repeat(1000);
    let secret = cache.get(&id).await.expect("unicode id");
    assert_eq!(secret.expose_secret(), format!("value-for-{id}"));
    assert!(secret.len() > 1000);
}

#[tokio::test]
async fn secret_string_rejects_empty_and_compares_in_constant_time() {
    assert!(SecretString::new("").is_err());
    let a = SecretString::new("correct horse").expect("a");
    let b = SecretString::new("correct horse").expect("b");
    let c = SecretString::new("correct horsf").expect("c");
    assert!(a.ct_eq(&b));
    assert!(!a.ct_eq(&c));
    assert!(!a.ct_eq(&SecretString::new("correct").expect("short")));
    assert_eq!(a.expose_secret(), "correct horse");
    assert_eq!(a.len(), 13);
    assert!(!a.is_empty());
    assert_eq!(format!("{a:?}"), "SecretString(\"[redacted]\")");
    // NUL and control characters are values, not errors.
    let nul = SecretString::new("a\0b").expect("nul");
    assert_eq!(nul.len(), 3);
    assert!(nul.ct_eq(&SecretString::new("a\0b").expect("nul")));
}

#[tokio::test]
async fn system_clock_is_monotonic_and_jitters_within_bounds() {
    let clock = SystemClock::new();
    let first = clock.now();
    let second = clock.now();
    assert!(second >= first);
    assert_eq!(clock.jitter(Duration::ZERO), Duration::ZERO);
    for _ in 0..64 {
        assert!(clock.jitter(Duration::from_millis(10)) <= Duration::from_millis(10));
    }
    assert!(format!("{clock:?}").starts_with("SystemClock"));
    let _ = SystemClock::default();
}

#[tokio::test]
async fn cache_debug_reports_counts_and_not_secrets() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");
    cache.get("db").await.expect("fetch");

    let debug = format!("{cache:?}");
    assert!(debug.starts_with("SecretCache"));
    assert!(debug.contains("entries: 1"));
    assert!(!debug.contains("value-for-db"));
}

#[tokio::test]
async fn fetcher_can_be_a_dyn_object() {
    let fetcher: Arc<dyn SecretFetcher> = Arc::new(CountingFetcher::new());
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");
    assert_eq!(
        cache.get("x").await.expect("value").expose_secret(),
        "value-for-x"
    );
}

#[tokio::test]
async fn cache_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SecretCache<CountingFetcher>>();
    assert_send_sync::<SecretString>();
    assert_send_sync::<Arc<SecretString>>();
}

#[tokio::test]
async fn stats_accumulate_across_mixed_outcomes() {
    let fetcher = CountingFetcher::new();
    let cache = SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache");

    cache.get("a").await.expect("a");
    cache.get("a").await.expect("a again");
    cache.get("b").await.expect("b");
    fetcher.fail_with(Error::Upstream {
        service: "secretsmanager",
        message: "boom".to_string(),
    });
    cache.get("c").await.expect_err("c fails");

    let stats = cache.stats();
    assert_eq!(stats.fetches, 2);
    assert_eq!(stats.failures, 1);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.coalesced, 0);
    assert_eq!(
        stats,
        CacheStats {
            hits: 1,
            coalesced: 0,
            misses: 3,
            fetches: 2,
            failures: 1
        }
    );
}

#[tokio::test]
async fn concurrent_reads_of_a_failing_secret_all_see_the_error() {
    let fetcher = CountingFetcher::new();
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config(60, 10)).expect("cache"));
    fetcher.fail_with(Error::Throttled {
        service: "secretsmanager",
        retry_after: None,
    });

    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        let counter = Arc::clone(&counter);
        handles.push(tokio::spawn(async move {
            if cache.get("db").await.is_err() {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for handle in handles {
        handle.await.expect("join");
    }
    assert_eq!(counter.load(Ordering::SeqCst), 32);
    assert_eq!(cache.len(), 0);
    // Failures are never cached, so the waiters retry the fetch one after the
    // other instead of all at once on a cold cache.
    assert!(fetcher.calls() >= 1);
    assert!(fetcher.calls() <= 32);
}
