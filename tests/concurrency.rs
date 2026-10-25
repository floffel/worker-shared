//! Concurrency: every shared type is hammered from 32 tasks, because "it is
//! `Send + Sync`" is a compiler claim about types, not a proof that the code
//! is correct under contention.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::CountingFetcher;

use worker_shared::secrets::{CacheConfig, SecretCache, SecretFetcher, SecretString};
use worker_shared::Error;

fn config() -> CacheConfig {
    CacheConfig::new(Duration::from_secs(300))
        .with_refresh_margin(Duration::from_secs(30))
        .with_jitter(Duration::from_secs(5))
}

/// The headline guarantee: 32 concurrent callers, one fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn thirty_two_tasks_share_one_fetch_for_the_same_secret() {
    let fetcher = Arc::new(CountingFetcher {
        delay: Duration::from_millis(50),
        ..CountingFetcher::default()
    });
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config()).expect("cache"));

    let mut handles = Vec::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            cache
                .get("shared")
                .await
                .map(|secret| secret.expose_secret().to_string())
        }));
    }

    let mut values = Vec::new();
    for handle in handles {
        values.push(handle.await.expect("join").expect("value"));
    }

    assert_eq!(values.len(), 32);
    assert!(values.iter().all(|value| value == "value-for-shared"));
    assert_eq!(fetcher.calls(), 1, "a stampede would be 32 calls");
    assert_eq!(cache.stats().fetches, 1);
    assert_eq!(cache.stats().misses, 32);
}

/// Different secrets must not serialise behind one another.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn thirty_two_tasks_fetch_distinct_secrets_in_parallel() {
    let fetcher = Arc::new(CountingFetcher {
        delay: Duration::from_millis(40),
        ..CountingFetcher::default()
    });
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config()).expect("cache"));

    let started = std::time::Instant::now();
    let mut handles = Vec::new();
    for index in 0..32 {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            cache.get(&format!("key-{index}")).await.is_ok()
        }));
    }
    for handle in handles {
        assert!(handle.await.expect("join"));
    }

    let elapsed = started.elapsed();
    assert_eq!(fetcher.calls(), 32);
    assert!(
        elapsed < Duration::from_millis(32 * 40 / 2),
        "32 independent fetches took {elapsed:?} — they were serialised"
    );
}

/// Invalidation from another task while readers hammer the cache must never
/// panic, deadlock or hand out a poisoned value.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn invalidation_churn_under_load_is_safe() {
    let fetcher = CountingFetcher::new();
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config()).expect("cache"));
    let reads = Arc::new(AtomicUsize::new(0));

    let mut readers = Vec::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        let reads = Arc::clone(&reads);
        readers.push(tokio::spawn(async move {
            for _ in 0..20 {
                if cache.get("churn").await.is_ok() {
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    let invalidator = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            for _ in 0..50 {
                cache.invalidate("churn");
                tokio::task::yield_now().await;
            }
        })
    };

    for reader in readers {
        reader.await.expect("join");
    }
    invalidator.await.expect("join");

    assert_eq!(reads.load(Ordering::Relaxed), 32 * 20);
    assert!(fetcher.calls() >= 1);
}

/// A shared `SecretString` is read-only and cheap to clone across tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_secret_is_safe_to_read_from_many_tasks() {
    let fetcher = CountingFetcher::new();
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config()).expect("cache"));
    let secret: Arc<SecretString> = cache.get("shared").await.expect("value");

    let mut handles = Vec::new();
    for _ in 0..32 {
        let secret = Arc::clone(&secret);
        handles.push(tokio::spawn(async move {
            assert!(secret.ct_eq(&SecretString::new("value-for-shared").expect("value")));
            secret.len()
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.expect("join"), "value-for-shared".len());
    }
}

/// `Lazy` under contention: one initialisation, 32 winners.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lazy_state_initialises_once_for_32_tasks() {
    use worker_shared::handler::Lazy;

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
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    7
                })
                .await
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.expect("join"), 7);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// SQS failures collected from many tasks are still reported in batch order.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "sqs")]
async fn concurrent_sqs_processing_keeps_failure_order() {
    use worker_shared::sqs::{run_batch, SqsBatchEvent};

    let records: Vec<serde_json::Value> = (0..32)
        .map(|index| serde_json::json!({"messageId": format!("m-{index}"), "body": "x"}))
        .collect();
    let event = SqsBatchEvent::from_value(serde_json::json!({"Records": records})).expect("parse");

    let failures = run_batch(&event, |index, _message| async move {
        // Even indexes "succeed", odd ones fail.
        index % 2 == 0
    })
    .await;

    let expected: Vec<String> = (0..32)
        .filter(|index| index % 2 == 1)
        .map(|index| format!("m-{index}"))
        .collect();
    assert_eq!(failures.as_slice(), expected.as_slice());
}

/// A cache shared behind `Arc` across threads keeps its counters consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stats_stay_consistent_under_contention() {
    let fetcher = Arc::new(CountingFetcher {
        delay: Duration::from_millis(5),
        ..CountingFetcher::default()
    });
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config()).expect("cache"));

    let mut handles = Vec::new();
    for index in 0..32 {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            let mut total = 0;
            for _ in 0..10 {
                total += cache
                    .get(&format!("key-{}", index % 4))
                    .await
                    .map_or(0, |secret| secret.len());
            }
            total
        }));
    }
    let mut lengths = 0;
    for handle in handles {
        lengths += handle.await.expect("join");
    }

    let stats = cache.stats();
    assert_eq!(stats.hits + stats.misses, 320);
    assert_eq!(
        stats.misses,
        stats.fetches + stats.failures + stats.coalesced
    );
    assert!(
        stats.fetches <= 4,
        "four keys, at most four fetches: {stats:?}"
    );
    assert!(lengths > 0);
}

/// A failing fetcher must not deadlock the per-key lock: the lock is released
/// on the error path, so the next caller proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_fetch_releases_the_single_flight_lock() {
    let fetcher = CountingFetcher::new();
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config()).expect("cache"));
    fetcher.fail_with(Error::Upstream {
        service: "secretsmanager",
        message: "boom".to_string(),
    });

    assert!(cache.get("k").await.is_err());
    fetcher.recover();
    assert!(cache.get("k").await.is_ok());

    // And the cache is usable from many tasks afterwards.
    let mut handles = Vec::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move { cache.get("k").await.is_ok() }));
    }
    for handle in handles {
        assert!(handle.await.expect("join"));
    }
}

/// `SecretCache` with a dynamic fetcher (object-safe trait) is still `Send`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dyn_fetcher_cache_is_shareable() {
    let fetcher: Arc<dyn SecretFetcher> = Arc::new(CountingFetcher::new());
    let cache = Arc::new(SecretCache::new(Arc::clone(&fetcher), config()).expect("cache"));
    let mut handles = Vec::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move { cache.get("dyn").await.is_ok() }));
    }
    for handle in handles {
        assert!(handle.await.expect("join"));
    }
}
