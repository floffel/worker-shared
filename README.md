# worker-shared

## Deprecated

**This crate is deprecated and 0.1.0 will not get fixes, features or security
patches.** It re-implemented infrastructure that AWS already publishes and
maintains, and it should not be adopted for new code.

Use these instead:

| This crate did | Use |
|---|---|
| EventBridge / SQS / DynamoDB-stream envelope parsing | [`aws_lambda_events`](https://crates.io/crates/aws_lambda_events) |
| Handler loop, graceful shutdown, invocation deadline | [`lambda_runtime`](https://crates.io/crates/lambda_runtime) |
| Secret cache with TTL and refresh | [`aws_secretsmanager_caching`](https://crates.io/crates/aws_secretsmanager_caching) |
| `PutEvents` batch chunking (10 entries / 256 KiB) | ~40 lines over [`aws_sdk_eventbridge`](https://crates.io/crates/aws-sdk-eventbridge) |
| SQS `batchItemFailures` response | a small helper over `aws_lambda_events::sqs` |

The extraction also never carried the parts that are product-side by nature
(the DynamoDB idempotency store keyed on an application table schema, the
handler-specific error taxonomy). Those belong in the application, next to
their consumers.

Status: no further releases planned. The 0.1.0 source stays here for reference;
consumers should migrate, and yanking the published version is a separate
decision by the repository owner.

---

Lambda plumbing for Rust AWS workers: a single-flight secret cache, EventBridge
`PutEvents` batching with partial-failure reporting, SQS partial-batch
responses, envelope parsing for the event sources, and handler glue that stops
before the platform kills the invocation.

Extracted from a large product monorepo, with the product domain stripped out.
No table names, no org ids, no event names of ours — the parts every Rust
Lambda estate rewrites are the parts that stayed.

```toml
[dependencies]
worker-shared = "0.1"
```

## Quickstart

```rust,no_run
use std::time::Duration;

use worker_shared::eventbridge::{dispatch, plan, Batch, Entry, EntryResult, PutEventsResponse};
use worker_shared::prelude::*;
use worker_shared::sqs::{run_batch, SqsBatchEvent};

// A handler that reads a rotation-aware secret and publishes a batch of events.
# async fn handle(raw: &str, cache: &SecretCache<MyFetcher>) -> Result<(), Error> {
let api_key = cache.get("prod/api-key").await?;   // one fetch, 32 concurrent callers
let _len = api_key.len();                          // never log `expose_secret()`

let entries = vec![Entry::new("app.orders", "OrderPlaced", serde_json::json!({"id": 7}))?];
for batch in plan(entries)? {                      // split into <=10 / <=256 KiB
    let report = dispatch(&batch, &MyDispatcher).await?;
    for index in report.retryable_indexes() {
        eprintln!("entry {index} should be retried");
    }
}

// SQS: report only the messages that failed.
let event = SqsBatchEvent::parse(raw)?;
let failures = run_batch(&event, |_index, _message| async { true }).await;
let _body = failures.to_json_string()?;
# Ok(())
# }
# struct MyFetcher;
# #[async_trait::async_trait]
# impl SecretFetcher for MyFetcher {
#     async fn fetch(&self, _id: &str) -> Result<String, Error> { Ok("value".into()) }
# }
# struct MyDispatcher;
# #[async_trait::async_trait]
# impl worker_shared::eventbridge::Dispatcher for MyDispatcher {
#     async fn put_events(&self, entries: &[worker_shared::eventbridge::Entry])
#         -> Result<PutEventsResponse, Error> {
#         Ok(PutEventsResponse::all_accepted(entries.len()))
#     }
# }
# fn main() {}
```

Every public item carries a runnable example (`cargo test --doc`), because an
example that is not compiled is a claim, not documentation.

## What is covered

| Area | Types | Notes |
| --- | --- | --- |
| Secret cache | `SecretCache`, `CacheConfig`, `SecretString`, `SecretFetcher`, `Clock` | TTL, refresh margin, jitter, single-flight, generation-guarded invalidation, counters |
| Secrets Manager | `SecretsManagerFetcher` (`secrets-aws`) | SDK adapter; maps throttling vs. terminal failures |
| EventBridge inbound | `EventEnvelope`, `EventBridgeScheduledEvent`, `ScheduledTick` | permissive parse; scheduled-rule validation |
| EventBridge outbound | `Entry`, `Batch`, `plan`, `dispatch`, `Dispatcher`, `DeliveryReport` | 10-entry / 256 KiB limits enforced at push time, per-entry partial failure by position |
| SQS | `SqsBatchEvent`, `SqsMessage`, `BatchItemFailures`, `run_batch` | ordered, de-duplicated `batchItemFailures` |
| DynamoDB Streams | `DdbStreamEvent`, `StreamRecord`, `StreamRecordKind` | unknown event names carried, table name from the stream ARN |
| Config | `Config`, `MapSource`, `Source`, `parse_duration` | typed errors, no panics, values never echoed |
| Handler glue | `Lazy`, `Deadline`, `Cancellation`, `with_deadline`, `run_bounded` | one init that stays retryable, deadlines, `SIGTERM` |

### Feature matrix

| Feature | Default | Adds | Cost |
| --- | --- | --- | --- |
| `eventbridge` | yes | envelope parsing, batching, dispatch | none (pure `serde_json`) |
| `sqs` | yes | batch parsing, partial-batch responses | none |
| `ddb` | no | Streams envelope parsing | none |
| `secrets-aws` | no | `SecretsManagerFetcher` | `aws-sdk-secretsmanager` |
| `tracing` | no | warn-level lines on secret-fetch failure and partial dispatch (ids and codes only, never values) | `tracing` |

The cache, config and handler glue are always compiled — they are the crate's
reason to exist and pull in nothing heavier than `tokio`'s `sync` + `time`
features. `--no-default-features` still gives a complete secret cache, config
and handler toolkit.

## Design decisions worth knowing

* **Single flight, not a lock around everything.** One `tokio::sync::Mutex` per
  secret id. 32 concurrent readers of the same secret issue one fetch; readers
  of different secrets never wait for each other. Proven by
  `tests/concurrency.rs` against a counting fake, not by inspection.
* **Failures are never cached.** A throttled fetch is retried by the next
  caller instead of being remembered for the whole TTL. The trade-off: waiters
  that arrive while a failing fetch is in flight retry one after another.
* **Invalidation is generation-guarded.** A fetch that started before an
  invalidation does not store its result, so a rotation mid-flight cannot
  resurrect the old value.
* **Jitter only shortens a lifetime.** `ttl - refresh_margin - jitter` means a
  served value is always younger than `ttl`; jitter de-synchronises a fleet
  without ever missing a rotation deadline.
* **EventBridge limits are structural.** `Batch` cannot be constructed outside
  the limits, so `dispatch` has no "too large" branch to get wrong. `plan`
  splits a long list greedily and preserves order.
* **Partial failure is attributed by position.** `PutEvents` returns results in
  request order and nothing else correlates them. If the service returns a
  different number of results than entries were sent, `dispatch` fails with
  `ResponseLengthMismatch` rather than guessing.
* **Unknown is carried, not dropped.** Unknown `detail-type`s, unknown stream
  event names and unknown fields all survive parsing.

## Deliberately not modelled

Silence is not an exclusion, so here is the list:

* **A DynamoDB-backed idempotency store.** The original crate had one bound to
  a specific table schema (`pk`, `sk`, `status`, TTL attribute) and a specific
  worker naming convention. Idempotency is a persistence decision: use your own
  table, or `mock-ddb` in tests. The primitives here (`ScheduledTick::event_id`,
  `StreamRecord::event_id`) are the keys you would use.
* **Retry loops and backoff.** `Error::is_retryable` and `is_ambiguous` tell you
  whether a retry is safe; when and how often to retry belongs to the event
  source (EventBridge, SQS redrive, Lambda destinations), not to the library.
* **An AWS SDK client builder.** `SecretsManagerFetcher::new` takes a
  `Client`; constructing one reads ambient AWS configuration, which is the
  caller's decision.
* **An opinionated logger.** No `tracing` subscriber setup, no JSON log format.
  The `tracing` feature exists so the crate can log without forcing a
  dependency on everyone.
* **Lambda runtime integration.** No `lambda_runtime` types. This crate is the
  plumbing *inside* the handler; the `main` stays yours.
* **Secret rotation scheduling.** No background refresher task: the cache
  refreshes on read, ahead of expiry, which keeps the Lambda model (no
  long-lived background work, frozen between invocations).

## Security notes

* `SecretString` has a redacting `Debug` (`SecretString("[redacted]")`), no
  `Display` and no `PartialEq`. The only accessor is `expose_secret()`, so every
  place a secret can leak is greppable. Comparison goes through `ct_eq`, built
  on `subtle`, so a key comparison is not a timing oracle.
* Error messages name the secret **id** and the configuration **key**, never a
  value: `ConfigError` deliberately drops the offending value from its message.
* Empty secrets fail closed (`EmptySecret`): `""` is a misconfiguration, and
  serving it turns a deploy mistake into a downstream 401.
* The SDK adapter never echoes a secret in `Debug`; the client is opaque there
  on purpose.
* Values are **not zeroised on drop**. `#![forbid(unsafe_code)]` rules out a
  hand-written wipe; if you need that guarantee, wrap the value with
  `zeroize::Zeroizing` at the call site (`expose_secret()` gives you the
  `&str`).
* Secrets are not written to disk, not put in `Result` payloads, and not
  embedded in `tracing` fields.

## Testing

```
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features          # unit, integration, property, doctests
cargo test --no-default-features   # the core without the parsers
cargo llvm-cov --features ddb --summary-only --fail-under-lines 95
```

Coverage is measured with every feature except `secrets-aws` (the SDK adapter's
only uncovered code is the network call itself). The current measurement is
**99.1 % of lines** across the compiled surface; the few uncovered lines are
`Option`/`Err` arms that need an impossible condition (a `serde_json`
serialisation failure of a `String`, for instance).

Fuzz targets (`fuzz/`, built and run for 2000 execs each in CI):

| Target | What it feeds |
| --- | --- |
| `eventbridge_envelope` | arbitrary bytes into the envelope and scheduled-tick parsers |
| `sqs_batch` | arbitrary bytes into the SQS batch parser, then into a failure report |
| `ddb_record` | arbitrary bytes into the Streams parser, including the ARN table-name extractor |

Properties (`tests/properties.rs`) pin the invariants the example tests cannot:

* a cached value is served while `age < ttl - refresh_margin` and never after;
* `parse_duration` is total (no panic) and unit arithmetic is consistent;
* a `Batch` is either rejected or within 10 entries / 256 KiB — there is no
  third state;
* `plan` is a partition: every entry appears exactly once, in order;
* partial failures are ordered by position and `accepted + failures == total`;
* the SQS failure report is de-duplicated and keeps first-occurrence order;
* every parser is total on arbitrary input.

## MSRV

`rust-version = "1.89"`, tested in CI. The `secrets-aws` feature follows the
AWS SDK's own MSRV, which is allowed to be newer.

## License

Apache-2.0 OR MIT.
