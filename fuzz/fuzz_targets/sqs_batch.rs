#![no_main]
//! Feeds arbitrary bytes into the SQS batch parser and the failure-report
//! builder.
//!
//! What must hold for every input: no panic, every parsed message has a
//! non-empty id (otherwise it could not be reported as failed), and the report
//! never contains an empty identifier or a duplicate.

use libfuzzer_sys::fuzz_target;
use worker_shared::sqs::{run_batch, BatchItemFailures, SqsBatchEvent};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(event) = SqsBatchEvent::parse(text) else {
        return;
    };

    assert!(event.records.iter().all(|record| !record.message_id.is_empty()));

    // Every message becomes a failure id occasionally; the report must stay
    // well-formed for the service.
    let mut failures = BatchItemFailures::new();
    for (index, record) in event.records.iter().enumerate() {
        if index % 2 == 0 {
            failures.push(record.message_id.clone());
        }
    }
    assert!(failures.as_slice().iter().all(|id| !id.is_empty()));
    assert!(failures.len() <= event.records.len());
    let json = failures.to_json_string().expect("report serialises");
    assert!(json.starts_with("{\"batchItemFailures\":["));

    // The batch runner agrees with the positional map.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let reported = runtime.block_on(run_batch(&event, |index, _| async move { index % 2 != 0 }));
    assert_eq!(reported.len(), failures.len());
});
