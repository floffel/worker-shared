#![no_main]
//! Feeds arbitrary bytes into the DynamoDB Streams parser, including the
//! `eventSourceARN` → table-name extractor.
//!
//! What must hold for every input: no panic; a successful parse has at least
//! one record; every parsed record has a non-empty id; the ARN extractor never
//! returns an empty name.

use libfuzzer_sys::fuzz_target;
use worker_shared::ddb_streams::{table_from_arn, DdbStreamEvent};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    if let Ok(event) = DdbStreamEvent::parse(text) {
        assert!(!event.records.is_empty());
        assert!(event.records.iter().all(|record| !record.event_id.is_empty()));
    }

    // The ARN extractor is called on untrusted input too (event source ARNs
    // are configuration, but a replay can carry anything).
    if let Some(table) = table_from_arn(text) {
        assert!(!table.is_empty());
        assert!(!table.starts_with("table/"));
    }
});
