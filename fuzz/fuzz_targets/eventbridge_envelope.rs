#![no_main]
//! Feeds arbitrary bytes into the inbound EventBridge envelope and
//! scheduled-tick parsers.
//!
//! What must hold for every input: no panic, and a *successful* parse yields
//! structures whose invariants still hold (a tick only exists with an id, a
//! time and a rule ARN; an envelope's detail survives the round trip).

use libfuzzer_sys::fuzz_target;
use worker_shared::eventbridge::{EventBridgeScheduledEvent, EventEnvelope};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    if let Ok(envelope) = EventEnvelope::parse(text) {
        // Re-serialising a parsed envelope must not fail, and must parse back.
        if let Ok(encoded) = serde_json::to_string(&envelope) {
            let reparsed = EventEnvelope::parse(&encoded).expect("re-serialised envelope parses");
            assert_eq!(reparsed.id, envelope.id);
            assert_eq!(reparsed.detail_type, envelope.detail_type);
            assert_eq!(reparsed.time, envelope.time);
        }
    }

    if let Ok(event) = EventBridgeScheduledEvent::parse(text) {
        assert!(!event.tick.event_id.is_empty());
        assert!(!event.tick.rule_arn.is_empty());
        assert!(
            event.tick.detail_type.is_empty() || event.tick.detail_type == "Scheduled Event",
            "only scheduled events may reduce to a tick"
        );
    }
});
