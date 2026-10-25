//! Environment configuration: typed reads, validation, no panics, no value
//! leakage.

mod common;

use std::time::Duration;

use worker_shared::config::{parse_duration, Config, Env, MapSource, Source};
use worker_shared::{ConfigError, Error};

fn config(pairs: &[(&str, &str)]) -> Config<MapSource> {
    Config::new(
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<MapSource>(),
    )
}

#[test]
fn missing_and_empty_are_distinct_errors() {
    let config = config(&[("EMPTY", "")]);

    let missing = config.required("ABSENT").expect_err("absent");
    assert!(matches!(missing, ConfigError::Missing { .. }));
    assert_eq!(missing.key(), "ABSENT");
    assert_eq!(
        missing.to_string(),
        "missing required environment variable `ABSENT`"
    );

    let empty = config.required("EMPTY").expect_err("empty");
    assert!(matches!(empty, ConfigError::Empty { .. }));
    assert_eq!(empty.to_string(), "environment variable `EMPTY` is empty");

    assert_eq!(config.optional("EMPTY"), None);
    assert_eq!(config.optional("ABSENT"), None);
}

#[test]
fn required_parse_reports_the_key_but_never_the_value() {
    let config = config(&[("PORT", "not-a-port"), ("SECRET", "hunter2")]);

    let error = config.required_parse::<u16>("PORT").expect_err("invalid");
    assert_eq!(error.key(), "PORT");
    let rendered = error.to_string();
    assert!(rendered.contains("PORT"));
    assert!(
        !rendered.contains("not-a-port"),
        "values must not leak: {rendered}"
    );

    assert_eq!(
        config
            .required_parse::<u8>("MISSING")
            .expect_err("missing")
            .key(),
        "MISSING"
    );
    assert_eq!(config.required("SECRET").expect("value"), "hunter2");
}

#[test]
fn parse_reads_present_values_and_reports_bad_ones() {
    let config = config(&[("N", "42"), ("BAD", "x")]);
    assert_eq!(config.parse::<u8>("N").expect("value"), Some(42));
    let error = config.parse::<u8>("BAD").expect_err("invalid");
    assert_eq!(error.key(), "BAD");
    assert!(
        !error.to_string().contains("`BAD` is invalid: x"),
        "value must not leak"
    );
    assert_eq!(config.u64("N").expect("value"), Some(42));
}

#[test]
fn optional_parsers_return_none_when_unset() {
    let config = config(&[]);
    assert_eq!(config.parse::<u16>("N").expect("none"), None);
    assert_eq!(config.u64("N").expect("none"), None);
    assert_eq!(config.bool("N").expect("none"), None);
    assert_eq!(config.duration("N").expect("none"), None);
}

#[test]
fn booleans_accept_the_documented_spellings_only() {
    for (raw, expected) in [
        ("1", true),
        ("true", true),
        ("TRUE", true),
        ("yes", true),
        ("On", true),
        ("0", false),
        ("false", false),
        ("NO", false),
        ("off", false),
        ("  true  ", true),
    ] {
        let config = config(&[("FLAG", raw)]);
        assert_eq!(config.bool("FLAG").expect(raw), Some(expected), "{raw}");
        assert_eq!(
            config.flag("FLAG", !expected).expect(raw),
            expected,
            "{raw}"
        );
    }

    // Empty counts as unset…
    assert_eq!(config(&[("FLAG", "")]).bool("FLAG").expect("empty"), None);
    // …and every other unrecognised spelling is an error, not a silent false.
    for raw in ["enabled", "2", "y"] {
        let config = config(&[("FLAG", raw)]);
        let error = config.bool("FLAG").expect_err(raw);
        assert_eq!(error.key(), "FLAG");
        assert!(error.to_string().contains("expected one of"));
    }

    assert!(config(&[]).flag("UNSET", true).expect("default"));
    assert!(!config(&[]).flag("UNSET", false).expect("default"));
}

#[test]
fn durations_parse_from_bare_seconds_and_suffixes() {
    let cases = [
        ("0", Duration::ZERO),
        ("30", Duration::from_secs(30)),
        ("250ms", Duration::from_millis(250)),
        ("90s", Duration::from_secs(90)),
        ("5m", Duration::from_secs(300)),
        ("2h", Duration::from_secs(7_200)),
        ("1d", Duration::from_secs(86_400)),
        ("1h30m", Duration::from_secs(5_400)),
        ("1h30m15s", Duration::from_secs(5_415)),
        ("1h30", Duration::from_secs(3_630)),
        (" 10 ", Duration::from_secs(10)),
    ];
    for (raw, expected) in cases {
        assert_eq!(parse_duration(raw), Some(expected), "{raw}");
        let config = config(&[("BUDGET", raw)]);
        assert_eq!(config.duration("BUDGET").expect(raw), Some(expected));
    }

    for raw in [
        "", " ", "ms", "10x", "10.5s", "-5", "abc", "1h30x", "h", "s", "10 s",
    ] {
        assert_eq!(parse_duration(raw), None, "{raw}");
    }

    let error = config(&[("BUDGET", "10x")])
        .duration("BUDGET")
        .expect_err("invalid");
    assert_eq!(error.key(), "BUDGET");
    assert!(error.to_string().contains("expected a duration"));
}

#[test]
fn durations_that_overflow_are_rejected() {
    assert_eq!(parse_duration("18446744073709551616"), None);
    assert_eq!(parse_duration("18446744073709551615s"), None);
    assert_eq!(parse_duration("999999999999999999d"), None);
}

#[test]
fn map_source_builders_and_trait_impls() {
    let source = MapSource::new().with("A", "1").with("B", "2");
    assert_eq!(source.get("A").as_deref(), Some("1"));
    assert_eq!(source.get("B").as_deref(), Some("2"));
    assert_eq!(source.get("C"), None);

    let from_array = MapSource::from([("K", "v")]);
    assert_eq!(from_array.get("K").as_deref(), Some("v"));

    let from_iter: MapSource = vec![("X", "1"), ("X", "2")].into_iter().collect();
    assert_eq!(from_iter.get("X").as_deref(), Some("2"), "last write wins");

    let config = Config::new(from_iter.clone());
    assert_eq!(config.source().get("X").as_deref(), Some("2"));

    let debug = format!("{from_array:?}");
    assert!(debug.contains("MapSource"));
    assert_eq!(MapSource::default().get("anything"), None);
}

#[test]
fn env_source_reads_the_process_environment_without_panicking() {
    let env = Env;
    assert!(env.get("PATH").is_some(), "PATH is always set on CI");
    assert_eq!(env.get("WORKER_SHARED_DEFINITELY_UNSET_VAR"), None);
    assert_eq!(format!("{env:?}"), "Env");
    assert_eq!(
        Config::from_env().source().get("PATH"),
        Some(std::env::var("PATH").expect("path"))
    );

    // A real variable round-trips through the typed layer.
    std::env::set_var("WORKER_SHARED_TEST_VALUE", "42");
    let value = Config::from_env()
        .required_parse::<u32>("WORKER_SHARED_TEST_VALUE")
        .expect("typed read");
    assert_eq!(value, 42);
    std::env::remove_var("WORKER_SHARED_TEST_VALUE");
}

#[test]
fn config_errors_convert_into_the_crate_error() {
    let config = config(&[]);
    let error: Error = config.required("MISSING").expect_err("missing").into();
    assert_eq!(error.code(), "config");
    assert!(error.to_string().starts_with("configuration error:"));
    assert!(!error.is_retryable());
    assert!(!error.is_ambiguous());
}

#[test]
fn unicode_values_are_returned_verbatim() {
    let config = config(&[("LABEL", "Grüße 🚀")]);
    assert_eq!(config.required("LABEL").expect("value"), "Grüße 🚀");
    assert!(config.required_parse::<u8>("LABEL").is_err());
}
