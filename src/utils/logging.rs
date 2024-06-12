// Copyright 2024 Google LLC

//! Helpers for ensuring valid log tags and initializing logging in samples and tests.

use log::LevelFilter;

/// Validate a string for use as a tag for logging.
///
/// This ensures that the log tag is less than or equal to 23 characters, as it will otherwise be
/// truncated by adb.
pub const fn validated_log_tag(log_tag: &str) -> &str {
    assert!(
        log_tag.len() <= 23,
        "Log tag should be less than or equal to 23 characters, as it will otherwise be truncated by adb."
    );
    log_tag
}

/// Initialize the logging infrastructure for samples and tests. Production code should use
/// `sdv_log::init_logger` instead.
///
/// This sets a log level of `LevelFilter::Trace`.
pub fn init_logger_for_samples_or_tests(log_tag: &str) {
    assert!(
        logger::init(
            logger::Config::default()
                .with_tag_on_device(log_tag)
                .with_max_level(LevelFilter::Trace)
        ),
        "Logger was already initialized."
    );
}
