// Copyright 2024 Google LLC

//! Wrapper around the C time APIs.

use sdv_telemetry_time_bindgen::{getMonotonicTimeSinceBoot, getUnixTimestamp};

/// Wrapper around an `i64` that represents a duration in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Microseconds(pub i64);

/// Get the current system time in microseconds as a UNIX timestamp.
pub fn get_unix_timestamp() -> Microseconds {
    // Why do we not use `SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap()` here?
    // Because Rust's duration only supports positive durations, so any dates before
    // `SystemTime::UNIX_EPOCH` would cause an error/panic. Instead, we use the C-API, which treats
    // such dates as negative numbers.

    // SAFETY: Always safe to call.
    Microseconds(unsafe { getUnixTimestamp() })
}

/// Get the amount of time since boot in microseconds.
pub fn get_monotonic_time_since_boot() -> Microseconds {
    // SAFETY: Always safe to call.
    Microseconds(unsafe { getMonotonicTimeSinceBoot() })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::SystemTime;

    #[test]
    fn get_unix_timestamp_success() {
        let c_timestamp1 = get_unix_timestamp();
        let rust_timestamp = i64::try_from(
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros(),
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let c_timestamp2 = get_unix_timestamp();

        assert!(
            (rust_timestamp - c_timestamp1.0).abs() < 50_000,
            "Expected: ({rust_timestamp:?} - {c_timestamp1:?}).abs() < 50"
        );

        // Note: These could fail in the rare case where the clock jumps during the call to `sleep`
        // above.
        assert!(c_timestamp2.0 > c_timestamp1.0, "Expected: {c_timestamp2:?} > {c_timestamp1:?}");
        assert!(
            c_timestamp2.0 - c_timestamp1.0 > 50_000,
            "Expected: {c_timestamp2:?} - {c_timestamp1:?} > 50"
        );
        assert!(
            c_timestamp2.0 - c_timestamp1.0 < 150_000,
            "Expected: {c_timestamp2:?} - {c_timestamp1:?} < 150"
        );
    }

    #[test]
    fn get_monotonic_time_since_boot_success() {
        let c_timestamp1 = get_monotonic_time_since_boot();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let c_timestamp2 = get_monotonic_time_since_boot();

        assert!(c_timestamp1.0 > 0);
        assert!(c_timestamp2.0 > c_timestamp1.0, "Expected: {c_timestamp2:?} > {c_timestamp1:?}");
        assert!(
            c_timestamp2.0 - c_timestamp1.0 > 50_000,
            "Expected: {c_timestamp2:?} - {c_timestamp1:?} > 50"
        );
        assert!(
            c_timestamp2.0 - c_timestamp1.0 < 150_000,
            "Expected: {c_timestamp2:?} - {c_timestamp1:?} < 150"
        );
    }
}
