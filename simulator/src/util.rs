// Copyright 2024 Google LLC

use std::time::Duration;

/// Returns the duration formatted as a string in the format HH:MM:SS.
pub fn printable_duration(duration: Duration) -> Result<String, std::fmt::Error> {
    let total_seconds = duration.as_secs();
    let seconds = total_seconds % 60;
    let minutes = (total_seconds / 60) % 60;
    let hours = (total_seconds / 60) / 60;
    Ok(format!("{hours:02}:{minutes:02}:{seconds:02}"))
}

/// Returns the duration formatted as a string showing seconds and milliseconds.
pub fn printable_small_duration(duration: Duration) -> Result<String, std::fmt::Error> {
    let seconds = duration.as_secs();
    let milliseconds = duration.subsec_millis();
    Ok(format!("{seconds}s {milliseconds}ms"))
}
