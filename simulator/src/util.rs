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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Add;

    #[test]
    fn test_printable_duration_zero() {
        let duration = Duration::from_secs(0);
        let formatted = printable_duration(duration).unwrap();
        assert_eq!(formatted, "00:00:00");
    }

    #[test]
    fn test_printable_duration_seconds() {
        let duration = Duration::from_secs(30);
        let formatted = printable_duration(duration).unwrap();
        assert_eq!(formatted, "00:00:30");
    }

    #[test]
    fn test_printable_duration_minutes() {
        let duration = Duration::from_secs(120);
        let formatted = printable_duration(duration).unwrap();
        assert_eq!(formatted, "00:02:00");
    }

    #[test]
    fn test_printable_duration_hours() {
        let duration = Duration::from_secs(3600);
        let formatted = printable_duration(duration).unwrap();
        assert_eq!(formatted, "01:00:00");
    }

    #[test]
    fn test_printable_duration_large() {
        let duration = Duration::from_secs(172800);
        let formatted = printable_duration(duration).unwrap();
        assert_eq!(formatted, "48:00:00");
    }

    #[test]
    fn test_printable_duration_mixed() {
        let duration = Duration::from_secs(3600 * 2 + 60 * 3 + 15); // 2 hours, 3 minutes, 15 seconds
        let formatted = printable_duration(duration).unwrap();
        assert_eq!(formatted, "02:03:15");
    }

    #[test]
    fn test_printable_small_duration_zero() {
        let duration = Duration::from_secs(0);
        let formatted = printable_small_duration(duration).unwrap();
        assert_eq!(formatted, "0s 0ms");
    }

    #[test]
    fn test_printable_small_duration_seconds() {
        let duration = Duration::from_secs(5);
        let formatted = printable_small_duration(duration).unwrap();
        assert_eq!(formatted, "5s 0ms");
    }

    #[test]
    fn test_printable_small_duration_milliseconds() {
        let duration = Duration::from_millis(500);
        let formatted = printable_small_duration(duration).unwrap();
        assert_eq!(formatted, "0s 500ms");
    }

    #[test]
    fn test_printable_small_duration_mixed() {
        let duration = Duration::from_secs(2).add(Duration::from_millis(750));
        let formatted = printable_small_duration(duration).unwrap();
        assert_eq!(formatted, "2s 750ms");
    }
}
