// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

/// Returns the duration formatted as a string in the format HH:MM:SS.
pub fn printable_duration(duration: &Duration) -> Result<String, std::fmt::Error> {
    let total_seconds = duration.as_secs();
    let seconds = total_seconds % 60;
    let minutes = (total_seconds / 60) % 60;
    let hours = (total_seconds / 60) / 60;
    Ok(format!("{hours:02}:{minutes:02}:{seconds:02}"))
}

/// Returns the duration formatted as a string showing seconds and milliseconds.
pub fn printable_small_duration(duration: &Duration) -> Result<String, std::fmt::Error> {
    let seconds = duration.as_secs();
    let milliseconds = duration.subsec_millis();
    Ok(format!("{seconds}s {milliseconds}ms"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Add;

    use sdv_telemetry_test_utils::parameterized_test;

    parameterized_test!(
        #[test]
        printable_duration,

        |duration, expected| {
            let formatted = printable_duration(&duration).unwrap();
            assert_eq!(formatted, expected);
        },

        zero: (Duration::ZERO, "00:00:00",),
        seconds: (Duration::from_secs(30), "00:00:30",),
        minutes: (Duration::from_secs(120), "00:02:00",),
        hours: (Duration::from_secs(3600), "01:00:00",),
        large: (Duration::from_secs(172800), "48:00:00",),
        mixed: (
            Duration::from_secs(3600 * 2 + 60 * 3 + 15), // 2 hours, 3 minutes, 15 seconds
            "02:03:15",
        ),
    );

    parameterized_test!(
        #[test]
        printable_small_duration,

        |duration, expected| {
            let formatted = printable_small_duration(&duration).unwrap();
            assert_eq!(formatted, expected);
        },

        zero: (Duration::ZERO, "0s 0ms",),
        seconds: (Duration::from_secs(5), "5s 0ms",),
        milliseconds: (Duration::from_millis(500), "0s 500ms",),
        mixed: (Duration::from_secs(2).add(Duration::from_millis(750)), "2s 750ms",),
    );
}
