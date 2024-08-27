// Copyright 2024 Google LLC

//! Provides atomic helpers.

use std::sync::atomic::{AtomicU64, Ordering};

/// The underlying type of a `SequenceNumber`.
pub type SequenceNumberType = u64;

/// An atomically incrementing sequence number. Produced values are guaranteed to not be re-used,
/// unless the internal counter overflows.
pub struct SequenceNumber {
    next: AtomicU64,
}

impl Default for SequenceNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceNumber {
    /// Construct a new `SequenceNumber`.
    pub const fn new() -> Self {
        Self { next: AtomicU64::new(0) }
    }

    /// Return the integer one greater than was returned by the previous call on this instance, or 0
    /// if there have been no such calls. Provided overflow does not occur, no two calls on the same
    /// instance will return the same value, even in the face of concurrency.
    pub fn next(&self) -> SequenceNumberType {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sequence_number_is_0() {
        let s = SequenceNumber::new();
        assert_eq!(s.next(), 0);
    }

    #[test]
    fn single_threaded_numbers_are_in_order() {
        let s = SequenceNumber::new();
        for i in 0..100000 {
            assert_eq!(s.next(), i);
        }
    }

    /// This test deliberately does not use `tokio::test` and async, but real OS threads instead.
    /// This makes the code easier to write (due to the availability of `std::thread::scope` and
    /// does not tie it to Tokio).
    #[test]
    fn multi_threaded_numbers_are_unique() {
        let s = SequenceNumber::new();
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            for _ in 1..=10 {
                scope.spawn(|| {
                    let mut thread_values = vec![];
                    for _ in 0..100000 {
                        thread_values.push(s.next());
                    }
                    tx.send(thread_values).unwrap();
                });
            }
        });

        let mut values = vec![];
        for _ in 1..=10 {
            values.append(&mut rx.recv().unwrap());
        }
        values.sort();

        let expected_values = (0..1000000).collect::<Vec<_>>();
        assert_eq!(values, expected_values);
    }
}
