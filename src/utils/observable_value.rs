// Copyright 2025 Google LLC

//! This module implements a thin wrapper around a value, which makes changes to it observable.

use std::fmt::Debug;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// `ObservableValue` wraps a value T in an `Option<T>`, which is initially `None`. It exposes
/// getter and setter for the value, as well as a method to observe changes to it.
///
/// The `C` const parameter determines the capacity of the broadcast channel used internally to
/// implement observation of changes: If the `BroadcastStream`s created through `Self::observe` are
/// not read fast enough, the channel's buffer fills up. If the buffer is full, values will be
/// discarded in FIFO order.
pub struct ObservableValue<T: Clone + Debug + Send + 'static, const C: usize> {
    value: Option<T>,
    tx: broadcast::Sender<T>,
}

impl<T: Clone + Debug + Send + 'static, const C: usize> Default for ObservableValue<T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Debug + Send + 'static, const C: usize> ObservableValue<T, C> {
    const BUFFER_CAPACITY: usize = C;

    /// Creates a new `Self` without a value.
    pub fn new() -> Self {
        // rx is created on demand by calling `tx.subscribe()`.
        let (tx, _) = broadcast::channel(Self::BUFFER_CAPACITY);
        Self { value: None, tx }
    }

    /// Gets the value. Returns `None` if the value has never been set.
    pub fn get(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Sets the value.
    pub fn set(&mut self, value: T) {
        self.value = Some(value.clone());
        // This only fails if there are no receivers, which is fine.
        let _ = self.tx.send(value);
    }

    /// Returns a stream that can be used to observe changes to the value. The stream ends when
    /// `self` is dropped. The stream does _not_ include the currently set value, if present, but
    /// only future changes to the value. Use `Self::get` to fetch the currently set value
    /// if needed.
    pub fn observe(&self) -> BroadcastStream<T> {
        BroadcastStream::new(self.tx.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;
    use googletest::{
        expect_that,
        matchers::{anything, eq, err, none, ok, pat, some},
    };
    use tokio::time::{timeout, Duration};
    use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

    #[googletest::test]
    fn new_is_empty() {
        let observable = ObservableValue::<i32, 1>::new();
        expect_that!(observable.get(), none());
    }

    #[googletest::test]
    fn default_is_empty() {
        let observable = ObservableValue::<i32, 1>::default();
        expect_that!(observable.get(), none());
    }

    #[googletest::test]
    fn set_and_get() {
        let mut observable = ObservableValue::<i32, 1>::new();

        observable.set(42);
        expect_that!(observable.get(), some(eq(&42)));

        observable.set(1337);
        expect_that!(observable.get(), some(eq(&1337)));
    }

    #[googletest::test]
    #[tokio::test]
    #[sdv_telemetry_test_utils::timeout]
    async fn observe_receives_new_values() {
        let mut observable = ObservableValue::<i32, 1>::new();
        let mut observer = observable.observe();

        observable.set(1);
        expect_that!(observer.next().await, some(ok(eq(&1))));

        observable.set(2);
        expect_that!(observer.next().await, some(ok(eq(&2))));
    }

    #[googletest::test]
    #[tokio::test]
    #[sdv_telemetry_test_utils::timeout]
    async fn observe_after_set_does_not_receive_current_value() {
        let mut observable = ObservableValue::<i32, 1>::new();
        observable.set(42);

        let mut observer = observable.observe();

        expect_that!(
            timeout(Duration::from_millis(10), observer.next()).await,
            err(anything()),
            "Observer should not receive the value that was set before it subscribed."
        );

        observable.set(1337);
        expect_that!(observer.next().await, some(ok(eq(&1337))), "It should receive new values.");
    }

    #[googletest::test]
    #[tokio::test]
    #[sdv_telemetry_test_utils::timeout]
    async fn multiple_observers_receive_values() {
        let mut observable = ObservableValue::<String, 1>::new();
        let mut observer1 = observable.observe();
        let mut observer2 = observable.observe();

        observable.set("hello".to_string());
        expect_that!(observer1.next().await, some(ok(eq("hello"))));
        expect_that!(observer2.next().await, some(ok(eq("hello"))));

        observable.set("world".to_string());
        expect_that!(observer1.next().await, some(ok(eq("world"))));
        expect_that!(observer2.next().await, some(ok(eq("world"))));
    }

    #[googletest::test]
    #[tokio::test]
    #[sdv_telemetry_test_utils::timeout]
    async fn stream_ends_on_drop() {
        let observable = ObservableValue::<i32, 1>::new();
        let mut observer = observable.observe();

        drop(observable);

        expect_that!(observer.next().await, none());
    }

    #[googletest::test]
    #[tokio::test]
    #[sdv_telemetry_test_utils::timeout]
    async fn lagging_observer_drops_values() {
        const CAPACITY: usize = 2;
        let mut observable = ObservableValue::<&'static str, CAPACITY>::new();
        let mut observer = observable.observe();

        observable.set("a");
        observable.set("b");
        observable.set("c");
        observable.set("d");
        observable.set("e");

        expect_that!(
            observer.next().await,
            some(err(pat!(BroadcastStreamRecvError::Lagged(eq(&3))))),
            "The first three values should be dropped due to channel capacity ({CAPACITY})."
        );

        expect_that!(observer.next().await, some(ok(eq(&"d"))));
        expect_that!(observer.next().await, some(ok(eq(&"e"))));

        observable.set("f");
        observable.set("g");
        expect_that!(observer.next().await, some(ok(eq(&"f"))));
        expect_that!(observer.next().await, some(ok(eq(&"g"))));
    }
}
