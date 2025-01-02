// Copyright 2024 Google LLC

//! Defines value holder that can be either immediately available, or when a future is resolved.

use futures::stream::{self, FuturesUnordered};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use std::future;

/// Holder of a value that can be either immediately available, or when a future is resolved.
#[must_use]
pub enum ValueHolder<'a, T> {
    /// The value is provided by a future.
    Asynchronous(BoxFuture<'a, T>),
    /// The value is immediately available.
    Synchronous(T),
}

impl<'a, T: 'a> ValueHolder<'a, T> {
    /// Collects results of all operations into a single unordered list.
    pub fn collect_unordered<I>(holders_iterator: I) -> ValueHolder<'a, Vec<T>>
    where
        I: IntoIterator<Item = ValueHolder<'a, T>>,
        T: Send,
    {
        let async_results = FuturesUnordered::new();
        let mut sync_results = Vec::new();
        for holder in holders_iterator {
            match holder {
                ValueHolder::Synchronous(result) => sync_results.push(result),
                ValueHolder::Asynchronous(future) => async_results.push(future),
            }
        }

        if async_results.is_empty() {
            // Return as a synchronous value if all results are available synchronously.
            ValueHolder::Synchronous(sync_results)
        } else {
            // Create a stream and collect it into a single future that returns a vector of results.
            ValueHolder::Asynchronous(
                stream::iter(sync_results).chain(async_results).collect().boxed(),
            )
        }
    }

    /// Map value in the holder.
    pub fn map<U>(self, op: impl FnOnce(T) -> U + Send + 'a) -> ValueHolder<'a, U> {
        match self {
            ValueHolder::Synchronous(value) => ValueHolder::Synchronous(op(value)),
            ValueHolder::Asynchronous(future) => ValueHolder::Asynchronous(future.map(op).boxed()),
        }
    }

    /// Chain a computation to `self`.
    pub fn then<U: Send + 'a>(
        self,
        op: impl FnOnce(T) -> ValueHolder<'a, U> + Send + 'a,
    ) -> ValueHolder<'a, U> {
        match self {
            ValueHolder::Synchronous(t) => op(t),
            ValueHolder::Asynchronous(future) => ValueHolder::Asynchronous(
                future
                    .then(|t| match op(t) {
                        ValueHolder::Synchronous(result) => future::ready(result).boxed(),
                        ValueHolder::Asynchronous(future) => future,
                    })
                    .boxed(),
            ),
        }
    }

    /// Convert `self` into a future.
    pub fn into_future(self) -> BoxFuture<'a, T>
    where
        T: Send,
    {
        match self {
            ValueHolder::Synchronous(t) => future::ready(t).boxed(),
            ValueHolder::Asynchronous(future) => future,
        }
    }

    /// Return underlying future or panic if the value is immediately available.
    pub fn unwrap_async(self) -> BoxFuture<'a, T> {
        match self {
            ValueHolder::Synchronous(_) => panic!("The value is immediately available"),
            ValueHolder::Asynchronous(future) => future,
        }
    }

    /// Return immediately available value or panic if is provided by a future.
    pub fn unwrap_sync(self) -> T {
        match self {
            ValueHolder::Synchronous(value) => value,
            ValueHolder::Asynchronous(_) => panic!("The value is provided by a future"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::future;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn collect_unordered_async_success() {
        let res = ValueHolder::collect_unordered(vec![
            ValueHolder::Synchronous(1),
            ValueHolder::Asynchronous(future::ready(2).boxed()),
        ])
        .unwrap_async()
        .await;

        assert_eq!(res, vec![1, 2]);
    }

    #[test]
    fn collect_unordered_all_sync_success() {
        let res = ValueHolder::collect_unordered(vec![
            ValueHolder::Synchronous(1),
            ValueHolder::Synchronous(2),
        ])
        .unwrap_sync();

        assert_eq!(res, vec![1, 2]);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn map_async_success() {
        let res =
            ValueHolder::Asynchronous(future::ready(1).boxed()).map(|x| x + 1).unwrap_async().await;

        assert_eq!(res, 2);
    }

    #[test]
    fn map_sync_success() {
        let res = ValueHolder::Synchronous(1).map(|x| x + 1).unwrap_sync();

        assert_eq!(res, 2);
    }

    #[test]
    fn then_sync_sync_success() {
        let res = ValueHolder::Synchronous(1).then(ValueHolder::Synchronous).unwrap_sync();

        assert_eq!(res, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn then_sync_async_success() {
        let res = ValueHolder::Synchronous(1)
            .then(|t| ValueHolder::Asynchronous(future::ready(t).boxed()))
            .unwrap_async()
            .await;

        assert_eq!(res, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn then_async_sync_success() {
        let res = ValueHolder::Asynchronous(future::ready(1).boxed())
            .then(ValueHolder::Synchronous)
            .unwrap_async()
            .await;

        assert_eq!(res, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn then_async_async_success() {
        let res = ValueHolder::Asynchronous(future::ready(1).boxed())
            .then(|t| ValueHolder::Asynchronous(future::ready(t).boxed()))
            .unwrap_async()
            .await;

        assert_eq!(res, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn sync_into_future_success() {
        let res = ValueHolder::Synchronous(1).into_future().await;

        assert_eq!(res, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn async_into_future_success() {
        let res = ValueHolder::Asynchronous(future::ready(1).boxed()).into_future().await;

        assert_eq!(res, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unwrap_async_success() {
        let res = ValueHolder::Asynchronous(future::ready(1).boxed()).unwrap_async().await;

        assert_eq!(res, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unwrap_async_failure() {
        ValueHolder::Synchronous(1).unwrap_async().await;
    }

    #[test]
    fn unwrap_sync_success() {
        let res = ValueHolder::Synchronous(1).unwrap_sync();

        assert_eq!(res, 1);
    }

    #[test]
    #[should_panic]
    fn unwrap_sync_failure() {
        ValueHolder::Asynchronous(future::ready(1).boxed()).unwrap_sync();
    }
}
