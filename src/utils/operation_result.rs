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

//! Defines functions for an operation that can be either synchronous or asynchronous.

use futures::{FutureExt, TryFutureExt};
use std::future;

use crate::value_holder::ValueHolder;

/// Result of an operation that can be either synchronous or asynchronous.
pub type OperationResult<'a, T, E> = ValueHolder<'a, Result<T, E>>;

impl<'a, T: 'a, E: 'a> OperationResult<'a, T, E> {
    /// Executes another operation after this one resolves successfully.
    /// The success value is passed to a closure to create this subsequent operation result.
    pub fn and_then<U: Send + 'a>(
        self,
        op: impl FnOnce(T) -> OperationResult<'a, U, E> + Send + 'a,
    ) -> OperationResult<'a, U, E>
    where
        E: Send,
    {
        match self {
            OperationResult::Synchronous(Ok(t)) => op(t),
            OperationResult::Synchronous(Err(e)) => Err(e).into(),
            OperationResult::Asynchronous(future) => future
                .and_then(|t| match op(t) {
                    OperationResult::Synchronous(result) => future::ready(result).boxed(),
                    OperationResult::Asynchronous(future) => future,
                })
                .boxed()
                .into(),
        }
    }

    /// Executes another synchronous operation after this one resolves successfully.
    /// The success value is passed to a closure to create this subsequent operation result.
    pub fn and_then_sync<U>(
        self,
        op: impl FnOnce(T) -> Result<U, E> + Send + 'a,
    ) -> OperationResult<'a, U, E> {
        match self {
            OperationResult::Synchronous(result) => result.and_then(op).into(),
            OperationResult::Asynchronous(future) => {
                future.map(|res| res.and_then(op)).boxed().into()
            }
        }
    }

    /// Maps operation result's Error to a new error type using the Into trait.
    pub fn err_into<F: 'a>(self) -> OperationResult<'a, T, F>
    where
        E: Into<F>,
    {
        match self {
            OperationResult::Synchronous(result) => result.map_err(|e| e.into()).into(),
            OperationResult::Asynchronous(future) => future.err_into().boxed().into(),
        }
    }

    /// Maps operation result's Error value to a different value.
    pub fn map_err<F>(self, op: impl FnOnce(E) -> F + Send + 'a) -> OperationResult<'a, T, F> {
        match self {
            OperationResult::Synchronous(result) => result.map_err(op).into(),
            OperationResult::Asynchronous(future) => future.map_err(op).boxed().into(),
        }
    }

    /// Maps operation result's Success value to a different value.
    pub fn map_ok<U>(self, op: impl FnOnce(T) -> U + Send + 'a) -> OperationResult<'a, U, E> {
        match self {
            OperationResult::Synchronous(result) => result.map(op).into(),
            OperationResult::Asynchronous(future) => future.map_ok(op).boxed().into(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn and_then_first_async_err_returns_err() {
        let err = OperationResult::Asynchronous(future::ready(Err(1)).boxed())
            .and_then(|_: String| -> OperationResult<f32, _> { unreachable!() })
            .unwrap_async()
            .await
            .unwrap_err();

        assert_eq!(err, 1);
    }

    #[test]
    fn and_then_first_sync_err_returns_err() {
        let err = OperationResult::Synchronous(Err(1))
            .and_then(|_: String| -> OperationResult<f32, _> { unreachable!() })
            .unwrap_sync()
            .unwrap_err();

        assert_eq!(err, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn and_then_async_sync_success() {
        let res = OperationResult::Asynchronous(future::ready(Ok::<_, String>(1)).boxed())
            .and_then(|i| OperationResult::Synchronous(Ok(i + 1)))
            .unwrap_async()
            .await
            .unwrap();

        assert_eq!(res, 2);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn and_then_async_async_success() {
        let res = OperationResult::Asynchronous(future::ready(Ok::<_, String>(1)).boxed())
            .and_then(|i| OperationResult::Asynchronous(future::ready(Ok(i + 1)).boxed()))
            .unwrap_async()
            .await
            .unwrap();

        assert_eq!(res, 2);
    }

    #[test]
    fn and_then_sync_sync_success() {
        let res = OperationResult::Synchronous(Ok::<_, String>(1))
            .and_then(|i| OperationResult::Synchronous(Ok(i + 1)))
            .unwrap_sync()
            .unwrap();

        assert_eq!(res, 2);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn and_then_sync_first_async_err_returns_err() {
        let err = OperationResult::Asynchronous(future::ready(Err(1)).boxed())
            .and_then_sync(|_: String| -> Result<f32, _> { unreachable!() })
            .unwrap_async()
            .await
            .unwrap_err();

        assert_eq!(err, 1);
    }

    #[test]
    fn and_then_sync_first_sync_err_returns_err() {
        let err = OperationResult::Synchronous(Err(1))
            .and_then_sync(|_: String| -> Result<f32, _> { unreachable!() })
            .unwrap_sync()
            .unwrap_err();

        assert_eq!(err, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn and_then_sync_first_async_success() {
        let res = OperationResult::Asynchronous(future::ready(Ok::<_, String>(1)).boxed())
            .and_then_sync(|i| Ok(i + 1))
            .unwrap_async()
            .await
            .unwrap();

        assert_eq!(res, 2);
    }

    #[test]
    fn and_then_sync_first_sync_success() {
        let res = OperationResult::Synchronous(Ok::<_, String>(1))
            .and_then_sync(|i| Ok(i + 1))
            .unwrap_sync()
            .unwrap();

        assert_eq!(res, 2);
    }
}
