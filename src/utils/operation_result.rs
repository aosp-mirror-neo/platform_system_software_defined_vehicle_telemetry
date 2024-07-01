// Copyright 2024 Google LLC

//! Result of operation that can be either synchronous or asynchronous.

use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use std::future;

/// Operation result
pub enum OperationResult<'a, T, E> {
    /// The operation completed synchronously.
    Synchronous(Result<T, E>),
    /// The operation will complete asynchronously.
    Asynchronous(BoxFuture<'a, Result<T, E>>),
}

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

    /// Return asynchronous result of the operation or panic if the result is synchronous.
    pub fn unwrap_async(self) -> BoxFuture<'a, Result<T, E>> {
        match self {
            OperationResult::Synchronous(_) => panic!("The operation result is synchronous"),
            OperationResult::Asynchronous(future) => future,
        }
    }

    /// Return synchronous result of the operation or panic if the result is asynchronous.
    pub fn unwrap_sync(self) -> Result<T, E> {
        match self {
            OperationResult::Synchronous(result) => result,
            OperationResult::Asynchronous(_) => panic!("The operation result is asynchronous"),
        }
    }
}

impl<T, E> From<Result<T, E>> for OperationResult<'_, T, E> {
    fn from(result: Result<T, E>) -> Self {
        OperationResult::Synchronous(result)
    }
}

impl<'a, T, E> From<BoxFuture<'a, Result<T, E>>> for OperationResult<'a, T, E> {
    fn from(future: BoxFuture<'a, Result<T, E>>) -> Self {
        OperationResult::Asynchronous(future)
    }
}
