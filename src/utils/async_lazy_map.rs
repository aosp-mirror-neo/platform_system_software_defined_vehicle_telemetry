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

//! This module provides a thread-safe map that lazily initializes values asynchronously.
//!
//! [`AsyncLazyMap`] allows values to be initialized only when requested.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedMappedMutexGuard;
use tokio::sync::OwnedMutexGuard;

/// A map that lazily initializes its values asynchronously.
///
/// `AsyncLazyMap` allows concurrent access to a collection of values where each value is
/// initialized only when requested. Initialization is performed asynchronously and is guaranteed to
/// happen at most once per key (unless initialization fails or the entry is removed using
/// `AsyncLazyMap::take_if`).
///
/// This structure is thread-safe. Concurrent requests for the same key will wait for the ongoing
/// initialization to complete and then share the result. Requests for different keys can proceed in
/// parallel.
#[derive(Default)]
pub struct AsyncLazyMap<K: Eq + Hash, V>(
    // The use of a top-level `Mutex` may appear odd. However, we chose this over the following
    // options:
    // - `Arc<Mutex<HashMap<...>>>`: If we wrapped the top-level mutex in an `Arc`, we'd also need
    //   to implement `Clone` for `AsyncLazyMap`. However, maybe counterintuitively, the clone
    //   implementation would just perform a shallow-clone, because it'd just clone the `Arc`. By
    //   letting the caller wrap `AsyncLazyMap` into an `Arc`, it becomes much clearer to the caller
    //   that using `Arc::clone()` on the `Arc<AsyncLazyMap>` only performs a shallow-clone.
    // - `HashMap<...>`: Instead of wrapping the `HashMap` into a `Mutex`, we could've just made all
    //   of our methods take `&mut self` instead of `&self`. However, this would effectively lock
    //   the entire `AsyncLazyMap` while *any* of its keys are initialized, since the
    //   `get_or_try_init(&mut self)` method would only return once the asynchronous initialization
    //   was completed. This is counter to the goal of this struct.
    Mutex<
        HashMap<
            K,
            Arc<
                AsyncMutex<
                    // If the `HashMap` doesn't have an entry for the key, we need to initialize the
                    // value synchronously. By using `Option<V>`, we can construct the entire
                    // `Arc<AsyncMutex<Option<V>>>` synchronously by using `None` as the value.
                    Option<V>,
                >,
            >,
        >,
    >,
);

impl<K: Eq + Hash, V> AsyncLazyMap<K, V> {
    /// Creates a new, empty `AsyncLazyMap`.
    pub fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    /// Returns an `OwnedMappedMutexGuard` to a value from the map if it exists and is initialized.
    ///
    /// If the key is not present in the map, or if the value has not yet been initialized (or
    /// initialization failed), this returns `None`.
    ///
    /// If initialization is currently in progress for the key, this method will wait for it to
    /// complete.
    ///
    /// The returned `OwnedMappedMutexGuard` grants exclusive (mutable) access to the value. The
    /// lock on the specific key is held until the guard is dropped.
    pub async fn get(&self, key: &K) -> Option<OwnedMappedMutexGuard<Option<V>, V>> {
        let value = {
            // Important: Do not lock `self.0` for longer than absolutely necessary, as that locks
            // the entire map.
            let map_mutex_guard = self.0.lock();
            let value = map_mutex_guard.get(key)?;
            Arc::clone(value)
        };

        let value_mutex_guard = value.lock_owned().await;
        if value_mutex_guard.is_none() {
            return None;
        }
        Some(OwnedMutexGuard::map(value_mutex_guard, |value| {
            // It is safe to `unwrap` here, because we checked that the value is not `None` above
            // and we are still holding the mutex guard.
            //
            // Returning `V` instead of `Option<V>` here nicely hides the fact that `V` is
            // internally wrapped into an `Option` from the caller.
            value.as_mut().unwrap()
        }))
    }

    /// Returns an `OwnedMappedMutexGuard` to a value from the map, initializing it if necessary.
    ///
    /// If the key is not present in the map, or if the value has not yet been initialized, the
    /// `try_init` closure is called. The closure should return a future that resolves to a
    /// `Result<V, E>`.
    ///
    /// - If `init` succeeds, the value is stored and a mutex guard to it is returned.
    /// - If `init` fails, the error is returned, and the key remains uninitialized (subsequent
    ///   calls will retry initialization).
    ///
    /// The returned `OwnedMappedMutexGuard` grants exclusive (mutable) access to the value. The
    /// lock on the specific key is held until the guard is dropped.
    pub async fn get_or_try_init<F, Fut, E>(
        &self,
        key: K,
        try_init: F,
    ) -> Result<OwnedMappedMutexGuard<Option<V>, V>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        let value = {
            // Important: Do not lock `self.0` for longer than absolutely necessary, as that locks
            // the entire map.
            let mut map_mutex_guard = self.0.lock();
            let value = map_mutex_guard.entry(key).or_default();
            Arc::clone(value)
        };

        let mut value_mutex_guard = value.lock_owned().await;
        if value_mutex_guard.is_none() {
            let value = try_init().await?;
            *value_mutex_guard = Some(value);
        }
        Ok(OwnedMutexGuard::map(value_mutex_guard, |value| {
            // It is safe to `unwrap` here, because the value was initialized above and we are still
            // holding the mutex guard.
            //
            // Returning `V` instead of `Option<V>` here nicely hides the fact that `V` is
            // internally wrapped into an `Option` from the caller.
            value.as_mut().unwrap()
        }))
    }

    /// Removes and returns the value associated with the key if the predicate returns `true`.
    ///
    /// The predicate receives a mutable reference to the value.
    ///
    /// If initialization is currently in progress for the key, this method will wait for it to
    /// complete.
    ///
    /// Returns `None` if:
    /// - The key is not present.
    /// - The value is not initialized.
    /// - The predicate returns `false`.
    pub async fn take_if(&self, key: &K, pred: impl FnOnce(&mut V) -> bool) -> Option<V> {
        let value = {
            // Important: Do not lock `self.0` for longer than absolutely necessary.
            let map_mutex_guard = self.0.lock();
            let value = map_mutex_guard.get(key)?;
            Arc::clone(value)
        };

        let mut value_mutex_guard = value.lock().await;
        let result = value_mutex_guard.take_if(pred);
        if result.is_some() {
            // If we successfully extracted the value from `value_mutex_guard`, it means that the
            // value stored in the map is now `Arc<AsyncMutex<None>>`. The code below attempts to
            // remove the entry from the map entirely. The complication here is that we need to
            // ensure that this _doesn't_ remove the map entry if `get_or_try_init` has been running
            // concurrently and currently holds a reference to the same `Arc`, which it will
            // initialize as soon as we drop the `value_mutex_guard`.
            //
            // Therefore, we synchronously lock the map again and only remove the entry if:
            // 1. The map still has an entry for the `key`.
            // 2. The `Arc` stored in the map is still `Arc::ptr_eq` to the `Arc` we retrieved from
            //    the map above.
            // 3. Only two references to the `Arc` exist (`Arc::strong_count`). Why 2? Because the
            //    first reference is the `value` retrieved from the map above, and the second
            //    reference is the value received via `map_mutex_guard.get(key)` below. Since we are
            //    holding a mutex guard to the map, and the map value `Arc`s are only ever cloned
            //    while the map is locked, we are guaranteed that no further `Arc`s are created
            //    concurrently.
            {
                // Important: Do not lock `self.0` for longer than absolutely necessary.
                let mut map_mutex_guard = self.0.lock();
                if let Some(stored_value) = map_mutex_guard.get(key) {
                    if Arc::ptr_eq(&value, stored_value) && Arc::strong_count(&value) == 2 {
                        // It is safe to drop the mutex guard now - we know that nobody else may be
                        // awaiting this `Arc<AsyncMutex>` because all references to the
                        // `Arc<AsyncMutex>` are held by us, and new references cannot be acquired
                        // since the map is locked.
                        drop(value_mutex_guard);

                        // It is safe to call `.unwrap()` now because we have a mutable reference to
                        // the map and checked for the presence of the key using
                        // `map_mutex_guard.get(key)` above. Ideally, we'd use `HashMap::entry`
                        // above, but that would require an owned `key`.
                        let arc1 = map_mutex_guard.remove(key).unwrap();
                        let arc2 = value;
                        // The following assertions can never fail, because we just checked them
                        // above. They are merely here to catch potential future errors caused by
                        // refactorings of this code.
                        assert!(Arc::ptr_eq(&arc1, &arc2));
                        assert_eq!(Arc::strong_count(&arc1), 2);
                        assert_eq!(Arc::strong_count(&arc2), 2);
                        // Explicitly drop the `Arc`s to ensure that they are truly gone and not
                        // accidentally used below.
                        drop(arc1);
                        drop(arc2);
                    }
                }
            }
        }
        result
    }
}

impl<K: Eq + Hash, V> fmt::Debug for AsyncLazyMap<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncLazyMap").field(&self.0).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{derefs_to, eq, err, len, none, ok, some};
    use std::convert::Infallible;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::sleep;

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn get_returns_none_if_missing() {
        const KEY: usize = 1;
        let map = AsyncLazyMap::<usize, i32>::new();
        let result = map.get(&KEY).await;
        expect_that!(result, none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn get_returns_value_if_present() {
        const KEY: usize = 1;
        let map = AsyncLazyMap::new();
        let value_mutex_guard =
            map.get_or_try_init(KEY, || async { Ok::<_, Infallible>(100) }).await.unwrap();
        drop(value_mutex_guard);

        let result = map.get(&KEY).await;
        expect_that!(result, some(derefs_to(eq(&100))));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn get_waits_for_init() {
        const KEY: usize = 1;
        let map = Arc::new(AsyncLazyMap::new());
        let map_clone = Arc::clone(&map);
        let (tx, rx) = oneshot::channel();

        let init_handle = tokio::spawn(async move {
            let value_mutex_guard = map_clone
                .get_or_try_init(KEY, || async {
                    tx.send(()).unwrap();
                    sleep(Duration::from_millis(50)).await;
                    Ok::<_, Infallible>(100)
                })
                .await
                .unwrap();
            expect_that!(value_mutex_guard, derefs_to(eq(&100)));
            drop(value_mutex_guard);
        });

        // Wait for init to start
        rx.await.unwrap();

        // Try to get while init is running
        let result = map.get(&KEY).await;
        expect_that!(result, some(derefs_to(eq(&100))));

        init_handle.await.unwrap();
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn init_is_called_only_once() {
        let map = AsyncLazyMap::new();
        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);

        let val = map
            .get_or_try_init(1, || async move {
                *counter_clone.lock() += 1;
                Ok::<_, Infallible>(10)
            })
            .await
            .unwrap();
        expect_that!(*val, eq(10));
        drop(val);

        let counter_clone = Arc::clone(&counter);
        let val = map
            .get_or_try_init(1, || async move {
                *counter_clone.lock() += 1;
                Ok::<_, Infallible>(20)
            })
            .await
            .unwrap();
        expect_that!(*val, eq(10));

        expect_that!(*counter.lock(), eq(1));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn init_failure() {
        const KEY: usize = 1;
        let map = AsyncLazyMap::new();

        let result = map.get_or_try_init(KEY, || async { Err::<i32, _>("error") }).await;
        expect_that!(result, err(eq(&"error")));
        drop(result);

        let result = map.get_or_try_init(KEY, || async { Ok::<_, Infallible>(42) }).await;
        expect_that!(result, ok(derefs_to(eq(&42))));
        drop(result);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn take_if_removes_entry() {
        const KEY: usize = 1;
        let map = AsyncLazyMap::new();

        let value_mutex_guard =
            map.get_or_try_init(KEY, || async { Ok::<_, Infallible>(100) }).await.unwrap();
        expect_that!(*value_mutex_guard, eq(100));
        expect_that!(*map.0.lock(), len(eq(1)));
        drop(value_mutex_guard);

        let value = map.take_if(&KEY, |v| *v == 123).await;
        expect_that!(value, none());
        expect_that!(*map.0.lock(), len(eq(1)));

        let value = map.take_if(&KEY, |v| *v == 100).await;
        expect_that!(value, some(eq(100)));
        expect_that!(*map.0.lock(), len(eq(0)));

        let value_mutex_guard =
            map.get_or_try_init(KEY, || async move { Ok::<_, Infallible>(200) }).await.unwrap();
        expect_that!(*value_mutex_guard, eq(200));
        expect_that!(*map.0.lock(), len(eq(1)));
        drop(value_mutex_guard);

        let value = map.take_if(&KEY, |v| *v == 100).await;
        expect_that!(value, none());
        expect_that!(*map.0.lock(), len(eq(1)));

        let value = map.take_if(&KEY, |v| *v == 200).await;
        expect_that!(value, some(eq(200)));
        expect_that!(*map.0.lock(), len(eq(0)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn take_if_predicate_mismatch_does_not_remove() {
        const KEY: usize = 1;
        let map = AsyncLazyMap::new();
        let value_mutex_guard =
            map.get_or_try_init(KEY, || async { Ok::<_, Infallible>(10) }).await.unwrap();
        drop(value_mutex_guard);

        let taken = map.take_if(&KEY, |v| *v == 99).await;
        expect_that!(taken, none());

        // Next get should return original value without re-init
        let counter = Arc::new(Mutex::new(0));
        let counter_clone = Arc::clone(&counter);
        let value_mutex_guard = map
            .get_or_try_init(KEY, || async move {
                *counter_clone.lock() += 1;
                Ok::<_, Infallible>(20)
            })
            .await
            .unwrap();

        expect_that!(value_mutex_guard, derefs_to(eq(&10)));
        expect_that!(*counter.lock(), eq(0));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn recursive_init_different_keys() {
        let map = Arc::new(AsyncLazyMap::new());
        let map_clone = Arc::clone(&map);

        let value_mutex_guard = map
            .get_or_try_init(1, || async move {
                // Init key 2 inside key 1 init
                let val2 = map_clone
                    .get_or_try_init(2, || async { Ok::<_, Infallible>(20) })
                    .await
                    .unwrap();
                expect_that!(*val2, eq(20));
                Ok::<_, Infallible>(10)
            })
            .await
            .unwrap();

        expect_that!(value_mutex_guard, derefs_to(eq(&10)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn concurrent_access() {
        const KEY: usize = 1;
        let map = Arc::new(AsyncLazyMap::new());
        let map_clone = Arc::clone(&map);
        let (tx, rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            *map_clone
                .get_or_try_init(KEY, || async {
                    tx.send(()).unwrap();
                    sleep(Duration::from_millis(50)).await;
                    Ok::<_, Infallible>(1)
                })
                .await
                .unwrap()
        });

        // Wait for `get_or_try_init` above to start.
        rx.await.unwrap();

        let value_mutex_guard =
            map.get_or_try_init(KEY, || async { Ok::<_, Infallible>(2) }).await.unwrap();
        expect_that!(value_mutex_guard, derefs_to(eq(&1)));
        drop(value_mutex_guard);

        // Wait for `get_or_try_init` above to finish.
        let value = handle.await.unwrap();
        expect_that!(value, eq(1));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn concurrent_independent_keys() {
        let map = Arc::new(AsyncLazyMap::new());
        let map1 = Arc::clone(&map);
        let map2 = Arc::clone(&map);

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let t1 = tokio::spawn(async move {
            map1.get_or_try_init(1, || async {
                tx1.send(()).unwrap(); // Signal we started
                rx2.await.unwrap(); // Wait for T2 to signal
                Ok::<_, Infallible>(1)
            })
            .await
            .unwrap()
        });

        let t2 = tokio::spawn(async move {
            map2.get_or_try_init(2, || async {
                tx2.send(()).unwrap(); // Signal we started
                rx1.await.unwrap(); // Wait for T1 to signal
                Ok::<_, Infallible>(2)
            })
            .await
            .unwrap()
        });

        expect_that!(t1.await, ok(derefs_to(eq(&1))));
        expect_that!(t2.await, ok(derefs_to(eq(&2))));
    }
}
