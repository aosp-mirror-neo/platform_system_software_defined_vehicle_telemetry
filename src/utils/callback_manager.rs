// Copyright 2023 Google LLC. All Rights Reserved.

//! This module contains the callback manager implementation.

use std::{collections::HashMap, fmt::Display, hash::Hash, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

use crate::collection_extension::HashMapExtension;

type CallbackFn = Box<dyn FnMut() + Send + Sync>;

/// A manager for callbacks.
pub struct CallbackManager<TKey: Display + Eq + Hash> {
    callback_map: Arc<AsyncMutex<HashMap<TKey, CallbackFn>>>,
}

impl<TKey: Display + Eq + Hash> CallbackManager<TKey> {
    /// Creates a new instance of the callback manager and returns a callback map
    /// that's in sync with the manager's internal map.
    pub fn new() -> (Self, Arc<AsyncMutex<HashMap<TKey, CallbackFn>>>) {
        let callback_map = Arc::new(AsyncMutex::new(HashMap::new()));
        (CallbackManager { callback_map: callback_map.clone() }, callback_map)
    }

    /// Add a new callback to the map.
    /// Panics if a callback with the specified key already exists.
    pub async fn add_callback(&self, key: TKey, callback: CallbackFn) {
        self.callback_map.lock().await.try_insert_value(key, callback).unwrap_or_else(|entry| {
            panic!("Key {} is already present in the callback map.", entry.key())
        });
    }

    /// Remove callback with the specified key from the map.
    /// Returns true if there are no more callbacks left.
    /// Panics if no callbacks with the specified key exist.
    pub async fn remove_callback(&self, key: &TKey) -> bool {
        let mut callbacks = self.callback_map.lock().await;
        assert!(callbacks.remove(key).is_some(), "Key {} isn't present in the callback map.", key);
        callbacks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "test_key";

    fn empty_fn() {}

    #[tokio::test]
    #[should_panic]
    async fn add_callback_duplicate_key_panics() {
        let (manager, callback_map) = CallbackManager::new();

        // Add a callback to the map.
        manager.add_callback(KEY.to_owned(), Box::new(empty_fn)).await;

        // Add a callback with the same key.
        manager.add_callback(KEY.to_owned(), Box::new(empty_fn)).await;
    }

    #[tokio::test]
    async fn add_callback_success() {
        let (manager, callback_map) = CallbackManager::new();

        // Add a callback to the map.
        manager.add_callback(KEY.to_owned(), Box::new(empty_fn)).await;

        // Verify that the callback was added to the map.
        assert!(callback_map.lock().await.contains_key(KEY));
    }

    #[tokio::test]
    #[should_panic]
    async fn remove_unknown_key_panics() {
        let (manager, _) = CallbackManager::new();

        // Try to remove a callback that doesn't exist.
        manager.remove_callback(KEY).await;
    }

    #[tokio::test]
    async fn remove_last_callback_success() {
        let (manager, callback_map) = CallbackManager::new();

        // Add a callback to the map.
        manager.add_callback(KEY.to_owned(), Box::new(empty_fn)).await;

        // Remove the callback from the map.
        let is_last = manager.remove_callback(KEY).await;

        // Verify that the callback was reported as the last.
        assert!(is_last);

        // Verify that the callback was removed from the map.
        assert!(!callback_map.lock().await.contains_key(KEY));
    }

    #[tokio::test]
    async fn remove_not_last_callback_success() {
        let (manager, callback_map) = CallbackManager::new();

        // Add two callbacks to the map.
        manager.add_callback(KEY.to_owned(), Box::new(empty_fn)).await;
        manager.add_callback(String::default(), Box::new(empty_fn)).await;

        // Remove the callback from the map.
        let is_last = manager.remove_callback(KEY).await;

        // Verify that the callback wasn't reported as the last.
        assert!(!is_last);

        // Verify that the callback was removed from the map.
        assert!(!callback_map.lock().await.contains_key(KEY));
    }
}
