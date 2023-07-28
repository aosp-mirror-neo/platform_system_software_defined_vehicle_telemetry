// Copyright 2023 Google LLC. All Rights Reserved.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

/// Additional HashMap functions that are either not available in the standard library, or hidden
/// behind experiment flags.
pub trait HashMapExtension<K: Eq + Hash, V> {
    /// Creates topologically sorted vector of keys using Kahn's algorithm based on dependency
    /// information stored in a HashMap.
    ///
    /// # Arguments
    ///
    /// * `dependency_selector` - A function that accepts HashMap value and returns keys that are
    ///   dependent on the corresponding entry.
    fn to_topological_sorted_vec<F: Fn(&V) -> &Vec<K>>(
        &self,
        dependency_selector: F,
    ) -> Option<Vec<&K>>;

    /// Tries to insert a key-value pair into the map, and returns Ok if insertion was successful.
    /// If the map already had this key present, nothing is updated, and Err is returned.
    fn try_insert_value(&mut self, key: K, value: V) -> Result<(), ()>;
}

/// Additional Vec functions that are either not available in the standard library, or hidden
/// behind experiment flags.
pub trait VecExtension<T> {
    /// Consumes vector and returns a HashMap with vector elements as values.
    ///
    /// # Arguments
    ///
    /// * `key_provider` - A function that accepts vector element and returns the key that will
    ///   correspond to the element in HashMap.
    fn into_hash_map<K: Eq + Hash, F: Fn(&T) -> K>(self, key_provider: F) -> HashMap<K, T>;
}

impl<K: Eq + Hash, V> HashMapExtension<K, V> for HashMap<K, V> {
    fn to_topological_sorted_vec<F: Fn(&V) -> &Vec<K>>(
        &self,
        dependency_selector: F,
    ) -> Option<Vec<&K>> {
        let mut in_degrees: HashMap<&K, usize> = HashMap::new();
        let mut sorted_order: Vec<&K> = Vec::new();
        let mut queue: VecDeque<&K> = VecDeque::new();

        // Initialize in-degrees for all dependencies.
        for value in self.values() {
            for dependency in dependency_selector(value) {
                *in_degrees.entry(dependency).or_insert(0) += 1;
            }
        }

        // Enqueue keys with no dependencies.
        for key in self.keys() {
            if !in_degrees.contains_key(key) {
                queue.push_back(key);
            }
        }

        // Process keys in the queue.
        while let Some(key) = queue.pop_front() {
            sorted_order.push(key);
            if let Some(value) = self.get(key) {
                for dependency in dependency_selector(value) {
                    let in_degree = in_degrees.get_mut(dependency).unwrap();
                    *in_degree -= 1;
                    if *in_degree == 0 {
                        queue.push_back(dependency);
                    }
                }
            }
        }

        // if every vertex appears in sorted list, sort is successful.
        if sorted_order.len() >= self.len() {
            Some(sorted_order)
        } else {
            None
        }
    }

    fn try_insert_value(&mut self, key: K, value: V) -> Result<(), ()> {
        match self.entry(key) {
            Entry::Occupied(_) => Err(()),
            Entry::Vacant(v) => {
                v.insert(value);
                Ok(())
            }
        }
    }
}

impl<T> VecExtension<T> for Vec<T> {
    fn into_hash_map<K: Eq + Hash, F: Fn(&T) -> K>(self, key_provider: F) -> HashMap<K, T> {
        self.into_iter().map(|val| (key_provider(&val), val)).collect()
    }
}
