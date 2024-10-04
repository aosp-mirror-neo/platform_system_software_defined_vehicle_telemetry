// Copyright 2024 Google LLC

use protobuf::MessageDyn;
use sdv_telemetry_time_wrapper::Microseconds;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
use sdv_telemetry_utils::atomic::{SequenceNumber, SequenceNumberType};

#[cfg(test)]
static NEXT_NODE_EVALUATION_CONTEXT_ID: SequenceNumber = SequenceNumber::new();

#[cfg_attr(test, derive(Debug))]
pub struct NodeEvaluationContext {
    #[cfg(test)]
    root_id: SequenceNumberType,

    // Map of messages keyed by publisher name.
    messages: HashMap<String, Arc<dyn MessageDyn>>,

    // By only querying the time once and putting it into the context, we ensure that it remains
    // constant while nodes are being evaluated, even if multiple nodes as part of the same
    // evaluation query the time.
    pub(super) unix_timestamp: Microseconds,
    pub(super) monotonic_time_since_boot: Microseconds,
}

impl NodeEvaluationContext {
    /// Creates a new instance of context with no messages.
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(test)]
            root_id: NEXT_NODE_EVALUATION_CONTEXT_ID.next(),
            messages: HashMap::new(),
            unix_timestamp: sdv_telemetry_time_wrapper::get_unix_timestamp(),
            monotonic_time_since_boot: sdv_telemetry_time_wrapper::get_monotonic_time_since_boot(),
        }
    }

    /// Creates a new instance of context with a publisher message.
    pub(crate) fn with_publisher_message(
        publisher_name: String,
        publisher_message: Arc<dyn MessageDyn>,
    ) -> Self {
        let mut res = Self::new();
        res.messages
            .try_insert_value(publisher_name, publisher_message)
            .expect("Hashmap should be empty");
        res
    }

    #[cfg(test)]
    pub(crate) fn root_id(&self) -> SequenceNumberType {
        self.root_id
    }

    pub(crate) fn get_message(&self, publisher_name: &str) -> Option<&Arc<dyn MessageDyn>> {
        self.messages.get(publisher_name)
    }

    /// Creates a child context with a copy of current context messages and a new message.
    pub(crate) fn to_child_context(
        &self,
        publisher_name: String,
        publisher_message: Arc<dyn MessageDyn>,
    ) -> Self {
        let mut messages = self.messages.clone();
        messages
            .try_insert_value(publisher_name, publisher_message)
            .expect("Message from each publisher should only be added once");
        Self {
            #[cfg(test)]
            root_id: self.root_id,
            messages,
            unix_timestamp: self.unix_timestamp,
            monotonic_time_since_boot: self.monotonic_time_since_boot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_evaluation_context_ids_are_ascending() {
        let context1 = NodeEvaluationContext::new();
        let context2 = NodeEvaluationContext::new();
        assert!(context1.root_id() < context2.root_id());
        assert!(context1.monotonic_time_since_boot <= context2.monotonic_time_since_boot);
    }
}
