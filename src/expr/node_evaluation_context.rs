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

use sdv_telemetry_time_wrapper::Microseconds;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(any(test, feature = "test_utils"))]
use sdv_telemetry_utils::atomic::{SequenceNumber, SequenceNumberType};

use crate::dynamic_message::DynamicMessage;

#[cfg(any(test, feature = "test_utils"))]
static NEXT_NODE_EVALUATION_CONTEXT_ID: SequenceNumber = SequenceNumber::new();

#[cfg_attr(any(test, feature = "test_utils"), derive(Debug))]
pub struct NodeEvaluationContext {
    #[cfg(any(test, feature = "test_utils"))]
    root_id: SequenceNumberType,

    // Map of messages keyed by publisher name.
    messages: HashMap<String, Arc<dyn DynamicMessage>>,

    // By only querying the time once and putting it into the context, we ensure that it remains
    // constant while nodes are being evaluated, even if multiple nodes as part of the same
    // evaluation query the time.
    pub unix_timestamp: Microseconds,
    pub monotonic_time_since_boot: Microseconds,
    pub monotonic_time_since_boot_or_resume: Microseconds,
}

impl NodeEvaluationContext {
    /// Creates a new instance of context with no messages.
    pub fn new(resumed_at: Option<Microseconds>) -> Self {
        let unix_timestamp = sdv_telemetry_time_wrapper::get_unix_timestamp();
        let monotonic_time_since_boot = sdv_telemetry_time_wrapper::get_monotonic_time_since_boot();

        Self {
            #[cfg(any(test, feature = "test_utils"))]
            root_id: NEXT_NODE_EVALUATION_CONTEXT_ID.next(),
            messages: HashMap::new(),
            unix_timestamp,
            monotonic_time_since_boot,
            monotonic_time_since_boot_or_resume: resumed_at
                .map_or(monotonic_time_since_boot, |resumed_at| {
                    Microseconds(monotonic_time_since_boot.0 - resumed_at.0)
                }),
        }
    }

    /// Creates a new instance of context with a publisher message.
    pub fn with_publisher_message(
        resumed_at: Option<Microseconds>,
        publisher_name: String,
        publisher_message: Arc<dyn DynamicMessage>,
    ) -> Self {
        let mut res = Self::new(resumed_at);
        res.messages
            .try_insert_value(publisher_name, publisher_message)
            .expect("Hashmap should be empty");
        res
    }

    #[cfg(any(test, feature = "test_utils"))]
    pub fn root_id(&self) -> SequenceNumberType {
        self.root_id
    }

    pub fn get_message(&self, publisher_name: &str) -> Option<&Arc<dyn DynamicMessage>> {
        self.messages.get(publisher_name)
    }

    /// Creates a child context with a copy of current context messages and a new message.
    pub fn to_child_context(
        &self,
        publisher_name: String,
        publisher_message: Arc<dyn DynamicMessage>,
    ) -> Self {
        let mut messages = self.messages.clone();
        messages
            .try_insert_value(publisher_name, publisher_message)
            .expect("Message from each publisher should only be added once");
        Self {
            #[cfg(any(test, feature = "test_utils"))]
            root_id: self.root_id,
            messages,
            unix_timestamp: self.unix_timestamp,
            monotonic_time_since_boot: self.monotonic_time_since_boot,
            monotonic_time_since_boot_or_resume: self.monotonic_time_since_boot_or_resume,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{eq, le, lt};

    #[googletest::test]
    fn ids_are_ascending() {
        let context1 = NodeEvaluationContext::new(None);
        let context2 = NodeEvaluationContext::new(None);

        expect_that!(context1.root_id(), lt(context2.root_id()));
    }

    #[googletest::test]
    fn monotonic_times_are_ascending() {
        let context1 = NodeEvaluationContext::new(None);
        let context2 = NodeEvaluationContext::new(None);

        expect_that!(context1.monotonic_time_since_boot, le(context2.monotonic_time_since_boot));
        expect_that!(
            context1.monotonic_time_since_boot_or_resume,
            le(context2.monotonic_time_since_boot_or_resume)
        );
    }

    #[googletest::test]
    fn monotonic_time_since_boot_or_resume_is_time_since_boot_when_not_provided() {
        let context = NodeEvaluationContext::new(None);

        expect_that!(
            context.monotonic_time_since_boot_or_resume,
            eq(context.monotonic_time_since_boot)
        );
    }

    #[googletest::test]
    fn monotonic_time_since_boot_or_resume_calculated_correctly() {
        // Pretend that the OS resumed from sleep 100ms after booting (we do not use a more
        // realistic, bigger number here, because the VM might only have booted shortly before this
        // test starts executing).
        const RESUMED_AT: Microseconds = Microseconds(1000 * 100);

        let context = NodeEvaluationContext::new(Some(RESUMED_AT));

        expect_that!(
            context.monotonic_time_since_boot_or_resume,
            eq(Microseconds(context.monotonic_time_since_boot.0 - RESUMED_AT.0))
        );
    }
}
