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

use sdv_telemetry_expr::{
    dynamic_message::DynamicMessage, node_evaluation_context::NodeEvaluationContext,
};
use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use std::sync::Arc;

pub trait NodeEvaluationContextFactory: Send + Sync {
    fn create(&self) -> NodeEvaluationContext;

    fn create_with_publisher_message(
        &self,
        publisher_name: String,
        publisher_message: Arc<dyn DynamicMessage>,
    ) -> NodeEvaluationContext;
}

pub struct NodeEvaluationContextFactoryImpl {
    pal: Arc<dyn PlatformAbstractionLayer>,
}

impl NodeEvaluationContextFactory for NodeEvaluationContextFactoryImpl {
    fn create(&self) -> NodeEvaluationContext {
        NodeEvaluationContext::new(self.pal.get_resumed_at())
    }

    fn create_with_publisher_message(
        &self,
        publisher_name: String,
        publisher_message: Arc<dyn DynamicMessage>,
    ) -> NodeEvaluationContext {
        NodeEvaluationContext::with_publisher_message(
            self.pal.get_resumed_at(),
            publisher_name,
            publisher_message,
        )
    }
}

impl NodeEvaluationContextFactoryImpl {
    pub fn new(pal: Arc<dyn PlatformAbstractionLayer>) -> Self {
        Self { pal }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::eq;
    use lazy_static::lazy_static;
    use protobuf::well_known_types::empty::Empty;
    use sdv_telemetry_platform_abstraction_layer::MockPal;
    use sdv_telemetry_time_wrapper::Microseconds;

    use crate::metrics_config_orchestrator::protobuf_dynamic_message::ProtobufDynamicMessage;

    lazy_static! {
        static ref PUBLISHER_NAME: String = "name".to_owned();
        static ref PUBLISHER_MESSAGE: Arc<dyn DynamicMessage> =
            Arc::new(ProtobufDynamicMessage::new(Arc::new(Empty::new())));
    }

    #[googletest::test]
    fn create_time_since_boot_and_time_since_resume_are_equal() {
        let mut pal = MockPal::new();
        pal.expect_get_resumed_at().return_const(None);
        let factory = NodeEvaluationContextFactoryImpl::new(Arc::new(pal));

        let context1 = factory.create();
        let context2 = factory
            .create_with_publisher_message(PUBLISHER_NAME.clone(), Arc::clone(&PUBLISHER_MESSAGE));

        expect_that!(
            context1.monotonic_time_since_boot_or_resume,
            eq(context1.monotonic_time_since_boot)
        );
        expect_that!(
            context2.monotonic_time_since_boot_or_resume,
            eq(context2.monotonic_time_since_boot)
        );
    }

    #[googletest::test]
    fn create_time_since_resume_is_calculated_correctly() {
        // Pretend that the OS resumed from sleep 100ms after booting (we do not use a more
        // realistic, bigger number here, because the VM might only have booted shortly before this
        // test starts executing).
        const RESUMED_AT: Microseconds = Microseconds(1000 * 100);

        let mut pal = MockPal::new();
        pal.expect_get_resumed_at().return_const(Some(RESUMED_AT));
        let factory = NodeEvaluationContextFactoryImpl::new(Arc::new(pal));

        let context1 = factory.create();
        let context2 = factory
            .create_with_publisher_message(PUBLISHER_NAME.clone(), Arc::clone(&PUBLISHER_MESSAGE));

        expect_that!(
            context1.monotonic_time_since_boot_or_resume,
            eq(Microseconds(context1.monotonic_time_since_boot.0 - RESUMED_AT.0))
        );
        expect_that!(
            context2.monotonic_time_since_boot_or_resume,
            eq(Microseconds(context2.monotonic_time_since_boot.0 - RESUMED_AT.0))
        );
    }
}
