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

use sdv_telemetry_expr::node::Node;
use sdv_telemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::Aggregated_field_value as AggregatedFieldValueProto;
use std::sync::Arc;

use crate::metrics_config_orchestrator::message_builder::value_aggregator::mock::MockValueAggregator;
use crate::metrics_config_orchestrator::message_builder::value_aggregator_factory::{
    ValueAggregatorFactory, ValueAggregatorFactoryCreateContext, ValueAggregatorFactoryError,
    ValueAggregatorFactoryValidateContext,
};
use crate::metrics_config_orchestrator::message_builder::ValueAggregator;

pub(crate) struct MockValueAggregatorFactory {
    fail_create: bool,
    fail_validate: bool,
}

impl ValueAggregatorFactory for MockValueAggregatorFactory {
    fn create(
        &self,
        _aggregated_field_value: &AggregatedFieldValueProto,
        _context: &ValueAggregatorFactoryCreateContext,
    ) -> Result<(Box<dyn ValueAggregator>, Option<Arc<Node>>), ValueAggregatorFactoryError> {
        if self.fail_create {
            return Err(ValueAggregatorFactoryError::MissingExpressionNodeIndex);
        }
        Ok((Box::new(MockValueAggregator::new()), None))
    }

    fn validate(
        &self,
        _aggregated_field_value: &AggregatedFieldValueProto,
        _context: &ValueAggregatorFactoryValidateContext,
    ) -> Result<(), ValueAggregatorFactoryError> {
        if self.fail_validate {
            return Err(ValueAggregatorFactoryError::MissingExpressionNodeIndex);
        }
        Ok(())
    }
}

impl MockValueAggregatorFactory {
    pub(crate) fn new() -> Self {
        Self { fail_create: false, fail_validate: false }
    }

    pub(crate) fn fail_create(&mut self) {
        self.fail_create = true;
    }

    pub(crate) fn fail_validate(&mut self) {
        self.fail_validate = true;
    }
}
