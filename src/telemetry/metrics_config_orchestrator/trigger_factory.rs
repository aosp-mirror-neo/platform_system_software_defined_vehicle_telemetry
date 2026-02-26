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

use anyhow::{anyhow, Context};
use protobuf::{well_known_types::duration::Duration as DurationProto, Message};
use sdv_telemetry_expr::{
    node::Node, node_error::NodeEvaluationError, node_value_type::NodeValueType,
    node_value_type::OperatorProtoExtension,
};
use sdv_telemetry_proto_rust::expression::{
    combination_node::Operator as OperatorProto, combination_node::RelationalOperator,
};
use sdv_telemetry_proto_rust::metrics_configuration::{
    conditional_trigger::ConditionTypeAllChanges, conditional_trigger::ConditionTypeFallingEdge,
    conditional_trigger::ConditionTypeRisingEdge,
    conditional_trigger::Condition_type as ConditionTypeProto,
    conditional_trigger::EdgeOptions as EdgeOptionsProto,
    data_source::ConnectionType as ConnectionTypeProto, source::Source_type as SourceTypeProto,
    trigger::Trigger_type as TriggerTypeProto, ConditionalTrigger as ConditionalTriggerProto,
    PeriodicTrigger as PeriodicTriggerProto, Source as PublisherProto, Trigger as TriggerProto,
};
use sdv_telemetry_utils::duration_utils::try_into_duration;
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use super::trigger::conditional::{
    ConditionType, ConditionalTrigger, EdgeOptions, FirstValuePolicy,
};
use super::trigger::data::DataTrigger;
use super::trigger::periodic::PeriodicTrigger;
use crate::metrics_config_orchestrator::{
    node_evaluation_context_factory::NodeEvaluationContextFactory, Publisher, Trigger,
};
use crate::metrics_config_uuid::MetricsConfigUuid;
use crate::status_reporter::StatusReporter;

#[cfg_attr(test, sdv_telemetry_utils::with_super_trait(downcast_rs::Downcast))]
pub(crate) trait TriggerFactory: Send + Sync {
    /// Returns a new trigger encapsulated object based on the protobuf message.
    fn create(
        &self,
        metrics_config_uuid: MetricsConfigUuid,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryCreateContext,
    ) -> Result<Arc<dyn Trigger>, TriggerFactoryError>;

    /// Verifies that the protobuf message is valid and can be used to create a trigger encapsulated object.
    fn validate(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryValidateContext,
    ) -> Result<(), TriggerFactoryError>;
}

#[cfg(test)]
downcast_rs::impl_downcast!(TriggerFactory);

pub(crate) struct TriggerFactoryCreateContext<'a> {
    pub expression_nodes: &'a Vec<Arc<Node>>,
    pub node_types: &'a [Option<NodeValueType>],
    pub publishers: &'a HashMap<String, Arc<dyn Publisher>>,
    pub triggers: &'a HashMap<String, Arc<dyn Trigger>>,
}

pub(crate) struct TriggerFactoryValidateContext<'a> {
    pub node_types: &'a [Option<NodeValueType>],
    pub publishers: &'a HashMap<String, &'a PublisherProto>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum TriggerFactoryError {
    #[error("Incorrect conditional trigger node index {node_index}")]
    IncorrectNodeIndex { node_index: u32 },
    #[error(
        "Condition {condition_type:?} cannot be applied to an expression of type {value_type:?}"
    )]
    InvalidNodeValueType { condition_type: ConditionTypeProto, value_type: NodeValueType },
    #[error("Required field selector_node_index is not set for conditional trigger")]
    MissingNodeIndex,
    #[error("Trigger name not set")]
    NameNotSet,
    #[error("No parent triggers set for conditional trigger")]
    NoParentTriggers,
    #[error("Periodic trigger interval ({interval}) is invalid: {error:#}")]
    PeriodicTriggerInvalidDuration { interval: DurationProto, error: anyhow::Error },
    #[error("Source {source_name} has ON_DEMAND connection type and cannot be listened to")]
    UnableToListenToSourceOnDemand { source_name: String },
    #[error("Unknown condition type {condition_type:?}")]
    UnknownConditionTypeProto { condition_type: ConditionTypeProto },
    #[error("Unknown trigger type")]
    UnknownTriggerType,
    #[error("Condition type is not specified")]
    UnspecifiedConditionTypeProto,
    #[error("Trigger type is not specified")]
    UnspecifiedTriggerType,
    #[error("Edge options ({edge_options}) are invalid: {error:#}")]
    InvalidEdgeOptions { edge_options: EdgeOptionsProto, error: anyhow::Error },
    #[error("Condition type is invalid: {error:#}")]
    InvalidConditionTypeProto { error: anyhow::Error },
    #[error("initialize_node_index has type which is not compatible with type of selector_node_index: {error:#}")]
    IncompatibleNodeTypes { error: NodeEvaluationError },
}

pub(crate) struct TriggerFactoryImpl {
    conditional_trigger_new_func: Box<
        dyn Fn(
                String,
                ConditionType,
                MetricsConfigUuid,
                Arc<Node>,
                Vec<Arc<dyn Trigger>>,
                Arc<dyn NodeEvaluationContextFactory>,
                Arc<dyn StatusReporter>,
            ) -> Arc<dyn Trigger>
            + Send
            + Sync,
    >,
    data_trigger_new_func: Box<
        dyn Fn(String, MetricsConfigUuid, Arc<dyn Publisher>) -> Arc<dyn Trigger> + Send + Sync,
    >,
    node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    periodic_trigger_new_func: Box<
        dyn Fn(
                String,
                MetricsConfigUuid,
                Duration,
                Arc<dyn NodeEvaluationContextFactory>,
            ) -> Arc<dyn Trigger>
            + Send
            + Sync,
    >,
    status_reporter: Arc<dyn StatusReporter>,
}

impl TriggerFactory for TriggerFactoryImpl {
    fn create(
        &self,
        metrics_config_uuid: MetricsConfigUuid,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryCreateContext,
    ) -> Result<Arc<dyn Trigger>, TriggerFactoryError> {
        Self::verify_name_set(trigger_proto)?;
        let trigger_name = trigger_proto.name.clone();
        let trigger_type = trigger_proto
            .trigger_type
            .as_ref()
            .ok_or_else(|| TriggerFactoryError::UnspecifiedTriggerType)?;
        Ok(match trigger_type {
            TriggerTypeProto::DataTrigger(data_trigger) => {
                let publisher = &context.publishers[&data_trigger.source_name];

                if !publisher.is_subscribable() {
                    return Err(TriggerFactoryError::UnableToListenToSourceOnDemand {
                        source_name: publisher.get_name().to_owned(),
                    });
                }

                (self.data_trigger_new_func)(
                    trigger_name,
                    metrics_config_uuid,
                    Arc::<dyn Publisher>::clone(publisher),
                )
            }
            TriggerTypeProto::PeriodicTrigger(periodic_trigger) => {
                (self.periodic_trigger_new_func)(
                    trigger_name,
                    metrics_config_uuid,
                    Self::get_periodic_interval(periodic_trigger)?,
                    Arc::clone(&self.node_evaluation_context_factory),
                )
            }
            TriggerTypeProto::ConditionalTrigger(conditional_trigger) => {
                TriggerFactoryImpl::verify_has_parent_triggers(&conditional_trigger.trigger_names)?;

                let selector_node_index = Self::get_selector_node_index(conditional_trigger)?;
                let selector_expression =
                    Self::get_node(selector_node_index, context.expression_nodes)?;

                let condition_type = Self::get_condition_type(
                    conditional_trigger,
                    selector_node_index,
                    context.node_types,
                    Some(context.expression_nodes),
                )?;
                let triggers = conditional_trigger
                    .trigger_names
                    .iter()
                    .map(|name| Arc::clone(&context.triggers[name]) as Arc<dyn Trigger>)
                    .collect();
                (self.conditional_trigger_new_func)(
                    trigger_name,
                    condition_type,
                    metrics_config_uuid,
                    selector_expression,
                    triggers,
                    Arc::clone(&self.node_evaluation_context_factory),
                    Arc::clone(&self.status_reporter),
                )
            }
            _ => return Err(TriggerFactoryError::UnknownTriggerType),
        })
    }

    fn validate(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryValidateContext,
    ) -> Result<(), TriggerFactoryError> {
        Self::verify_name_set(trigger_proto)?;

        let trigger_type = trigger_proto
            .trigger_type
            .as_ref()
            .ok_or_else(|| TriggerFactoryError::UnspecifiedTriggerType)?;
        match trigger_type {
            TriggerTypeProto::DataTrigger(data_trigger) => {
                let publisher = &context.publishers[&data_trigger.source_name];

                if let Some(SourceTypeProto::DataSource(service_publisher)) = &publisher.source_type
                {
                    if let Ok(connection_type) = service_publisher.connection_type.enum_value() {
                        match connection_type {
                            ConnectionTypeProto::ON_DEMAND => {
                                return Err(TriggerFactoryError::UnableToListenToSourceOnDemand {
                                    source_name: publisher.name.clone(),
                                })
                            }
                            ConnectionTypeProto::SUBSCRIPTION => {
                                // Subscription Publishers are allowed.
                            }
                            ConnectionTypeProto::CONNECTION_TYPE_UNSPECIFIED => {
                                // Don't validate connection type here, because it's responsibility
                                // of PublisherFactory to do it.
                            }
                        }
                    }
                }
            }
            TriggerTypeProto::PeriodicTrigger(periodic_trigger) => {
                Self::get_periodic_interval(periodic_trigger)?;
            }
            TriggerTypeProto::ConditionalTrigger(conditional_trigger) => {
                TriggerFactoryImpl::verify_has_parent_triggers(&conditional_trigger.trigger_names)?;

                let selector_node_index = Self::get_selector_node_index(conditional_trigger)?;
                Self::get_condition_type(
                    conditional_trigger,
                    selector_node_index,
                    context.node_types,
                    None,
                )?;
            }
            _ => return Err(TriggerFactoryError::UnknownTriggerType),
        };
        Ok(())
    }
}

impl TriggerFactoryImpl {
    pub(super) fn new(
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
        status_reporter: Arc<dyn StatusReporter>,
    ) -> Self {
        Self {
            data_trigger_new_func: Box::new(|name, metrics_config_uuid, publisher| {
                DataTrigger::new(name, metrics_config_uuid, publisher)
            }),
            conditional_trigger_new_func: Box::new(
                |name,
                 condition_type,
                 metrics_config_uuid,
                 selector_expression,
                 triggers,
                 node_evaluation_context_factory,
                 status_reporter| {
                    ConditionalTrigger::new(
                        name,
                        condition_type,
                        metrics_config_uuid,
                        selector_expression,
                        triggers,
                        node_evaluation_context_factory,
                        status_reporter,
                    )
                },
            ),
            node_evaluation_context_factory,
            periodic_trigger_new_func: Box::new(
                |name, metrics_config_uuid, interval, node_evaluation_context_factory| {
                    PeriodicTrigger::new(
                        name,
                        metrics_config_uuid,
                        interval,
                        node_evaluation_context_factory,
                    )
                },
            ),
            status_reporter,
        }
    }

    fn get_edge_options(
        edge_options: &EdgeOptionsProto,
    ) -> Result<EdgeOptions, TriggerFactoryError> {
        Ok(EdgeOptions {
            min_duration: try_into_duration(&edge_options.min_duration)
                .with_context(|| {
                    format!("{}.min_duration must be positive", EdgeOptionsProto::NAME)
                })
                .map_err(|error| TriggerFactoryError::InvalidEdgeOptions {
                    edge_options: edge_options.clone(),
                    error,
                })?,
            require_exact: edge_options.require_exact,
        })
    }

    fn get_condition_type(
        conditional_trigger_proto: &ConditionalTriggerProto,
        selector_node_index: u32,
        node_value_types: &[Option<NodeValueType>],
        expression_nodes: Option<&[Arc<Node>]>,
    ) -> Result<ConditionType, TriggerFactoryError> {
        let proto_condition_type = conditional_trigger_proto
            .condition_type
            .as_ref()
            .ok_or_else(|| TriggerFactoryError::UnspecifiedConditionTypeProto)?;

        let value_type = node_value_types.get(selector_node_index as usize).ok_or_else(|| {
            TriggerFactoryError::IncorrectNodeIndex { node_index: selector_node_index }
        })?;

        let condition_type = match proto_condition_type {
            ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options,
                fire_initial,
                initialize_node_index,
                special_fields: _,
            }) => {
                let first_value_policy = Self::get_first_value_policy(
                    Some(rising_options),
                    None,
                    *fire_initial,
                    initialize_node_index.as_ref(),
                    selector_node_index,
                    node_value_types,
                    expression_nodes,
                )?;
                ConditionType::RisingEdge {
                    rising_options: Self::get_edge_options(rising_options)?,
                    first_value_policy,
                }
            }
            ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options,
                fire_initial,
                initialize_node_index,
                special_fields: _,
            }) => {
                let first_value_policy = Self::get_first_value_policy(
                    None,
                    Some(falling_options),
                    *fire_initial,
                    initialize_node_index.as_ref(),
                    selector_node_index,
                    node_value_types,
                    expression_nodes,
                )?;
                ConditionType::FallingEdge {
                    falling_options: Self::get_edge_options(falling_options)?,
                    first_value_policy,
                }
            }
            ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options,
                falling_options,
                fire_initial,
                initialize_node_index,
                special_fields: _,
            }) => {
                let first_value_policy = Self::get_first_value_policy(
                    Some(rising_options),
                    Some(falling_options),
                    *fire_initial,
                    initialize_node_index.as_ref(),
                    selector_node_index,
                    node_value_types,
                    expression_nodes,
                )?;
                if Self::are_edge_options_set(rising_options)
                    || Self::are_edge_options_set(falling_options)
                {
                    ConditionType::BothEdges {
                        rising_options: Self::get_edge_options(rising_options)?,
                        falling_options: Self::get_edge_options(falling_options)?,
                        first_value_policy,
                    }
                } else {
                    ConditionType::AllChanges { first_value_policy }
                }
            }
            ConditionTypeProto::IsTrue(_) => ConditionType::IsTrue,
            ConditionTypeProto::IsFalse(_) => ConditionType::IsFalse,
            unknown_condition_type => {
                return Err(TriggerFactoryError::UnknownConditionTypeProto {
                    condition_type: unknown_condition_type.clone(),
                })
            }
        };

        let is_value_type_allowed = |value_type: &NodeValueType| match condition_type {
            ConditionType::RisingEdge { .. }
            | ConditionType::FallingEdge { .. }
            | ConditionType::BothEdges { .. }
            | ConditionType::IsTrue
            | ConditionType::IsFalse => value_type.is_bool() || value_type.is_numeric(),
            ConditionType::AllChanges { .. } => true,
        };

        if let Some(value_type) = value_type {
            if !is_value_type_allowed(value_type) {
                return Err(TriggerFactoryError::InvalidNodeValueType {
                    condition_type: proto_condition_type.to_owned(),
                    value_type: value_type.to_owned(),
                });
            }
        }

        Ok(condition_type)
    }

    // Validates and builds FirstValuePolicy based on the parameters provided in condition type
    // proto. If the expression nodes are not provided, FirstValueType::CompareTo can't be
    // constructed, and then FirstValueType::Skip is returned instead.
    fn get_first_value_policy(
        rising_options: Option<&EdgeOptionsProto>,
        falling_options: Option<&EdgeOptionsProto>,
        fire_initial: bool,
        initialize_node_index: Option<&u32>,
        selector_node_index: u32,
        node_value_types: &[Option<NodeValueType>],
        expression_nodes: Option<&[Arc<Node>]>,
    ) -> Result<FirstValuePolicy, TriggerFactoryError> {
        match (fire_initial, initialize_node_index) {
            (true, Some(_)) => Err(TriggerFactoryError::InvalidConditionTypeProto {
                error: anyhow!("Either fire_initial=true or initialize_node_index can be set"),
            }),
            (true, None) => {
                // Validate that edge options are not set or set to default.
                if [rising_options, falling_options]
                    .into_iter()
                    .flatten()
                    .any(Self::are_edge_options_set)
                {
                    return Err(TriggerFactoryError::InvalidConditionTypeProto {
                        error: anyhow!(
                            "Either fire_initial=true or non-empty edge options can be set"
                        ),
                    });
                }
                Ok(FirstValuePolicy::FireTrigger)
            }
            (false, None) => Ok(FirstValuePolicy::Skip),
            (false, Some(index)) => {
                // Validate that node types of selector and initial expression are compatible with
                // the given condition type.
                let maybe_operator = match (rising_options, falling_options) {
                    // ConditionType::BothEdges
                    (Some(rising), Some(falling))
                        if Self::are_edge_options_set(rising)
                            || Self::are_edge_options_set(falling) =>
                    {
                        // Also GT and EQ (if edge options contain require_exact: true) are used,
                        // but just LT is enough for validation.
                        Some(OperatorProto::RelationalOperator(RelationalOperator::LT.into()))
                    }
                    // ConditionType::AllChanges
                    (Some(_), Some(_)) => {
                        Some(OperatorProto::RelationalOperator(RelationalOperator::NOT_EQ.into()))
                    }
                    // ConditionType::RisingEdge
                    (Some(_), None) => {
                        Some(OperatorProto::RelationalOperator(RelationalOperator::LT.into()))
                    }
                    // ConditionType::FallingEdge
                    (None, Some(_)) => {
                        Some(OperatorProto::RelationalOperator(RelationalOperator::GT.into()))
                    }
                    // this should never happen as callers should provide at least some of edge
                    // options (possibly, unset of default)
                    (None, None) => None,
                };

                if let Some(operator) = maybe_operator {
                    operator
                        .get_node_value_type(
                            node_value_types,
                            *index as usize,
                            selector_node_index.into(),
                        )
                        .map_err(|error| TriggerFactoryError::IncompatibleNodeTypes { error })?;
                }

                // Expression nodes do not exist when get_first_value_policy is called for
                // validation purposes. We then return any policy, as the actual enum variant is
                // irrelevant for validations.
                Ok(match expression_nodes {
                    None => FirstValuePolicy::Skip,
                    Some(nodes) => {
                        FirstValuePolicy::CompareTo { node: Self::get_node(*index, nodes)? }
                    }
                })
            }
        }
    }

    fn get_selector_node_index(
        conditional_trigger_proto: &ConditionalTriggerProto,
    ) -> Result<u32, TriggerFactoryError> {
        conditional_trigger_proto.selector_node_index.ok_or(TriggerFactoryError::MissingNodeIndex)
    }

    fn get_node(
        node_index: u32,
        expression_nodes: &[Arc<Node>],
    ) -> Result<Arc<Node>, TriggerFactoryError> {
        let expression_node = expression_nodes
            .get(node_index as usize)
            .ok_or_else(|| TriggerFactoryError::IncorrectNodeIndex { node_index })?;
        Ok(Arc::clone(expression_node))
    }

    fn get_periodic_interval(
        periodic_trigger_proto: &PeriodicTriggerProto,
    ) -> Result<Duration, TriggerFactoryError> {
        let interval = try_into_duration(&periodic_trigger_proto.interval).map_err(|err| {
            TriggerFactoryError::PeriodicTriggerInvalidDuration {
                interval: (*periodic_trigger_proto.interval).clone(),
                error: err.into(),
            }
        })?;
        if interval.is_zero() {
            return Err(TriggerFactoryError::PeriodicTriggerInvalidDuration {
                interval: (*periodic_trigger_proto.interval).clone(),
                error: anyhow!("Duration must not be zero."),
            });
        }
        Ok(interval)
    }

    fn verify_name_set(trigger_proto: &TriggerProto) -> Result<(), TriggerFactoryError> {
        if trigger_proto.name.is_empty() {
            Err(TriggerFactoryError::NameNotSet)
        } else {
            Ok(())
        }
    }

    fn verify_has_parent_triggers(names: &[String]) -> Result<(), TriggerFactoryError> {
        if names.is_empty() {
            Err(TriggerFactoryError::NoParentTriggers)
        } else {
            Ok(())
        }
    }

    fn are_edge_options_set(edge_options: &EdgeOptionsProto) -> bool {
        edge_options != EdgeOptionsProto::default_instance()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::matchers::{
        anything, contains_substring, eq, err, matches_pattern, ok, result_of_ref, some,
    };
    use googletest::{assert_that, expect_that};
    use lazy_static::lazy_static;
    use protobuf::MessageField;
    use sdv_telemetry_expr::node_value::NodeValue;
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_proto_rust::metrics_configuration::{
        conditional_trigger::Condition_type as ConditionTypeProto,
        DataSource as ServicePublisherProto,
    };
    use sdv_telemetry_service_publisher_accessor::SubSamplingInterval;
    use sdv_telemetry_test_utils::parameterized_test;
    use sdv_telemetry_utils::collection_extension::VecExtension;
    use sdv_telemetry_utils::countdown_latch::MultiLevelCountdownLatch;
    use sdv_telemetry_utils::duration_utils::try_into_proto_duration;
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::time::Duration;

    use crate::metrics_config_orchestrator::publisher::service::{
        ConnectionType, ServicePublisher,
    };
    use crate::metrics_config_orchestrator::{
        node_evaluation_context_factory::NodeEvaluationContextFactoryImpl,
        service_adapter::mock::MockServiceAdapter, trigger::mock::MockTrigger,
    };
    use crate::StatusReporterImpl;

    lazy_static! {
        static ref CONFIG_UUID: MetricsConfigUuid =
            "01bf0754-df95-46bb-bb35-522b6af66600".parse().unwrap();
    }

    const INTERVAL: Duration = Duration::from_secs(u32::MAX as u64);
    const NODE_INDEX: u32 = 42;
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service_name";
    const TRIGGER_NAME: &str = "trigger";

    struct DataTriggerData {
        publisher: Arc<dyn Publisher>,
    }

    struct ConditionalTriggerData {
        condition_type: ConditionType,
        selector_expression: Arc<Node>,
        triggers: Vec<Arc<dyn Trigger>>,
    }

    struct PeriodicTriggerData {
        interval: Duration,
    }

    fn create_factory() -> TriggerFactoryImpl {
        TriggerFactoryImpl::new(
            Arc::new(NodeEvaluationContextFactoryImpl::new(new_stub_pal())),
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
        )
    }

    fn new_trigger(name: &str) -> TriggerProto {
        TriggerProto { name: name.to_owned(), ..Default::default() }
    }

    fn create_node() -> Node {
        Node::ConstantLeaf { value: NodeValue::Bool(false) }
    }

    fn create_valid_data_trigger() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let data_trigger = trigger.mut_data_trigger();
        PUBLISHER_NAME.clone_into(&mut data_trigger.source_name);
        trigger
    }

    fn create_valid_periodic_trigger() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let periodic_trigger = trigger.mut_periodic_trigger();
        periodic_trigger.interval = MessageField::some(try_into_proto_duration(&INTERVAL).unwrap());
        trigger
    }

    fn create_conditional_trigger_with_no_parent_triggers() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        trigger.mut_conditional_trigger();
        trigger
    }

    fn create_conditional_trigger_with_missing_node_index() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        trigger.mut_conditional_trigger().trigger_names.push(String::default());
        trigger
    }

    fn create_valid_conditional_trigger(
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    ) -> (TriggerProto, Option<Vec<Arc<dyn Trigger>>>) {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.condition_type =
            Some(ConditionTypeProto::AllChanges(Default::default()));
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(PARENT_TRIGGER_NAME.to_owned());

        let parent_trigger = PeriodicTrigger::new(
            PARENT_TRIGGER_NAME.to_owned(),
            *CONFIG_UUID,
            INTERVAL,
            node_evaluation_context_factory,
        );

        (trigger, Some(vec![parent_trigger]))
    }

    fn create_conditional_trigger_with_incorrect_node_index() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.condition_type =
            Some(ConditionTypeProto::AllChanges(Default::default()));
        conditional_trigger.selector_node_index = Some(NODE_INDEX);
        conditional_trigger.trigger_names.push(String::default());
        trigger
    }

    fn create_conditional_trigger_with_unspecified_condition_type() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.condition_type = None;
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(String::default());
        trigger
    }

    impl TriggerFactoryImpl {
        fn capture_data_trigger(&mut self) -> Arc<OnceLock<DataTriggerData>> {
            let data_holder = Arc::new(OnceLock::new());
            let data_holder_clone = Arc::clone(&data_holder);
            self.data_trigger_new_func = Box::new(move |name, metrics_config_uuid, publisher| {
                assert!(data_holder_clone.set(DataTriggerData { publisher }).is_ok());
                Arc::new(MockTrigger::new(name, metrics_config_uuid))
            });
            data_holder
        }

        fn capture_conditional_trigger(&mut self) -> Arc<OnceLock<ConditionalTriggerData>> {
            let data_holder = Arc::new(OnceLock::new());
            let data_holder_clone = Arc::clone(&data_holder);
            self.conditional_trigger_new_func = Box::new(
                move |name,
                      condition_type,
                      metrics_config_uuid,
                      selector_expression,
                      triggers,
                      _node_evaluation_context_factory,
                      _status_reporter| {
                    assert!(data_holder_clone
                        .set(ConditionalTriggerData {
                            condition_type,
                            selector_expression,
                            triggers,
                        })
                        .is_ok());
                    Arc::new(MockTrigger::new(name, metrics_config_uuid))
                },
            );
            data_holder
        }

        fn capture_periodic_trigger(&mut self) -> Arc<OnceLock<PeriodicTriggerData>> {
            let data_holder = Arc::new(OnceLock::new());
            let data_holder_clone = Arc::clone(&data_holder);
            self.periodic_trigger_new_func = Box::new(
                move |name, metrics_config_uuid, interval, _node_evaluation_context_factory| {
                    assert!(data_holder_clone.set(PeriodicTriggerData { interval }).is_ok());
                    Arc::new(MockTrigger::new(name, metrics_config_uuid))
                },
            );
            data_holder
        }

        // The following functions create a context with passed data and use it to call `create`
        // or `validate`. This is needed because context structs contain borrows and it's impossible
        // to have a function that creates and returns it - it can only be used in the function
        // where it's created.
        fn create_using_default_context(
            &self,
            trigger_proto: &TriggerProto,
        ) -> Result<Arc<dyn Trigger>, TriggerFactoryError> {
            self.create_using_context_with(trigger_proto, None, vec![], None, None)
        }

        fn create_using_context_with(
            &self,
            trigger_proto: &TriggerProto,
            nodes: Option<Vec<Arc<Node>>>,
            node_types: Vec<Option<NodeValueType>>,
            publishers: Option<Vec<Arc<dyn Publisher>>>,
            triggers: Option<Vec<Arc<dyn Trigger>>>,
        ) -> Result<Arc<dyn Trigger>, TriggerFactoryError> {
            let expression_nodes = nodes.unwrap_or_default();
            let publishers = publishers.map_or(HashMap::new(), |publishers| {
                publishers.into_hash_map(|publisher| publisher.get_name().to_owned())
            });
            let triggers = triggers.map_or(HashMap::new(), |triggers| {
                triggers.into_hash_map(|trigger| trigger.get_name().to_owned())
            });
            let context = TriggerFactoryCreateContext {
                expression_nodes: &expression_nodes,
                node_types: &node_types,
                publishers: &publishers,
                triggers: &triggers,
            };
            self.create(*CONFIG_UUID, trigger_proto, &context)
        }

        fn validate_using_default_context(
            &self,
            trigger_proto: &TriggerProto,
        ) -> Result<(), TriggerFactoryError> {
            self.validate_using_context_with(trigger_proto, vec![], None)
        }

        fn validate_using_context_with(
            &self,
            trigger_proto: &TriggerProto,
            node_types: Vec<Option<NodeValueType>>,
            publishers: Option<Vec<PublisherProto>>,
        ) -> Result<(), TriggerFactoryError> {
            let publishers = if let Some(publishers) = &publishers {
                publishers.iter().map(|publisher| (publisher.name.clone(), publisher)).collect()
            } else {
                HashMap::new()
            };
            let context =
                TriggerFactoryValidateContext { node_types: &node_types, publishers: &publishers };
            self.validate(trigger_proto, &context)
        }
    }

    #[test]
    fn create_no_name_fails() {
        let factory = create_factory();

        let err = factory.create_using_default_context(&TriggerProto::default()).err().unwrap();

        assert!(matches!(&err, TriggerFactoryError::NameNotSet), "Unexpected error: {err:?}");
    }

    #[test]
    fn create_no_trigger_type_fails() {
        let factory = create_factory();

        // TODO(b/339033786): We cannot use `unwrap_err` here.
        let err = factory.create_using_default_context(&new_trigger(TRIGGER_NAME)).err().unwrap();

        assert!(
            matches!(&err, TriggerFactoryError::UnspecifiedTriggerType),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn create_data_trigger_getter_publisher_fails() {
        let factory = create_factory();
        let trigger = create_valid_data_trigger();
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            None,
            ConnectionType::Getter,
            false,
            *CONFIG_UUID,
            SERVICE_NAME.to_owned(),
            SubSamplingInterval::ZERO,
            Arc::clone(&factory.node_evaluation_context_factory),
            Arc::new(MockServiceAdapter::new()),
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
            MultiLevelCountdownLatch::builder().build(),
        ));

        let err = factory
            .create_using_context_with(
                &trigger,
                None,
                vec![],
                Some(vec![Arc::clone(&publisher) as Arc<dyn Publisher>]),
                None,
            )
            .err()
            .unwrap();

        assert!(
            matches!(&err, TriggerFactoryError::UnableToListenToSourceOnDemand { source_name } if source_name == PUBLISHER_NAME),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn create_data_trigger_success() {
        let mut factory = create_factory();
        let data_holder = factory.capture_data_trigger();
        let trigger = create_valid_data_trigger();
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            None,
            ConnectionType::Subscription,
            false,
            *CONFIG_UUID,
            SERVICE_NAME.to_owned(),
            SubSamplingInterval::ZERO,
            Arc::clone(&factory.node_evaluation_context_factory),
            Arc::new(MockServiceAdapter::new()),
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
            MultiLevelCountdownLatch::builder().build(),
        ));

        let res = factory
            .create_using_context_with(
                &trigger,
                None,
                vec![],
                Some(vec![Arc::clone(&publisher) as Arc<dyn Publisher>]),
                None,
            )
            .unwrap();

        assert_eq!(res.get_name(), TRIGGER_NAME);
        assert_eq!(res.get_metrics_config_uuid(), &*CONFIG_UUID);
        let data_holder_value = data_holder.get().unwrap();
        assert!(Arc::ptr_eq(&data_holder_value.publisher, &(publisher as Arc<dyn Publisher>)));
    }

    #[test]
    fn create_periodic_trigger_success() {
        let mut factory = create_factory();
        let data_holder = factory.capture_periodic_trigger();
        let trigger = create_valid_periodic_trigger();

        let res = factory.create_using_default_context(&trigger).unwrap();

        assert_eq!(res.get_name(), TRIGGER_NAME);
        assert_eq!(res.get_metrics_config_uuid(), &*CONFIG_UUID);
        let data_holder_value = data_holder.get().unwrap();
        assert_eq!(data_holder_value.interval, INTERVAL);
    }

    #[test]
    fn create_periodic_trigger_zero_interval_failure() {
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let periodic_trigger = trigger.mut_periodic_trigger();
        periodic_trigger.interval =
            MessageField::some(DurationProto { seconds: 0, nanos: 0, ..Default::default() });

        let err = factory.create_using_default_context(&trigger).err().unwrap();
        assert!(
            matches!(
                err,
                TriggerFactoryError::PeriodicTriggerInvalidDuration {
                    interval: DurationProto { seconds: 0, nanos: 0, .. },
                    ..
                }
            ),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn create_periodic_trigger_negative_interval_failure() {
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let periodic_trigger = trigger.mut_periodic_trigger();
        periodic_trigger.interval =
            MessageField::some(DurationProto { seconds: -1, nanos: 0, ..Default::default() });

        let err = factory.create_using_default_context(&trigger).err().unwrap();
        assert!(
            matches!(
                err,
                TriggerFactoryError::PeriodicTriggerInvalidDuration {
                    interval: DurationProto { seconds: -1, nanos: 0, .. },
                    ..
                }
            ),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn create_conditional_trigger_no_parent_triggers_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_no_parent_triggers();

        let err = factory.create_using_default_context(&trigger).err().unwrap();

        assert!(matches!(err, TriggerFactoryError::NoParentTriggers), "Unexpected error: {err:?}");
    }

    #[test]
    fn create_conditional_trigger_missing_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_missing_node_index();

        let err = factory.create_using_default_context(&trigger).err().unwrap();

        assert!(matches!(err, TriggerFactoryError::MissingNodeIndex), "Unexpected error: {err:?}");
    }

    #[test]
    fn create_conditional_trigger_incorrect_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_incorrect_node_index();

        let err = factory.create_using_default_context(&trigger).err().unwrap();

        assert!(
            matches!(err, TriggerFactoryError::IncorrectNodeIndex { node_index: NODE_INDEX }),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn create_conditional_trigger_unspecified_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_unspecified_condition_type();

        let err = factory
            .create_using_context_with(
                &trigger,
                Some(vec![Arc::new(create_node())]),
                vec![None],
                None,
                None,
            )
            .err()
            .unwrap();

        assert!(
            matches!(err, TriggerFactoryError::UnspecifiedConditionTypeProto),
            "Unexpected error: {err:?}"
        );
    }

    parameterized_test!(
        #[googletest::test]
        conditional_trigger_incompatible_condition_type_fails,

        |condition_type: ConditionTypeProto, node_value_type: NodeValueType| {
            let factory = create_factory();
            let mut trigger = create_conditional_trigger_with_unspecified_condition_type();
            trigger.mut_conditional_trigger().condition_type = Some(condition_type);

            let node_types = vec![Some(node_value_type)];

            let validate_result = factory
                .validate_using_context_with(&trigger, node_types.clone(), None);
            let create_err = factory
                .create_using_context_with(
                    &trigger,
                    Some(vec![Arc::new(create_node())]),
                    node_types,
                    None,
                    None,
                )
                .err();

            expect_that!(validate_result, err(matches_pattern!(TriggerFactoryError::InvalidNodeValueType { .. })));
            expect_that!(create_err, some(matches_pattern!(TriggerFactoryError::InvalidNodeValueType { .. })));
        },

        rising_edge_bool_list: (
            ConditionTypeProto::RisingEdge(Default::default()),
            NodeValueType::BoolList,
        ),
        rising_edge_options_bool_list: (
            ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                ..Default::default()
            }),
            NodeValueType::BoolList,
        ),

        falling_edge_bool_list: (
            ConditionTypeProto::FallingEdge(Default::default()),
            NodeValueType::BoolList,
        ),
        falling_edge_options_bool_list: (
            ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                ..Default::default()
            }),
            NodeValueType::BoolList,
        ),

        all_changes_edge_options_bool_list: (
            ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                ..Default::default()
            }),
            NodeValueType::BoolList,
        ),

        is_true_bool_list: (
            ConditionTypeProto::IsTrue(Default::default()),
            NodeValueType::BoolList,
        ),
        is_false_bool_list: (
            ConditionTypeProto::IsTrue(Default::default()),
            NodeValueType::BoolList,
        ),
    );

    #[test]
    fn create_conditional_trigger_success() {
        let mut factory = create_factory();
        let data_holder = factory.capture_conditional_trigger();
        let (trigger, parent_triggers) =
            create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));

        let node_0 = Arc::new(create_node());
        let node_1 = Arc::new(create_node());
        let res = factory
            .create_using_context_with(
                &trigger,
                Some(vec![Arc::clone(&node_0), Arc::clone(&node_1)]),
                vec![Some(NodeValueType::Bool), Some(NodeValueType::Bool)],
                None,
                parent_triggers.clone(),
            )
            .unwrap();

        assert_eq!(res.get_name(), TRIGGER_NAME);
        assert_eq!(res.get_metrics_config_uuid(), &*CONFIG_UUID);
        let data_holder_value = data_holder.get().unwrap();
        assert_eq!(
            data_holder_value.condition_type,
            ConditionType::AllChanges { first_value_policy: FirstValuePolicy::Skip }
        );
        // selector_expression should match the node at index 0
        assert!(Arc::ptr_eq(&data_holder_value.selector_expression, &node_0));
        assert!(!Arc::ptr_eq(&data_holder_value.selector_expression, &node_1));
        assert_eq!(data_holder_value.triggers.len(), 1);
        assert!(Arc::ptr_eq(
            &data_holder_value.triggers[0],
            &(Arc::clone(&parent_triggers.unwrap()[0]) as Arc<dyn Trigger>)
        ));
    }

    #[test]
    fn create_conditional_trigger_unknown_node_type_success() {
        let factory = create_factory();
        let (trigger, parent_triggers) =
            create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));

        let node = Arc::new(create_node());
        factory
            .create_using_context_with(
                &trigger,
                Some(vec![node]),
                vec![None],
                None,
                parent_triggers,
            )
            .unwrap();
    }

    parameterized_test!(
        #[test]
        create_conditional_trigger_with_condition_type,

        |condition_type, matcher| {
            let mut factory = create_factory();
            let data_holder = factory.capture_conditional_trigger();
            let node = Arc::new(create_node());

            let (mut trigger, parent_triggers) = create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));
            trigger.mut_conditional_trigger().condition_type = condition_type;

            factory.create_using_context_with(&trigger, Some(vec![node]), vec![None], None, parent_triggers).unwrap();

            let data_holder_value = data_holder.get().unwrap();
            assert_that!(data_holder_value.condition_type, matcher);
        },

        all_changes_default: (
            Some(ConditionTypeProto::AllChanges(Default::default())),
            eq(&ConditionType::AllChanges { first_value_policy: FirstValuePolicy::Skip }),
        ),
        rising_default: (
            Some(ConditionTypeProto::RisingEdge(Default::default())),
            eq(&ConditionType::RisingEdge { rising_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::Skip }),
        ),
        falling_default: (
            Some(ConditionTypeProto::FallingEdge(Default::default())),
            eq(&ConditionType::FallingEdge { falling_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::Skip }),
        ),
        is_true_default: (
            Some(ConditionTypeProto::IsTrue(Default::default())),
            eq(&ConditionType::IsTrue),
        ),
        is_false_default: (
            Some(ConditionTypeProto::IsFalse(Default::default())),
            eq(&ConditionType::IsFalse),
        ),

        all_changes_with_default_options: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto::default()).into(),
                falling_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default()
            })),
            eq(&ConditionType::AllChanges { first_value_policy: FirstValuePolicy::Skip }),
        ),
        rising_with_default_options: (
            Some(ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default()
            })),
            eq(&ConditionType::RisingEdge { rising_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::Skip }),
        ),
        falling_with_default_options: (
            Some(ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default()
            })),
            eq(&ConditionType::FallingEdge { falling_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::Skip }),
        ),

        all_changes_with_rising_options: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    require_exact: true,
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
            eq(&ConditionType::BothEdges {
                rising_options: EdgeOptions { min_duration: Duration::from_secs(3), require_exact: true },
                falling_options: EdgeOptions::default(),
                first_value_policy: FirstValuePolicy::Skip,
            }),
        ),
        all_changes_with_falling_options: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    require_exact: false,
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
            eq(&ConditionType::BothEdges {
                rising_options: EdgeOptions::default(),
                falling_options: EdgeOptions { min_duration: Duration::from_secs(3), require_exact: false },
                first_value_policy: FirstValuePolicy::Skip,
            }),
        ),
        all_changes_with_both_options: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 1, ..Default::default() }).into(),
                    require_exact: false,
                    ..Default::default()
                }).into(),
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    require_exact: true,
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
            eq(&ConditionType::BothEdges {
                rising_options: EdgeOptions { min_duration: Duration::from_secs(1), require_exact: false },
                falling_options: EdgeOptions { min_duration: Duration::from_secs(3), require_exact: true },
                first_value_policy: FirstValuePolicy::Skip,
            }),
        ),
        rising_with_options: (
            Some(ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 1, ..Default::default() }).into(),
                    require_exact: false,
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
            eq(&ConditionType::RisingEdge { rising_options: EdgeOptions { min_duration: Duration::from_secs(1), require_exact: false }, first_value_policy: FirstValuePolicy::Skip }),
        ),
        falling_with_options: (
            Some(ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 7, ..Default::default() }).into(),
                    require_exact: true,
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
            eq(&ConditionType::FallingEdge { falling_options: EdgeOptions { min_duration: Duration::from_secs(7), require_exact: true }, first_value_policy: FirstValuePolicy::Skip }),
        ),
        all_changes_fire_initial: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges { fire_initial: true, ..Default::default() })),
            eq(&ConditionType::AllChanges { first_value_policy: FirstValuePolicy::FireTrigger }),
        ),
        rising_edge_fire_initial: (
            Some(ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge { fire_initial: true, ..Default::default() })),
            eq(&ConditionType::RisingEdge { rising_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::FireTrigger }),
        ),
        falling_edge_fire_initial: (
            Some(ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge { fire_initial: true, ..Default::default() })),
            eq(&ConditionType::FallingEdge { falling_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::FireTrigger }),
        ),
        all_changes_with_default_options_fire_initial: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                fire_initial: true,
                rising_options: Some(EdgeOptionsProto::default()).into(),
                falling_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default() })),
            eq(&ConditionType::AllChanges { first_value_policy: FirstValuePolicy::FireTrigger }),
        ),
        rising_edge_with_default_options_fire_initial: (
            Some(ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                fire_initial: true,
                rising_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default() })),
            eq(&ConditionType::RisingEdge { rising_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::FireTrigger }),
        ),
        falling_edge_with_default_options_fire_initial: (
            Some(ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                fire_initial: true,
                falling_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default()
            })),
            eq(&ConditionType::FallingEdge { falling_options: EdgeOptions::default(), first_value_policy: FirstValuePolicy::FireTrigger }),
        ),
    );

    parameterized_test!(
        #[test]
        create_conditional_trigger_with_initialize_node_index_success,

        |condition_builder: fn(u32) -> ConditionTypeProto,
         node_types: Vec<Option<NodeValueType>>,
         matcher| {
            let mut factory = create_factory();
            let data_holder = factory.capture_conditional_trigger();

            let selector_expression = Arc::new(create_node());
            let initial_expression = Arc::new(create_node());
            let nodes = vec![selector_expression, Arc::clone(&initial_expression)];

            let (mut trigger, parent_triggers) = create_valid_conditional_trigger(
                Arc::clone(&factory.node_evaluation_context_factory)
            );

            // `create_valid_conditional_trigger` has already set `selector_node_index` to 0th node.
            // Now set `initialize_node_index` to 1st node.
            trigger.mut_conditional_trigger().condition_type = Some(condition_builder(1));

            let _ = factory.create_using_context_with(
                &trigger, Some(nodes), node_types, None, parent_triggers
            ).unwrap();

            let data = data_holder.get().unwrap();
            assert_that!(data.condition_type, matcher);

            // Verify that initial expression inside the enum points to the correct node (index 1)
            let result_expression = match &data.condition_type {
                ConditionType::RisingEdge { first_value_policy: FirstValuePolicy::CompareTo { node: expr }, .. } |
                ConditionType::FallingEdge { first_value_policy: FirstValuePolicy::CompareTo { node: expr }, .. } |
                ConditionType::AllChanges { first_value_policy: FirstValuePolicy::CompareTo { node: expr }, .. } |
                ConditionType::BothEdges { first_value_policy: FirstValuePolicy::CompareTo { node: expr }, .. } => expr,
                _ => panic!("initial expression should be set"),
            };

            assert!(Arc::ptr_eq(result_expression, &initial_expression));
        },

        rising_edge: (
            |idx| ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Int), Some(NodeValueType::Int)],
            matches_pattern!(ConditionType::RisingEdge {
                rising_options: eq(&EdgeOptions::default()),
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        rising_edge_default_options: (
            |idx| ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto::default()).into(),
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Float), Some(NodeValueType::Int)],
            matches_pattern!(ConditionType::RisingEdge {
                rising_options: eq(&EdgeOptions::default()),
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        rising_edge_with_options: (
            |idx| ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Bool), Some(NodeValueType::Bool)],
            matches_pattern!(ConditionType::RisingEdge {
                rising_options: eq(&EdgeOptions { min_duration: Duration::from_secs(3), require_exact: false }),
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        falling_edge: (
            |idx| ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Float), Some(NodeValueType::Float)],
            matches_pattern!(ConditionType::FallingEdge {
                falling_options: eq(&EdgeOptions::default()),
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        falling_edge_default_options: (
            |idx| ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto::default()).into(),
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Float), Some(NodeValueType::Float)],
            matches_pattern!(ConditionType::FallingEdge {
                falling_options: eq(&EdgeOptions::default()),
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        falling_edge_with_options: (
            |idx| ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 1, ..Default::default() }).into(),
                    require_exact: true,
                    ..Default::default()
                }).into(),
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Int), Some(NodeValueType::Int)],
            matches_pattern!(ConditionType::FallingEdge {
                falling_options: eq(&EdgeOptions { min_duration: Duration::from_secs(1), require_exact: true }),
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        all_changes: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Bool), Some(NodeValueType::Bool)],
            matches_pattern!(ConditionType::AllChanges {
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        all_changes_default_options: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto::default()).into(),
                falling_options: Some(EdgeOptionsProto::default()).into(),
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Bool), Some(NodeValueType::Bool)],
            matches_pattern!(ConditionType::AllChanges {
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        all_changes_with_options: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 2, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Int), Some(NodeValueType::Int)],
            matches_pattern!(ConditionType::BothEdges {
                rising_options: eq(&EdgeOptions { min_duration: Duration::from_secs(2), require_exact: false }),
                falling_options: eq(&EdgeOptions { min_duration: Duration::from_secs(3), require_exact: false }),
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
        all_changes_no_options_any_types: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            // Note that if non-default options were specified, the validation would fail with these
            // node value types.
            vec![Some(NodeValueType::MessageList { descriptor: None }), Some(NodeValueType::Bool)],
            matches_pattern!(ConditionType::AllChanges {
                // node is to be verified in the test itself
                first_value_policy: matches_pattern!(FirstValuePolicy::CompareTo { node: anything() }),
            })
        ),
    );

    parameterized_test!(
        #[test]
        create_conditional_trigger_with_initialize_node_index_fail,

        |condition_builder: fn(u32) -> ConditionTypeProto,
         node_types: Vec<Option<NodeValueType>>,
         error_matcher| {
            let factory = create_factory();

            // Create two generic nodes. The factory relies on the passed `node_types`
            // context for type validation, so the actual node content is secondary here,
            // but we provide them to satisfy the index lookups.
            let nodes = vec![Arc::new(create_node()), Arc::new(create_node())];

            let (mut trigger, parent_triggers) = create_valid_conditional_trigger(
                Arc::clone(&factory.node_evaluation_context_factory)
            );

            // `create_valid_conditional_trigger` has already set `selector_node_index` to 0th node.
            // Now set `initialize_node_index` to 1st node.
            trigger.mut_conditional_trigger().condition_type = Some(condition_builder(1));

            let result = factory.create_using_context_with(
                &trigger, Some(nodes), node_types, None, parent_triggers
            ).err().unwrap();

            assert_that!(result, error_matcher);
        },

        rising_edge_with_fire_initial: (
            |idx| ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Int), Some(NodeValueType::Int)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        rising_edge_default_options_with_fire_initial: (
            |idx| ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto::default()).into(),
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Float), Some(NodeValueType::Int)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        rising_edge_with_options_with_fire_initial: (
            |idx| ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Int), Some(NodeValueType::Int)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        falling_edge_with_fire_initial: (
            |idx| ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Float), Some(NodeValueType::Float)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        falling_edge_default_options_with_fire_initial: (
            |idx| ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto::default()).into(),
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Float), Some(NodeValueType::Float)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        falling_edge_with_options_with_fire_initial: (
            |idx| ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 1, ..Default::default() }).into(),
                    require_exact: true,
                    ..Default::default()
                }).into(),
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Int), Some(NodeValueType::Int)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        all_changes_with_fire_initial: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Bool), Some(NodeValueType::Bool)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        all_changes_default_options_with_fire_initial: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto::default()).into(),
                falling_options: Some(EdgeOptionsProto::default()).into(),
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Bool), Some(NodeValueType::Bool)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        all_changes_with_options_with_fire_initial: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 2, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                fire_initial: true,
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Int), Some(NodeValueType::Int)],
            matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }),
        ),
        rising_edge_incompatible_types: (
            |idx| ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Bool), Some(NodeValueType::Int)],
            matches_pattern!(TriggerFactoryError::IncompatibleNodeTypes { .. }),
        ),
        falling_edge_incompatible_types: (
            |idx| ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::Message { descriptor: None }), Some(NodeValueType::Float)],
            matches_pattern!(TriggerFactoryError::IncompatibleNodeTypes { .. }),
        ),
        all_changes_with_options_incompatible_types: (
            |idx| ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 1, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 1, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                initialize_node_index: Some(idx),
                ..Default::default()
            }),
            vec![Some(NodeValueType::MessageList { descriptor: None }), Some(NodeValueType::Bool)],
            matches_pattern!(TriggerFactoryError::IncompatibleNodeTypes { .. }),
        ),
    );
    #[test]
    fn get_edge_options_succeeds() {
        let edge_options = EdgeOptionsProto {
            min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
            require_exact: true,
            ..Default::default()
        };

        let result = TriggerFactoryImpl::get_edge_options(&edge_options);

        assert_that!(
            result,
            ok(eq(&EdgeOptions { min_duration: Duration::from_secs(3), require_exact: true }))
        );
    }

    #[test]
    fn get_edge_options_with_negative_duration_fails() {
        let edge_options = EdgeOptionsProto {
            min_duration: Some(DurationProto { seconds: -1, ..Default::default() }).into(),
            ..Default::default()
        };

        let result = TriggerFactoryImpl::get_edge_options(&edge_options);

        assert_that!(
            result,
            err(matches_pattern!(TriggerFactoryError::InvalidEdgeOptions {
                edge_options: &edge_options,
                error: result_of_ref!(
                    |error| format!("{error:#}"),
                    contains_substring("EdgeOptions.min_duration must be positive")
                )
            }))
        );
    }

    #[test]
    fn default_proto_edge_options_are_equal_to_rust_default_edge_options() {
        let result = TriggerFactoryImpl::get_edge_options(EdgeOptionsProto::default_instance());

        assert_that!(result, ok(eq(&EdgeOptions::default())));
    }

    #[test]
    fn validate_no_name_fails() {
        let factory = create_factory();

        let err = factory.validate_using_default_context(&TriggerProto::default()).unwrap_err();

        assert!(matches!(&err, TriggerFactoryError::NameNotSet), "Unexpected error: {err:?}");
    }

    #[test]
    fn validate_no_trigger_type_fails() {
        let factory = create_factory();

        let result = factory.validate_using_default_context(&new_trigger(TRIGGER_NAME));

        assert!(
            matches!(&result, Err(TriggerFactoryError::UnspecifiedTriggerType)),
            "Unexpected result: {result:?}"
        );
    }

    #[test]
    fn validate_data_trigger_getter_publisher_fails() {
        let factory = create_factory();
        let trigger = create_valid_data_trigger();
        let publisher = PublisherProto {
            name: PUBLISHER_NAME.to_string(),
            source_type: Some(SourceTypeProto::DataSource(ServicePublisherProto {
                connection_type: ConnectionTypeProto::ON_DEMAND.into(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let err = factory
            .validate_using_context_with(&trigger, vec![], Some(vec![publisher]))
            .err()
            .unwrap();

        assert!(
            matches!(&err, TriggerFactoryError::UnableToListenToSourceOnDemand { source_name } if source_name == PUBLISHER_NAME),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn validate_data_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_data_trigger();
        let publisher = PublisherProto {
            name: PUBLISHER_NAME.to_string(),
            source_type: Some(SourceTypeProto::DataSource(ServicePublisherProto {
                connection_type: ConnectionTypeProto::SUBSCRIPTION.into(),
                ..Default::default()
            })),
            ..Default::default()
        };

        factory.validate_using_context_with(&trigger, vec![], Some(vec![publisher])).unwrap();
    }

    #[test]
    fn validate_periodic_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_periodic_trigger();

        factory.validate_using_default_context(&trigger).unwrap();
    }

    #[test]
    fn validate_periodic_trigger_zero_interval_fails() {
        let factory = create_factory();
        let mut trigger = create_valid_periodic_trigger();
        trigger.mut_periodic_trigger().interval =
            MessageField::some(DurationProto { seconds: 0, nanos: 0, ..Default::default() });

        let result = factory.validate_using_default_context(&trigger);

        assert!(
            matches!(&result, Err(TriggerFactoryError::PeriodicTriggerInvalidDuration { .. })),
            "Unexpected result: {result:?}"
        );
    }

    #[test]
    fn validate_periodic_trigger_negative_interval_fails() {
        let factory = create_factory();
        let mut trigger = create_valid_periodic_trigger();
        trigger.mut_periodic_trigger().interval =
            MessageField::some(DurationProto { seconds: -1, nanos: 0, ..Default::default() });

        let result = factory.validate_using_default_context(&trigger);

        assert!(
            matches!(&result, Err(TriggerFactoryError::PeriodicTriggerInvalidDuration { .. })),
            "Unexpected result: {result:?}"
        );
    }

    #[test]
    fn validate_conditional_trigger_negative_min_duration_fails() {
        let factory = create_factory();
        let (mut trigger, _) =
            create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));
        trigger.mut_conditional_trigger().condition_type =
            Some(ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: -3, ..Default::default() }).into(),
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            }));

        let result = factory.validate_using_context_with(&trigger, vec![None], None);

        assert_that!(result, err(matches_pattern!(TriggerFactoryError::InvalidEdgeOptions { .. })));
    }

    #[test]
    fn validate_conditional_trigger_no_parent_triggers_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_no_parent_triggers();

        let err = factory.validate_using_default_context(&trigger).unwrap_err();

        assert!(matches!(err, TriggerFactoryError::NoParentTriggers), "Unexpected error: {err:?}");
    }

    #[test]
    fn validate_conditional_trigger_missing_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_missing_node_index();

        let err = factory.validate_using_default_context(&trigger).unwrap_err();

        assert!(matches!(err, TriggerFactoryError::MissingNodeIndex), "Unexpected error: {err:?}");
    }

    #[test]
    fn validate_conditional_trigger_incorrect_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_incorrect_node_index();

        let err = factory.validate_using_default_context(&trigger).unwrap_err();

        assert!(
            matches!(err, TriggerFactoryError::IncorrectNodeIndex { node_index: NODE_INDEX }),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn validate_conditional_trigger_unspecified_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_unspecified_condition_type();

        let err = factory.validate_using_context_with(&trigger, vec![None], None).unwrap_err();

        assert!(
            matches!(err, TriggerFactoryError::UnspecifiedConditionTypeProto),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn validate_conditional_trigger_success() {
        let factory = create_factory();
        let (trigger, _) =
            create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));

        factory
            .validate_using_context_with(&trigger, vec![Some(NodeValueType::Bool)], None)
            .unwrap();
    }

    #[test]
    fn validate_conditional_trigger_unknown_node_type_success() {
        let factory = create_factory();
        let (trigger, _) =
            create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));

        factory.validate_using_context_with(&trigger, vec![None], None).unwrap();
    }

    parameterized_test!(
        #[test]
        validate_conditional_trigger_fire_initial_and_options_fails,

        |condition_type| {
            let factory = create_factory();

            let (mut trigger, _) = create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));
            trigger.mut_conditional_trigger().condition_type = condition_type;

            let err = factory.validate_using_context_with(&trigger, vec![Some(NodeValueType::Bool)], None).unwrap_err();
            assert_that!(err, matches_pattern!(TriggerFactoryError::InvalidConditionTypeProto { .. }));
        },

        rising_edge_fire_initial_and_options: (
            Some(ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                fire_initial: true,
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 7, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
        ),
        falling_edge_fire_initial_and_options: (
            Some(ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                fire_initial: true,
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 1, ..Default::default() }).into(),
                    require_exact: true,
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
        ),
        all_changes_fire_initial_and_rising_options: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                fire_initial: true,
                rising_options: Some(EdgeOptionsProto { require_exact: true, ..Default::default() }).into(),
                ..Default::default()
            })),
        ),
        all_changes_fire_initial_and_falling_options: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                fire_initial: true,
                falling_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                ..Default::default()
            })),
        ),
        all_changes_fire_initial_and_both_options: (
            Some(ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                fire_initial: true,
                rising_options: Some(EdgeOptionsProto {
                    min_duration: Some(DurationProto { seconds: 3, ..Default::default() }).into(),
                    ..Default::default()
                }).into(),
                falling_options: Some(EdgeOptionsProto { require_exact: true, ..Default::default() }).into(),
                ..Default::default()
            })),
        ),
    );
}
