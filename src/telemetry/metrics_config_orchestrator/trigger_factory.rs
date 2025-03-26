// Copyright 2023 Google LLC

use anyhow::{anyhow, Context};
use protobuf::{well_known_types::duration::Duration as DurationProto, Message};
use sdv_telemetry_expr::{node::Node, node_value_type::NodeValueType};
use sdv_telemetry_proto_rust::metrics_configuration::{
    conditional_trigger::ConditionTypeAllChanges, conditional_trigger::ConditionTypeFallingEdge,
    conditional_trigger::ConditionTypeRisingEdge,
    conditional_trigger::Condition_type as ConditionTypeProto,
    conditional_trigger::EdgeOptions as EdgeOptionsProto,
    publisher::Publisher_type as PublisherTypeProto,
    service_publisher::ConnectionType as ConnectionTypeProto,
    trigger::Trigger_type as TriggerTypeProto, ConditionalTrigger as ConditionalTriggerProto,
    PeriodicTrigger as PeriodicTriggerProto, Publisher as PublisherProto, Trigger as TriggerProto,
};
use sdv_telemetry_utils::duration_utils::try_into_duration;
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use thiserror::Error;

use super::trigger::conditional::{ConditionType, ConditionalTrigger, EdgeOptions};
use super::trigger::data::DataTrigger;
use super::trigger::periodic::PeriodicTrigger;
use crate::metrics_config_orchestrator::{
    node_evaluation_context_factory::NodeEvaluationContextFactory, Publisher, Trigger,
};
use crate::status_reporter::StatusReporter;

#[cfg_attr(test, sdv_telemetry_utils::with_super_trait(downcast_rs::Downcast))]
pub(crate) trait TriggerFactory: Send + Sync {
    /// Returns a new trigger encapsulated object based on the protobuf message.
    fn create(
        &self,
        metrics_config_uuid: String,
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

#[derive(Error, Debug)]
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
    #[error("Publisher {publisher_name} has getter connection type and cannot be listened to")]
    UnableToListenToGetterPublisher { publisher_name: String },
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
}

pub(crate) struct TriggerFactoryImpl {
    node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    status_reporter: Arc<dyn StatusReporter>,
}

impl TriggerFactory for TriggerFactoryImpl {
    fn create(
        &self,
        metrics_config_uuid: String,
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
                let publisher = &context.publishers[&data_trigger.publisher_name];

                if !publisher.is_subscribable() {
                    return Err(TriggerFactoryError::UnableToListenToGetterPublisher {
                        publisher_name: publisher.get_name().to_owned(),
                    });
                }

                DataTrigger::new(
                    trigger_name,
                    metrics_config_uuid,
                    Arc::<dyn Publisher>::clone(publisher),
                )
            }
            TriggerTypeProto::PeriodicTrigger(periodic_trigger) => PeriodicTrigger::new(
                trigger_name,
                metrics_config_uuid,
                Self::get_periodic_interval(periodic_trigger)?,
                Arc::clone(&self.node_evaluation_context_factory),
            ),
            TriggerTypeProto::ConditionalTrigger(conditional_trigger) => {
                TriggerFactoryImpl::verify_has_parent_triggers(&conditional_trigger.trigger_names)?;

                let selector_node_index = Self::get_selector_node_index(conditional_trigger)?;
                let selector_expression = context
                    .expression_nodes
                    .get(selector_node_index as usize)
                    .ok_or_else(|| TriggerFactoryError::IncorrectNodeIndex {
                        node_index: selector_node_index,
                    })?;
                let condition_type = Self::get_condition_type(
                    conditional_trigger,
                    context.node_types[selector_node_index as usize].as_ref(),
                )?;
                let triggers = conditional_trigger
                    .trigger_names
                    .iter()
                    .map(|name| Arc::clone(&context.triggers[name]) as Arc<dyn Trigger>)
                    .collect();
                ConditionalTrigger::new(
                    trigger_name,
                    metrics_config_uuid,
                    condition_type,
                    triggers,
                    Arc::clone(selector_expression),
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
                let publisher = &context.publishers[&data_trigger.publisher_name];

                if let Some(PublisherTypeProto::ServicePublisher(service_publisher)) =
                    &publisher.publisher_type
                {
                    if let Ok(connection_type) = service_publisher.connection_type.enum_value() {
                        match connection_type {
                            ConnectionTypeProto::GETTER => {
                                return Err(TriggerFactoryError::UnableToListenToGetterPublisher {
                                    publisher_name: publisher.name.clone(),
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
                let value_type = context.node_types.get(selector_node_index as usize).ok_or(
                    TriggerFactoryError::IncorrectNodeIndex { node_index: selector_node_index },
                )?;
                Self::get_condition_type(conditional_trigger, value_type.as_ref())?;
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
        Self { node_evaluation_context_factory, status_reporter }
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
        value_type: Option<&NodeValueType>,
    ) -> Result<ConditionType, TriggerFactoryError> {
        let proto_condition_type = conditional_trigger_proto
            .condition_type
            .as_ref()
            .ok_or_else(|| TriggerFactoryError::UnspecifiedConditionTypeProto)?;

        let is_bool_or_numeric = Box::new(|value_type: &NodeValueType| -> bool {
            value_type.is_bool() || value_type.is_numeric()
        });

        let (condition_type, is_value_type_allowed): (_, Box<dyn FnOnce(&NodeValueType) -> bool>) =
            match proto_condition_type {
                ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                    rising_options,
                    special_fields: _,
                }) => (
                    ConditionType::RisingEdge(Self::get_edge_options(rising_options)?),
                    is_bool_or_numeric,
                ),
                ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                    falling_options,
                    special_fields: _,
                }) => (
                    ConditionType::FallingEdge(Self::get_edge_options(falling_options)?),
                    is_bool_or_numeric,
                ),
                ConditionTypeProto::AllChanges(ConditionTypeAllChanges {
                    rising_options,
                    falling_options,
                    special_fields: _,
                }) => {
                    // If either of the edge options are set and not the default, use
                    // `ConditionType::BothEdges`, otherwise use the `ConditionType::AllChanges`.
                    if rising_options
                        .as_ref()
                        .is_some_and(|options| options != EdgeOptionsProto::default_instance())
                        || falling_options
                            .as_ref()
                            .is_some_and(|options| options != EdgeOptionsProto::default_instance())
                    {
                        (
                            ConditionType::BothEdges {
                                rising_options: Self::get_edge_options(rising_options)?,
                                falling_options: Self::get_edge_options(falling_options)?,
                            },
                            is_bool_or_numeric,
                        )
                    } else {
                        (
                            ConditionType::AllChanges,
                            // `AllChanges` uses the NOT_EQ operator, which supports all node value
                            // types.
                            Box::new(|_| true),
                        )
                    }
                }
                ConditionTypeProto::IsTrue(_) => (ConditionType::IsTrue, is_bool_or_numeric),
                ConditionTypeProto::IsFalse(_) => (ConditionType::IsFalse, is_bool_or_numeric),
                unknown_condition_type => {
                    return Err(TriggerFactoryError::UnknownConditionTypeProto {
                        condition_type: unknown_condition_type.clone(),
                    })
                }
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

    fn get_selector_node_index(
        conditional_trigger_proto: &ConditionalTriggerProto,
    ) -> Result<u32, TriggerFactoryError> {
        conditional_trigger_proto.selector_node_index.ok_or(TriggerFactoryError::MissingNodeIndex)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::matchers::{
        contains_substring, eq, err, matches_pattern, ok, result_of_ref, some,
    };
    use googletest::{assert_that, expect_that};
    use protobuf::MessageField;
    use sdv_telemetry_expr::node_value::NodeValue;
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_proto_rust::metrics_configuration::{
        conditional_trigger::Condition_type as ConditionTypeProto,
        ServicePublisher as ServicePublisherProto,
    };
    use sdv_telemetry_service_publisher_accessor::SubSamplingInterval;
    use sdv_telemetry_test_utils::parameterized_test;
    use sdv_telemetry_utils::collection_extension::VecExtension;
    use sdv_telemetry_utils::duration_utils::try_into_proto_duration;
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::metrics_config_orchestrator::publisher::service::{
        ConnectionType, ServicePublisher,
    };
    use crate::metrics_config_orchestrator::{
        node_evaluation_context_factory::NodeEvaluationContextFactoryImpl,
        service_adapter::mock::MockServiceAdapter,
    };
    use crate::StatusReporterImpl;

    const INTERVAL: Duration = Duration::from_secs(u32::MAX as u64);
    const NODE_INDEX: u32 = 42;
    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service_name";
    const TRIGGER_NAME: &str = "trigger";

    fn create_factory() -> TriggerFactoryImpl {
        TriggerFactoryImpl {
            node_evaluation_context_factory: Arc::new(NodeEvaluationContextFactoryImpl::new(
                new_stub_pal(),
            )),
            status_reporter: Arc::new(StatusReporterImpl::new(|err| {
                panic!("Unexpected error: {err:?}")
            })),
        }
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
        PUBLISHER_NAME.clone_into(&mut data_trigger.publisher_name);
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
            CONFIG_UUID.to_owned(),
            INTERVAL,
            node_evaluation_context_factory,
        );

        (trigger, Some(vec![parent_trigger]))
    }

    fn create_conditional_trigger_with_incorrect_node_index() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
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
            self.create(CONFIG_UUID.to_owned(), trigger_proto, &context)
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

        assert!(matches!(&err, TriggerFactoryError::NameNotSet), "Unexpected error: {:?}", err);
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
            CONFIG_UUID.to_owned(),
            SERVICE_NAME.to_owned(),
            SubSamplingInterval::ZERO,
            Arc::clone(&factory.node_evaluation_context_factory),
            Arc::new(MockServiceAdapter::new()),
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
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
            matches!(&err, TriggerFactoryError::UnableToListenToGetterPublisher { publisher_name } if publisher_name == PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_data_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_data_trigger();
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            None,
            ConnectionType::Subscription,
            false,
            CONFIG_UUID.to_owned(),
            SERVICE_NAME.to_owned(),
            SubSamplingInterval::ZERO,
            Arc::clone(&factory.node_evaluation_context_factory),
            Arc::new(MockServiceAdapter::new()),
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
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
        assert_eq!(res.get_metrics_config_uuid(), CONFIG_UUID);
        let res_trigger = res.downcast_arc::<DataTrigger>().ok().unwrap();
        assert!(Arc::ptr_eq(&res_trigger.publisher, &(publisher as Arc<dyn Publisher>)));
    }

    #[test]
    fn create_periodic_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_periodic_trigger();

        let res = factory.create_using_default_context(&trigger).unwrap();

        assert_eq!(res.get_name(), TRIGGER_NAME);
        assert_eq!(res.get_metrics_config_uuid(), CONFIG_UUID);
        let res_trigger = res.downcast_arc::<PeriodicTrigger>().ok().unwrap();
        assert_eq!(res_trigger.interval, INTERVAL);
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

        assert!(
            matches!(err, TriggerFactoryError::NoParentTriggers),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_conditional_trigger_missing_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_missing_node_index();

        let err = factory.create_using_default_context(&trigger).err().unwrap();

        assert!(
            matches!(err, TriggerFactoryError::MissingNodeIndex),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_conditional_trigger_incorrect_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_incorrect_node_index();

        let err = factory.create_using_default_context(&trigger).err().unwrap();

        assert!(
            matches!(err, TriggerFactoryError::IncorrectNodeIndex { node_index: NODE_INDEX }),
            "Unexpected error: {:?}",
            err
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
            "Unexpected error: {:?}",
            err
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
        let factory = create_factory();
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
        assert_eq!(res.get_metrics_config_uuid(), CONFIG_UUID);
        let res_trigger = res.downcast_arc::<ConditionalTrigger>().ok().unwrap();
        // selector_expression should match the node at index 0
        assert!(Arc::ptr_eq(&res_trigger.selector_expression, &node_0));
        assert!(!Arc::ptr_eq(&res_trigger.selector_expression, &node_1));
        assert_eq!(res_trigger.triggers.len(), 1);
        assert!(Arc::ptr_eq(
            &res_trigger.triggers[0],
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
            let factory = create_factory();
            let node = Arc::new(create_node());

            let (mut trigger, parent_triggers) = create_valid_conditional_trigger(Arc::clone(&factory.node_evaluation_context_factory));
            trigger.mut_conditional_trigger().condition_type = condition_type;

            let result = factory.create_using_context_with(
                &trigger,
                Some(vec![node]),
                vec![None],
                None,
                parent_triggers,
            );

            assert_that!(
                result.unwrap().downcast_arc::<ConditionalTrigger>().ok().unwrap().condition_type,
                matcher
            );
        },

        all_changes_default: (
            Some(ConditionTypeProto::AllChanges(Default::default())),
            eq(&ConditionType::AllChanges),
        ),
        rising_default: (
            Some(ConditionTypeProto::RisingEdge(Default::default())),
            eq(&ConditionType::RisingEdge(EdgeOptions::default())),
        ),
        falling_default: (
            Some(ConditionTypeProto::FallingEdge(Default::default())),
            eq(&ConditionType::FallingEdge(EdgeOptions::default())),
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
            eq(&ConditionType::AllChanges),
        ),
        rising_with_default_options: (
            Some(ConditionTypeProto::RisingEdge(ConditionTypeRisingEdge {
                rising_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default()
            })),
            eq(&ConditionType::RisingEdge(EdgeOptions::default())),
        ),
        falling_with_default_options: (
            Some(ConditionTypeProto::FallingEdge(ConditionTypeFallingEdge {
                falling_options: Some(EdgeOptionsProto::default()).into(),
                ..Default::default()
            })),
            eq(&ConditionType::FallingEdge(EdgeOptions::default())),
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
            eq(&ConditionType::RisingEdge(EdgeOptions { min_duration: Duration::from_secs(1), require_exact: false })),
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
            eq(&ConditionType::FallingEdge(EdgeOptions { min_duration: Duration::from_secs(7), require_exact: true })),
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

        assert!(matches!(&err, TriggerFactoryError::NameNotSet), "Unexpected error: {:?}", err);
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
            publisher_type: Some(PublisherTypeProto::ServicePublisher(ServicePublisherProto {
                connection_type: ConnectionTypeProto::GETTER.into(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let err = factory
            .validate_using_context_with(&trigger, vec![], Some(vec![publisher]))
            .err()
            .unwrap();

        assert!(
            matches!(&err, TriggerFactoryError::UnableToListenToGetterPublisher { publisher_name } if publisher_name == PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_data_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_data_trigger();
        let publisher = PublisherProto {
            name: PUBLISHER_NAME.to_string(),
            publisher_type: Some(PublisherTypeProto::ServicePublisher(ServicePublisherProto {
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

        assert!(
            matches!(err, TriggerFactoryError::NoParentTriggers),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_missing_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_missing_node_index();

        let err = factory.validate_using_default_context(&trigger).unwrap_err();

        assert!(
            matches!(err, TriggerFactoryError::MissingNodeIndex),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_incorrect_node_index_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_incorrect_node_index();

        let err = factory.validate_using_default_context(&trigger).unwrap_err();

        assert!(
            matches!(err, TriggerFactoryError::IncorrectNodeIndex { node_index: NODE_INDEX }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_unspecified_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_unspecified_condition_type();

        let err = factory.validate_using_context_with(&trigger, vec![None], None).unwrap_err();

        assert!(
            matches!(err, TriggerFactoryError::UnspecifiedConditionTypeProto),
            "Unexpected error: {:?}",
            err
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
}
