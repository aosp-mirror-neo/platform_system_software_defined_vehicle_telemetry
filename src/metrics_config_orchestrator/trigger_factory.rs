// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::metrics_configuration::{
    conditional_trigger::ConditionType, trigger::Trigger_type as TriggerTypeProto,
    ConditionalTrigger as ConditionalTriggerProto, Trigger as TriggerProto,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::expr::{ast::Node, node_value_type::NodeValueType};
use crate::metrics_config_orchestrator::{
    trigger::TriggerType, Publisher, PublisherImpl, Trigger, TriggerImpl,
};

// TODO(b/304793694): refactor tests and remove generic parameters.
pub(crate) trait TriggerFactory<
    TPublisher: Publisher = PublisherImpl,
    TTrigger: Trigger = TriggerImpl,
>
{
    /// Returns a new trigger encapsulated object based on the protobuf message.
    fn create(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryCreateContext<TPublisher, TTrigger>,
    ) -> Result<Arc<TTrigger>, TriggerFactoryError>;

    /// Verifies that the protobuf message is valid and can be used to create a trigger encapsulated object.
    fn validate(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryValidateContext,
    ) -> Result<(), TriggerFactoryError>;
}

// TODO(b/304793694): refactor tests and remove generic parameters.
pub(crate) struct TriggerFactoryCreateContext<
    'a,
    Pub: Publisher = PublisherImpl,
    Trig: Trigger = TriggerImpl,
> {
    pub expression_nodes: &'a Vec<Arc<Node>>,
    pub node_types: &'a [Option<NodeValueType>],
    pub publishers: &'a HashMap<String, Arc<Pub>>,
    pub triggers: &'a HashMap<String, Arc<Trig>>,
}

pub(crate) struct TriggerFactoryValidateContext<'a> {
    pub node_types: &'a [Option<NodeValueType>],
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum TriggerFactoryError {
    #[error("Trigger name not set")]
    NameNotSet,
    #[error("No parent triggers set for conditional trigger")]
    NoParentTriggers,
    #[error("Required field selector_node_index is not set for conditional trigger")]
    MissingNodeIndex,
    #[error("Incorrect conditional trigger node index {node_index}")]
    IncorrectNodeIndex { node_index: u32 },
    #[error("Unknown condition type {condition_type}")]
    UnknownConditionType { condition_type: i32 },
    #[error("Condition type is not specified")]
    UnspecifiedConditionType,
    #[error(
        "Condition {condition_type:?} cannot be applied to an expression of type {value_type:?}"
    )]
    InvalidNodeValueType { condition_type: ConditionType, value_type: NodeValueType },
    #[error("Unknown trigger type")]
    UnknownTriggerType,
}

pub(crate) struct TriggerFactoryImpl {}

impl TriggerFactory for TriggerFactoryImpl {
    fn create(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryCreateContext,
    ) -> Result<Arc<TriggerImpl>, TriggerFactoryError> {
        Self::verify_name_set(trigger_proto)?;
        let trigger_name = trigger_proto.name.clone();
        let trigger_type = match &trigger_proto.trigger_type {
            Some(TriggerTypeProto::DataTrigger(data_trigger)) => TriggerType::Data {
                publisher: Arc::<PublisherImpl>::clone(
                    &context.publishers[&data_trigger.publisher_name],
                ),
            },
            Some(TriggerTypeProto::PeriodicTrigger(periodic_trigger)) => {
                TriggerType::Periodic { interval_ms: periodic_trigger.interval_ms }
            }
            Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                TriggerFactoryImpl::verify_has_parent_triggers(&conditional_trigger.trigger_names)?;

                let selector_node_index = Self::get_selector_node_index(conditional_trigger)?;
                let selector_expression =
                    context.expression_nodes.get(selector_node_index as usize).ok_or(
                        TriggerFactoryError::IncorrectNodeIndex { node_index: selector_node_index },
                    )?;
                let condition_type = Self::get_condition_type(
                    conditional_trigger,
                    &context.node_types[selector_node_index as usize],
                )?;
                let triggers = conditional_trigger
                    .trigger_names
                    .iter()
                    .map(|name| Arc::clone(&context.triggers[name]) as Arc<dyn Trigger>)
                    .collect();
                return Ok(TriggerImpl::new_conditional_trigger(
                    trigger_name,
                    condition_type,
                    triggers,
                    Arc::clone(selector_expression),
                ));
            }
            _ => return Err(TriggerFactoryError::UnknownTriggerType),
        };

        Ok(TriggerImpl::new(trigger_name, trigger_type))
    }

    fn validate(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryValidateContext,
    ) -> Result<(), TriggerFactoryError> {
        Self::verify_name_set(trigger_proto)?;
        match &trigger_proto.trigger_type {
            Some(TriggerTypeProto::DataTrigger(_)) | Some(TriggerTypeProto::PeriodicTrigger(_)) => {
            }
            Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                TriggerFactoryImpl::verify_has_parent_triggers(&conditional_trigger.trigger_names)?;
                let selector_node_index = Self::get_selector_node_index(conditional_trigger)?;
                let value_type = context.node_types.get(selector_node_index as usize).ok_or(
                    TriggerFactoryError::IncorrectNodeIndex { node_index: selector_node_index },
                )?;
                Self::get_condition_type(conditional_trigger, value_type)?;
            }
            _ => return Err(TriggerFactoryError::UnknownTriggerType),
        };
        Ok(())
    }
}

impl TriggerFactoryImpl {
    pub(super) fn new() -> Self {
        Self {}
    }

    fn get_condition_type(
        conditional_trigger_proto: &ConditionalTriggerProto,
        value_type: &Option<NodeValueType>,
    ) -> Result<ConditionType, TriggerFactoryError> {
        let condition_type = conditional_trigger_proto
            .condition_type
            .enum_value()
            .map_err(|value| TriggerFactoryError::UnknownConditionType { condition_type: value })?;

        if matches!(condition_type, ConditionType::CONDITION_TYPE_UNSPECIFIED) {
            return Err(TriggerFactoryError::UnspecifiedConditionType);
        }

        if let Some(value_type) = value_type {
            if !value_type.is_bool() && !value_type.is_numeric() {
                return Err(TriggerFactoryError::InvalidNodeValueType {
                    condition_type,
                    value_type: value_type.clone(),
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
    use protobuf::EnumOrUnknown;
    use sdv_telemetry_utils::collection_extension::VecExtension;
    use sdvtelemetry_proto_rust::metrics_configuration::conditional_trigger::ConditionType;
    use std::collections::HashMap;

    use super::*;
    use crate::expr::node_value::NodeValue;
    use crate::metrics_config_orchestrator::service_adapter::mock::MockServiceAdapter;

    const INTERVAL: u64 = u64::MAX;
    const NODE_INDEX: u32 = 42;
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service_name";
    const TRIGGER_NAME: &str = "trigger";

    fn create_factory() -> TriggerFactoryImpl {
        TriggerFactoryImpl {}
    }

    fn new_trigger(name: &str) -> TriggerProto {
        let mut trigger = TriggerProto::new();
        trigger.name = name.to_owned();
        trigger
    }

    fn create_node() -> Node {
        Node::ConstantLeaf { value: NodeValue::Bool(false) }
    }

    fn create_valid_data_trigger() -> (TriggerProto, Arc<PublisherImpl>) {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let data_trigger = trigger.mut_data_trigger();
        data_trigger.publisher_name = PUBLISHER_NAME.to_owned();

        let publisher = Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            Arc::new(MockServiceAdapter::new()),
        ));

        (trigger, publisher)
    }

    fn create_valid_periodic_trigger() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let periodic_trigger = trigger.mut_periodic_trigger();
        periodic_trigger.interval_ms = INTERVAL;
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

    fn create_valid_conditional_trigger() -> (TriggerProto, Option<Vec<Arc<TriggerImpl>>>) {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.condition_type = ConditionType::ALL_CHANGES.into();
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(PARENT_TRIGGER_NAME.to_owned());

        let parent_trigger = TriggerImpl::new(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL },
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

    fn create_conditional_trigger_with_unknown_condition_type() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.condition_type = EnumOrUnknown::from_i32(-1);
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(String::default());
        trigger
    }

    fn create_conditional_trigger_with_unspecified_condition_type() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.condition_type = ConditionType::CONDITION_TYPE_UNSPECIFIED.into();
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(String::default());
        trigger
    }

    fn create_conditional_trigger_with_all_changes_condition_type() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.condition_type = ConditionType::ALL_CHANGES.into();
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
        ) -> Result<Arc<TriggerImpl>, TriggerFactoryError> {
            self.create_using_context_with(trigger_proto, None, vec![], None, None)
        }

        fn create_using_context_with(
            &self,
            trigger_proto: &TriggerProto,
            nodes: Option<Vec<Arc<Node>>>,
            node_types: Vec<Option<NodeValueType>>,
            publishers: Option<Vec<Arc<PublisherImpl>>>,
            triggers: Option<Vec<Arc<TriggerImpl>>>,
        ) -> Result<Arc<TriggerImpl>, TriggerFactoryError> {
            let expression_nodes = nodes.unwrap_or_default();
            let publishers = publishers.map_or(HashMap::new(), |val| {
                val.into_hash_map(|publisher| publisher.name.clone())
            });
            let triggers = triggers
                .map_or(HashMap::new(), |val| val.into_hash_map(|trigger| trigger.name.clone()));
            let context = TriggerFactoryCreateContext {
                expression_nodes: &expression_nodes,
                node_types: &node_types,
                publishers: &publishers,
                triggers: &triggers,
            };
            self.create(trigger_proto, &context)
        }

        fn validate_using_default_context(
            &self,
            trigger_proto: &TriggerProto,
        ) -> Result<(), TriggerFactoryError> {
            self.validate_using_context_with(trigger_proto, vec![])
        }

        fn validate_using_context_with(
            &self,
            trigger_proto: &TriggerProto,
            node_types: Vec<Option<NodeValueType>>,
        ) -> Result<(), TriggerFactoryError> {
            let context = TriggerFactoryValidateContext { node_types: &node_types };
            self.validate(trigger_proto, &context)
        }
    }

    #[test]
    fn create_no_name_fails() {
        let factory = create_factory();

        let err = factory.create_using_default_context(&TriggerProto::new()).err().unwrap();

        assert!(matches!(&err, TriggerFactoryError::NameNotSet), "Unexpected error: {:?}", err);
    }

    #[test]
    fn create_data_trigger_success() {
        let factory = create_factory();
        let (trigger, publisher) = create_valid_data_trigger();

        let res = factory
            .create_using_context_with(
                &trigger,
                None,
                vec![],
                Some(vec![Arc::clone(&publisher)]),
                None,
            )
            .unwrap();

        assert_eq!(res.name, TRIGGER_NAME);
        let TriggerType::Data { publisher: res_publisher } = &res.trigger_type else {
            panic!("Unexpected trigger type");
        };
        assert!(Arc::ptr_eq(res_publisher, &(publisher as Arc<dyn Publisher>)));
    }

    #[test]
    fn create_periodic_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_periodic_trigger();

        let res = factory.create_using_default_context(&trigger).unwrap();

        assert_eq!(res.name, TRIGGER_NAME);
        let TriggerType::Periodic { interval_ms } = res.trigger_type else {
            panic!("Unexpected trigger type");
        };
        assert_eq!(interval_ms, INTERVAL);
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
    fn create_conditional_trigger_unknown_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_unknown_condition_type();

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
            matches!(err, TriggerFactoryError::UnknownConditionType { condition_type: -1 }),
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
            matches!(err, TriggerFactoryError::UnspecifiedConditionType),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_conditional_trigger_incompatible_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_all_changes_condition_type();

        let err = factory
            .create_using_context_with(
                &trigger,
                Some(vec![Arc::new(create_node())]),
                vec![Some(NodeValueType::BoolList)],
                None,
                None,
            )
            .err()
            .unwrap();

        assert!(
            matches!(err, TriggerFactoryError::InvalidNodeValueType { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_conditional_trigger_success() {
        let factory = create_factory();
        let (trigger, parent_triggers) = create_valid_conditional_trigger();

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

        assert_eq!(res.name, TRIGGER_NAME);
        let TriggerType::Conditional { condition_type, selector_expression, triggers, .. } =
            &res.trigger_type
        else {
            panic!("Unexpected trigger type");
        };
        assert!(matches!(condition_type, ConditionType::ALL_CHANGES));
        // selector_expression should match the node at index 0
        assert!(Arc::ptr_eq(selector_expression, &node_0));
        assert!(!Arc::ptr_eq(selector_expression, &node_1));
        assert_eq!(triggers.len(), 1);
        assert!(Arc::ptr_eq(
            &triggers[0],
            &(Arc::clone(&parent_triggers.unwrap()[0]) as Arc<dyn Trigger>)
        ));
    }

    #[test]
    fn create_conditional_trigger_unknown_node_type_success() {
        let factory = create_factory();
        let (trigger, parent_triggers) = create_valid_conditional_trigger();

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

    #[test]
    fn validate_no_name_fails() {
        let factory = create_factory();

        let err = factory.validate_using_default_context(&TriggerProto::new()).unwrap_err();

        assert!(matches!(&err, TriggerFactoryError::NameNotSet), "Unexpected error: {:?}", err);
    }

    #[test]
    fn validate_data_trigger_success() {
        let factory = create_factory();
        let (trigger, publisher) = create_valid_data_trigger();

        factory
            .create_using_context_with(&trigger, None, vec![], Some(vec![publisher]), None)
            .unwrap();
    }

    #[test]
    fn validate_periodic_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_periodic_trigger();

        factory.validate_using_default_context(&trigger).unwrap();
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
    fn validate_conditional_trigger_unknown_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_unknown_condition_type();

        let err = factory.validate_using_context_with(&trigger, vec![None]).unwrap_err();

        assert!(
            matches!(err, TriggerFactoryError::UnknownConditionType { condition_type: -1 }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_unspecified_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_unspecified_condition_type();

        let err = factory.validate_using_context_with(&trigger, vec![None]).unwrap_err();

        assert!(
            matches!(err, TriggerFactoryError::UnspecifiedConditionType),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_incompatible_condition_type_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_all_changes_condition_type();

        let err = factory
            .validate_using_context_with(&trigger, vec![Some(NodeValueType::BoolList)])
            .err()
            .unwrap();

        assert!(
            matches!(err, TriggerFactoryError::InvalidNodeValueType { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_success() {
        let factory = create_factory();
        let (trigger, _) = create_valid_conditional_trigger();

        factory.validate_using_context_with(&trigger, vec![Some(NodeValueType::Bool)]).unwrap();
    }

    #[test]
    fn validate_conditional_trigger_unknown_node_type_success() {
        let factory = create_factory();
        let (trigger, _) = create_valid_conditional_trigger();

        factory.validate_using_context_with(&trigger, vec![None]).unwrap();
    }
}
