// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::metrics_configuration::{
    trigger::Trigger_type as TriggerTypeProto, ConditionalTrigger as ConditionalTriggerProto,
    Trigger as TriggerProto,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::expr::ast::Node;
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
    pub publishers: &'a HashMap<String, Arc<Pub>>,
    pub triggers: &'a HashMap<String, Arc<Trig>>,
}

pub(crate) struct TriggerFactoryValidateContext {
    pub expression_nodes_count: usize,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum TriggerFactoryError {
    #[error("No parent triggers set for conditional trigger")]
    NoParentTriggers,
    #[error("Required field selector_node_index is not set for conditional trigger")]
    MissingNodeIndex,
    #[error("Incorrect conditional trigger node index {node_index}")]
    IncorrectNodeIndex { node_index: u32 },
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
        Ok(Arc::new(TriggerImpl::new(
            trigger_proto.name.clone(),
            match &trigger_proto.trigger_type {
                Some(TriggerTypeProto::DataTrigger(data_trigger)) => TriggerType::Data {
                    publisher: context.publishers[&data_trigger.publisher_name].clone(),
                },
                Some(TriggerTypeProto::PeriodicTrigger(periodic_trigger)) => {
                    TriggerType::Periodic { interval_ms: periodic_trigger.interval_ms }
                }
                Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                    TriggerFactoryImpl::verify_not_empty(&conditional_trigger.trigger_names)?;
                    let selector_node_index = Self::get_selector_node_index(conditional_trigger)?;
                    let selector_expression = context
                        .expression_nodes
                        .get(selector_node_index as usize)
                        .ok_or(TriggerFactoryError::IncorrectNodeIndex {
                            node_index: selector_node_index,
                        })?;
                    TriggerType::Conditional {
                        triggers: conditional_trigger
                            .trigger_names
                            .iter()
                            .map(|name| context.triggers[name].clone())
                            .collect(),
                        selector_expression: selector_expression.clone(),
                    }
                }
                _ => return Err(TriggerFactoryError::UnknownTriggerType),
            },
        )))
    }

    fn validate(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryValidateContext,
    ) -> Result<(), TriggerFactoryError> {
        match &trigger_proto.trigger_type {
            Some(TriggerTypeProto::DataTrigger(_)) | Some(TriggerTypeProto::PeriodicTrigger(_)) => {
            }
            Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                TriggerFactoryImpl::verify_not_empty(&conditional_trigger.trigger_names)?;
                let selector_node_index = Self::get_selector_node_index(conditional_trigger)?;
                if selector_node_index as usize >= context.expression_nodes_count {
                    return Err(TriggerFactoryError::IncorrectNodeIndex {
                        node_index: selector_node_index,
                    });
                }
            }
            _ => return Err(TriggerFactoryError::UnknownTriggerType),
        };
        Ok(())
    }
}

impl TriggerFactoryImpl {
    pub(super) fn new() -> Self {
        TriggerFactoryImpl {}
    }

    fn get_selector_node_index(
        conditional_trigger_proto: &ConditionalTriggerProto,
    ) -> Result<u32, TriggerFactoryError> {
        conditional_trigger_proto.selector_node_index.ok_or(TriggerFactoryError::MissingNodeIndex)
    }

    fn verify_not_empty(names: &[String]) -> Result<(), TriggerFactoryError> {
        if names.is_empty() {
            Err(TriggerFactoryError::NoParentTriggers)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::collection_extension::VecExtension;
    use crate::expr::node_value::NodeValue;
    use crate::metrics_config_orchestrator::publisher::PublisherType;
    use crate::test_utils::ptr_eq;

    const INTERVAL: u64 = u64::MAX;
    const NODE_INDEX: u32 = 42;
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const SIGNAL_NAME: &str = "signal";
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

        let publisher = Arc::new(PublisherImpl::new(
            PUBLISHER_NAME.to_owned(),
            PublisherType::Signal { signal_name: SIGNAL_NAME.to_owned() },
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
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(PARENT_TRIGGER_NAME.to_owned());

        let parent_trigger = Arc::new(TriggerImpl::new(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL },
        ));

        (trigger, Some(vec![parent_trigger]))
    }

    fn create_conditional_trigger_with_incorrect_node_index() -> TriggerProto {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.selector_node_index = Some(NODE_INDEX);
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
            self.create_using_context_with(trigger_proto, None, None, None)
        }

        fn create_using_context_with(
            &self,
            trigger_proto: &TriggerProto,
            publishers: Option<Vec<Arc<PublisherImpl>>>,
            triggers: Option<Vec<Arc<TriggerImpl>>>,
            nodes: Option<Vec<Arc<Node>>>,
        ) -> Result<Arc<TriggerImpl>, TriggerFactoryError> {
            let expression_nodes = nodes.unwrap_or_default();
            let publishers = publishers.map_or(HashMap::new(), |val| {
                val.into_hash_map(|publisher| publisher.name.clone())
            });
            let triggers = triggers
                .map_or(HashMap::new(), |val| val.into_hash_map(|trigger| trigger.name.clone()));
            let context = TriggerFactoryCreateContext {
                expression_nodes: &expression_nodes,
                publishers: &publishers,
                triggers: &triggers,
            };
            self.create(trigger_proto, &context)
        }

        fn validate_using_default_context(
            &self,
            trigger_proto: &TriggerProto,
        ) -> Result<(), TriggerFactoryError> {
            self.validate_using_context_with(trigger_proto, 0)
        }

        fn validate_using_context_with(
            &self,
            trigger_proto: &TriggerProto,
            nodes_count: usize,
        ) -> Result<(), TriggerFactoryError> {
            let context = TriggerFactoryValidateContext { expression_nodes_count: nodes_count };
            self.validate(trigger_proto, &context)
        }
    }

    #[test]
    fn create_data_trigger_success() {
        let factory = create_factory();
        let (trigger, publisher) = create_valid_data_trigger();

        let res = factory
            .create_using_context_with(&trigger, Some(vec![publisher.clone()]), None, None)
            .unwrap();

        assert_eq!(res.name, TRIGGER_NAME);
        if let TriggerType::Data { publisher: res_publisher } = &res.trigger_type {
            assert!(ptr_eq(res_publisher, &(publisher as Arc<dyn Publisher + Send + Sync>)));
        } else {
            panic!("Unexpected trigger type");
        }
    }

    #[test]
    fn create_periodic_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_periodic_trigger();

        let res = factory.create_using_default_context(&trigger).unwrap();

        assert_eq!(res.name, TRIGGER_NAME);
        if let TriggerType::Periodic { interval_ms } = res.trigger_type {
            assert_eq!(interval_ms, INTERVAL);
        } else {
            panic!("Unexpected trigger type");
        }
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
    fn create_conditional_trigger_success() {
        let factory = create_factory();
        let (trigger, parent_triggers) = create_valid_conditional_trigger();

        let node_0 = Arc::new(create_node());
        let node_1 = Arc::new(create_node());
        let res = factory
            .create_using_context_with(
                &trigger,
                None,
                parent_triggers.clone(),
                Some(vec![node_0.clone(), node_1.clone()]),
            )
            .unwrap();

        assert_eq!(res.name, TRIGGER_NAME);
        if let TriggerType::Conditional { triggers, selector_expression } = &res.trigger_type {
            assert_eq!(triggers.len(), 1);
            assert!(Arc::ptr_eq(&triggers[0], &parent_triggers.unwrap()[0]));
            // selector_expression should match the node at index 0
            assert!(Arc::ptr_eq(selector_expression, &node_0));
            assert!(!Arc::ptr_eq(selector_expression, &node_1))
        } else {
            panic!("Unexpected trigger type");
        }
    }

    #[test]
    fn validate_data_trigger_success() {
        let factory = create_factory();
        let (trigger, publisher) = create_valid_data_trigger();

        factory.create_using_context_with(&trigger, Some(vec![publisher]), None, None).unwrap();
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

        let err = factory.validate_using_default_context(&trigger).err().unwrap();

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

        let err = factory.validate_using_default_context(&trigger).err().unwrap();

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

        let err = factory.validate_using_default_context(&trigger).err().unwrap();

        assert!(
            matches!(err, TriggerFactoryError::IncorrectNodeIndex { node_index: NODE_INDEX }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_success() {
        let factory = create_factory();
        let (trigger, _) = create_valid_conditional_trigger();

        factory.validate_using_context_with(&trigger, 1).unwrap();
    }
}
