// Copyright 2023 Google LLC. All Rights Reserved.

use sdv_ast::Node;
use sdvtelemetry_proto_rust::metrics_configuration::{
    trigger::Trigger_type as TriggerTypeProto, ConditionalTrigger as ConditionalTriggerProto,
    Trigger as TriggerProto,
};
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::metrics_config_orchestrator::{trigger::TriggerType, MetricsConfig, Trigger};

pub(crate) trait TriggerFactory {
    /// Returns a new trigger encapsulated object based on the protobuf message.
    fn create(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryContext,
    ) -> Result<Trigger, TriggerFactoryError>;

    /// Verifies that the protobuf message is valid and can be used to create a trigger encapsulated object.
    fn validate(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryContext,
    ) -> Result<(), TriggerFactoryError>;
}

pub(crate) struct TriggerFactoryContext<'a> {
    pub config: &'a MetricsConfig,
    pub expression_nodes: &'a Vec<Arc<Node>>,
}

#[derive(Error, Debug)]
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

impl TriggerFactoryImpl {
    pub(super) fn new() -> Self {
        TriggerFactoryImpl {}
    }
}

impl TriggerFactoryImpl {
    fn get_conditional_trigger_expression<'a>(
        conditional_trigger_proto: &'a ConditionalTriggerProto,
        nodes: &'a [Arc<Node>],
    ) -> Result<&'a Arc<Node>, TriggerFactoryError> {
        let selector_node_index = conditional_trigger_proto
            .selector_node_index
            .ok_or(TriggerFactoryError::MissingNodeIndex)?;
        nodes
            .get(selector_node_index as usize)
            .ok_or(TriggerFactoryError::IncorrectNodeIndex { node_index: selector_node_index })
    }

    fn verify_not_empty(names: &[String]) -> Result<(), TriggerFactoryError> {
        if names.is_empty() {
            Err(TriggerFactoryError::NoParentTriggers)
        } else {
            Ok(())
        }
    }
}

impl TriggerFactory for TriggerFactoryImpl {
    fn create(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryContext,
    ) -> Result<Trigger, TriggerFactoryError> {
        Ok(Trigger::new(
            trigger_proto.name.clone(),
            match &trigger_proto.trigger_type {
                Some(TriggerTypeProto::DataTrigger(data_trigger)) => TriggerType::Data {
                    publisher: Arc::downgrade(
                        &context.config.publishers[&data_trigger.publisher_name],
                    ),
                },
                Some(TriggerTypeProto::PeriodicTrigger(periodic_trigger)) => {
                    TriggerType::Periodic { interval_ms: periodic_trigger.interval_ms }
                }
                Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                    TriggerFactoryImpl::verify_not_empty(&conditional_trigger.trigger_names)?;
                    let selector_expression =
                        TriggerFactoryImpl::get_conditional_trigger_expression(
                            conditional_trigger,
                            context.expression_nodes,
                        )?;
                    TriggerType::Conditional {
                        triggers: conditional_trigger
                            .trigger_names
                            .iter()
                            .map(|name| Arc::downgrade(&context.config.triggers[name]))
                            .collect(),
                        selector_expression: selector_expression.clone(),
                    }
                }
                _ => return Err(TriggerFactoryError::UnknownTriggerType),
            },
        ))
    }

    fn validate(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryContext,
    ) -> Result<(), TriggerFactoryError> {
        match &trigger_proto.trigger_type {
            Some(TriggerTypeProto::DataTrigger(_)) | Some(TriggerTypeProto::PeriodicTrigger(_)) => {
            }
            Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                TriggerFactoryImpl::verify_not_empty(&conditional_trigger.trigger_names)?;
                TriggerFactoryImpl::get_conditional_trigger_expression(
                    conditional_trigger,
                    context.expression_nodes,
                )?;
            }
            _ => return Err(TriggerFactoryError::UnknownTriggerType),
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sdv_ast::NodeValue;
    use std::collections::HashMap;

    use super::*;
    use crate::collection_extension::VecExtension;
    use crate::metrics_config_orchestrator::{publisher::PublisherType, Publisher};

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

    fn run_using_empty_factory_context<R>(
        execute_func: impl FnOnce(&TriggerFactoryContext) -> R,
    ) -> R {
        run_using_factory_context(None, None, None, execute_func)
    }

    // Creates a TriggerFactoryContext with passed data and uses it to call `execute_func`.
    // This is needed because TriggerFactoryContext contains borrows and it's impossible to have
    // a function that creates and returns it - it can only be used in the function where it's
    // created.
    fn run_using_factory_context<R>(
        publishers: Option<Vec<Arc<Publisher>>>,
        triggers: Option<Vec<Arc<Trigger>>>,
        nodes: Option<Vec<Arc<Node>>>,
        execute_func: impl FnOnce(&TriggerFactoryContext) -> R,
    ) -> R {
        let config = MetricsConfig {
            publishers: publishers
                .map_or(HashMap::new(), |val| val.into_hash_map(|trigger| trigger.name.clone())),
            triggers: triggers.map_or(HashMap::new(), |val| {
                val.into_hash_map(|publisher| publisher.name.clone())
            }),
        };
        let expression_nodes = nodes.unwrap_or_default();
        let context =
            TriggerFactoryContext { config: &config, expression_nodes: &expression_nodes };
        execute_func(&context)
    }

    fn create_valid_data_trigger() -> (TriggerProto, Option<Vec<Arc<Publisher>>>) {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let data_trigger = trigger.mut_data_trigger();
        data_trigger.publisher_name = PUBLISHER_NAME.to_owned();

        let publisher = Arc::new(Publisher {
            name: PUBLISHER_NAME.to_owned(),
            publisher_type: PublisherType::Signal { signal_name: SIGNAL_NAME.to_owned() },
        });

        (trigger, Some(vec![publisher]))
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

    fn create_valid_conditional_trigger() -> (TriggerProto, Option<Vec<Arc<Trigger>>>) {
        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(PARENT_TRIGGER_NAME.to_owned());

        let parent_trigger = Arc::new(Trigger::new(
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

    #[test]
    fn create_data_trigger_success() {
        let factory = create_factory();
        let (trigger, publishers) = create_valid_data_trigger();

        let res = run_using_factory_context(publishers.clone(), None, None, |context| {
            factory.create(&trigger, context)
        })
        .unwrap();

        assert_eq!(res.name, TRIGGER_NAME);
        if let TriggerType::Data { publisher } = res.trigger_type {
            let res_publisher = publisher.upgrade().unwrap();
            assert!(Arc::ptr_eq(&res_publisher, &publishers.unwrap()[0]));
            if let PublisherType::Signal { signal_name } = &res_publisher.publisher_type {
                assert_eq!(signal_name, SIGNAL_NAME);
            } else {
                panic!("Unexpected publisher type");
            }
        } else {
            panic!("Unexpected trigger type");
        }
    }

    #[test]
    fn create_periodic_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_periodic_trigger();

        let res =
            run_using_empty_factory_context(|context| factory.create(&trigger, context)).unwrap();

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

        let err = run_using_empty_factory_context(|context| factory.create(&trigger, context))
            .err()
            .unwrap();

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

        let err = run_using_empty_factory_context(|context| factory.create(&trigger, context))
            .err()
            .unwrap();

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

        let err = run_using_empty_factory_context(|context| factory.create(&trigger, context))
            .err()
            .unwrap();

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
        let res = run_using_factory_context(
            None,
            parent_triggers.clone(),
            Some(vec![node_0.clone(), node_1.clone()]),
            |context| factory.create(&trigger, context),
        )
        .unwrap();

        assert_eq!(res.name, TRIGGER_NAME);
        if let TriggerType::Conditional { triggers, selector_expression } = res.trigger_type {
            assert_eq!(triggers.len(), 1);
            let parent_trigger = triggers[0].upgrade().unwrap();
            assert_eq!(parent_trigger.name, parent_triggers.unwrap()[0].name);
            assert!(matches!(
                parent_trigger.trigger_type,
                TriggerType::Periodic { interval_ms: INTERVAL }
            ));
            // selector_expression should match the node at index 0
            assert!(Arc::ptr_eq(&selector_expression, &node_0));
            assert!(!Arc::ptr_eq(&selector_expression, &node_1))
        } else {
            panic!("Unexpected trigger type");
        }
    }

    #[test]
    fn validate_data_trigger_success() {
        let factory = create_factory();
        let (trigger, publishers) = create_valid_data_trigger();

        run_using_factory_context(publishers, None, None, |context| {
            factory.validate(&trigger, context)
        })
        .unwrap();
    }

    #[test]
    fn validate_periodic_trigger_success() {
        let factory = create_factory();
        let trigger = create_valid_periodic_trigger();

        run_using_empty_factory_context(|context| factory.validate(&trigger, context)).unwrap();
    }

    #[test]
    fn validate_conditional_trigger_no_parent_triggers_fails() {
        let factory = create_factory();
        let trigger = create_conditional_trigger_with_no_parent_triggers();

        let err = run_using_empty_factory_context(|context| factory.validate(&trigger, context))
            .err()
            .unwrap();

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

        let err = run_using_empty_factory_context(|context| factory.validate(&trigger, context))
            .err()
            .unwrap();

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

        let err = run_using_empty_factory_context(|context| factory.validate(&trigger, context))
            .err()
            .unwrap();

        assert!(
            matches!(err, TriggerFactoryError::IncorrectNodeIndex { node_index: NODE_INDEX }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_conditional_trigger_success() {
        let factory = create_factory();
        let (trigger, parent_triggers) = create_valid_conditional_trigger();

        run_using_factory_context(
            None,
            parent_triggers,
            Some(vec![Arc::new(create_node())]),
            |context| factory.create(&trigger, context),
        )
        .unwrap();
    }
}
