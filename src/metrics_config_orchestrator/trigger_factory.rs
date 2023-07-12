// Copyright 2023 Google LLC. All Rights Reserved.

use sdv_ast::Node;
use sdvtelemetry_proto_rust::metrics_configuration::{
    trigger::Trigger_type as TriggerTypeProto, Trigger as TriggerProto,
};
use std::{
    fmt::Debug,
    sync::{Arc, Weak},
};
use thiserror::Error;

use crate::metrics_config_orchestrator::{MetricsConfig, Publisher};

pub trait TriggerFactory {
    fn create(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryContext,
    ) -> Result<Trigger, TriggerFactoryError>;
}

pub struct Trigger {
    pub name: String,
    pub trigger_type: TriggerType,
}

pub enum TriggerType {
    Data { publisher: Weak<Publisher> },
    Periodic { interval_ms: u64 },
    Conditional { triggers: Vec<Weak<Trigger>>, selector_expression: Arc<Node> },
}

pub struct TriggerFactoryContext<'a> {
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

pub struct TriggerFactoryImpl {}

impl TriggerFactoryImpl {
    pub fn new() -> Self {
        TriggerFactoryImpl {}
    }
}

impl TriggerFactory for TriggerFactoryImpl {
    fn create(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryContext,
    ) -> Result<Trigger, TriggerFactoryError> {
        Ok(Trigger {
            name: trigger_proto.name.clone(),
            trigger_type: match &trigger_proto.trigger_type {
                Some(TriggerTypeProto::DataTrigger(data_trigger)) => TriggerType::Data {
                    publisher: Arc::downgrade(
                        &context.config.publishers[&data_trigger.publisher_name],
                    ),
                },
                Some(TriggerTypeProto::PeriodicTrigger(periodic_trigger)) => {
                    TriggerType::Periodic { interval_ms: periodic_trigger.interval_ms }
                }
                Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                    if conditional_trigger.trigger_names.is_empty() {
                        return Err(TriggerFactoryError::NoParentTriggers);
                    }
                    let selector_node_index = conditional_trigger
                        .selector_node_index
                        .ok_or(TriggerFactoryError::MissingNodeIndex)?;
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
                            .map(|name| Arc::downgrade(&context.config.triggers[name]))
                            .collect(),
                        selector_expression: selector_expression.clone(),
                    }
                }
                _ => return Err(TriggerFactoryError::UnknownTriggerType),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::metrics_config_orchestrator::publisher_factory::PublisherType;

    use super::*;

    const TRIGGER_NAME: &str = "trigger";

    fn create_factory() -> TriggerFactoryImpl {
        TriggerFactoryImpl {}
    }

    fn new_trigger(name: &str) -> TriggerProto {
        let mut trigger = TriggerProto::new();
        trigger.name = name.to_owned();
        trigger
    }

    fn run_using_empty_factory_context<R, F: FnOnce(&TriggerFactoryContext) -> R>(
        execute_func: F,
    ) -> R {
        run_using_factory_context(None, None, execute_func)
    }

    fn run_using_factory_context<R, FE>(
        init_metrics_config_func: Option<Box<dyn FnOnce(&mut MetricsConfig)>>,
        init_nodes_func: Option<Box<dyn FnOnce(&mut Vec<Arc<Node>>)>>,
        execute_func: FE,
    ) -> R
    where
        FE: FnOnce(&TriggerFactoryContext) -> R,
    {
        let mut config = MetricsConfig { publishers: HashMap::new(), triggers: HashMap::new() };
        let mut expression_nodes = vec![];
        if let Some(func) = init_metrics_config_func {
            func(&mut config);
        }
        if let Some(func) = init_nodes_func {
            func(&mut expression_nodes);
        }
        let context =
            TriggerFactoryContext { config: &config, expression_nodes: &expression_nodes };
        execute_func(&context)
    }

    #[test]
    fn create_data_trigger_success() {
        const PUBLISHER_NAME: &str = "publisher";
        const SIGNAL_NAME: &str = "signal";
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let data_trigger = trigger.mut_data_trigger();
        data_trigger.publisher_name = PUBLISHER_NAME.to_owned();

        let publisher = Arc::new(Publisher {
            name: PUBLISHER_NAME.to_owned(),
            publisher_type: PublisherType::Signal { signal_name: SIGNAL_NAME.to_owned() },
        });
        run_using_factory_context(
            Some(Box::new(move |configs| {
                configs.publishers.insert(PUBLISHER_NAME.to_owned(), publisher);
            })),
            None,
            |context| {
                let res = factory.create(&trigger, context).unwrap();
                assert_eq!(res.name, TRIGGER_NAME);
                if let TriggerType::Data { publisher } = res.trigger_type {
                    let res_publisher = publisher.upgrade().unwrap();
                    assert_eq!(res_publisher.name, PUBLISHER_NAME);
                    if let PublisherType::Signal { signal_name } = &res_publisher.publisher_type {
                        assert_eq!(signal_name, SIGNAL_NAME);
                    } else {
                        panic!("Unexpected publisher type");
                    }
                } else {
                    panic!("Unexpected trigger type");
                }
            },
        );
    }

    #[test]
    fn create_periodic_trigger_success() {
        const INTERVAL: u64 = u64::MAX;
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let periodic_trigger = trigger.mut_periodic_trigger();
        periodic_trigger.interval_ms = INTERVAL;

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

        let mut trigger = new_trigger(TRIGGER_NAME);
        trigger.mut_conditional_trigger();

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

        let mut trigger = new_trigger(TRIGGER_NAME);
        trigger.mut_conditional_trigger().trigger_names.push(String::default());

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
        const NODE_INDEX: u32 = 42;
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.selector_node_index = Some(NODE_INDEX);
        conditional_trigger.trigger_names.push(String::default());

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
        const PARENT_TRIGGER_NAME: &str = "parent_trigger";
        const INTERVAL: u64 = u64::MAX;
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(PARENT_TRIGGER_NAME.to_owned());

        let parent_trigger = Arc::new(Trigger {
            name: PARENT_TRIGGER_NAME.to_owned(),
            trigger_type: TriggerType::Periodic { interval_ms: INTERVAL },
        });

        let node_0 = Arc::new(Node::default());
        let node_1 = Arc::new(Node::default());
        // make clones for the init_nodes func closure, otherwise compiler complains
        // node_0 and node_1 don't live long enough.
        let node_0_clone = node_0.clone();
        let node_1_clone = node_1.clone();
        run_using_factory_context(
            Some(Box::new(move |configs| {
                configs.triggers.insert(PARENT_TRIGGER_NAME.to_owned(), parent_trigger);
            })),
            Some(Box::new(|nodes| {
                nodes.push(node_0_clone);
                nodes.push(node_1_clone);
            })),
            |context| {
                let res = factory.create(&trigger, context).unwrap();
                assert_eq!(res.name, TRIGGER_NAME);
                if let TriggerType::Conditional { triggers, selector_expression } = res.trigger_type
                {
                    assert_eq!(triggers.len(), 1);
                    let parent_trigger = triggers[0].upgrade().unwrap();
                    assert_eq!(parent_trigger.name, PARENT_TRIGGER_NAME);
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
            },
        );
    }
}
