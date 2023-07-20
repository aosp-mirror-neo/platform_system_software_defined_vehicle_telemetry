// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::{
    expression::Node as NodeProto, metrics_configuration::Trigger as TriggerProto,
};
use std::{
    fmt::Debug,
    sync::{Arc, Weak},
};
use thiserror::Error;

use crate::expression_evaluation::{ExpressionFactory, ExpressionFactoryImpl, ExpressionImpl};
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

pub struct TriggerFactoryContext<'a> {
    pub config: &'a MetricsConfig,
    pub expression_nodes: &'a [NodeProto],
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

pub enum TriggerType {
    Data { publisher: Weak<Publisher> },
    Periodic { interval_ms: u64 },
    Conditional { triggers: Vec<Weak<Trigger>>, selector_expression: ExpressionImpl },
}

pub struct TriggerFactoryImpl<ExprFactory: ExpressionFactory = ExpressionFactoryImpl> {
    expression_factory: ExprFactory,
}

impl TriggerFactoryImpl {
    pub fn new() -> Self {
        TriggerFactoryImpl { expression_factory: ExpressionFactoryImpl {} }
    }
}

impl<ExprFactory: ExpressionFactory> TriggerFactory for TriggerFactoryImpl<ExprFactory> {
    fn create(
        &self,
        trigger_proto: &TriggerProto,
        context: &TriggerFactoryContext,
    ) -> Result<Trigger, TriggerFactoryError> {
        Ok(Trigger {
            name: trigger_proto.name.clone(),
            trigger_type: if trigger_proto.has_data_trigger() {
                TriggerType::Data {
                    publisher: Arc::downgrade(
                        &context.config.publishers[&trigger_proto.data_trigger().publisher_name],
                    ),
                }
            } else if trigger_proto.has_periodic_trigger() {
                TriggerType::Periodic { interval_ms: trigger_proto.periodic_trigger().interval_ms }
            } else if trigger_proto.has_conditional_trigger() {
                let conditional_proto = trigger_proto.conditional_trigger();
                if conditional_proto.trigger_names.is_empty() {
                    return Err(TriggerFactoryError::NoParentTriggers);
                }
                let selector_node_index = conditional_proto
                    .selector_node_index
                    .ok_or(TriggerFactoryError::MissingNodeIndex)?;
                let selector_expression =
                    context.expression_nodes.get(selector_node_index as usize).ok_or(
                        TriggerFactoryError::IncorrectNodeIndex { node_index: selector_node_index },
                    )?;
                TriggerType::Conditional {
                    triggers: conditional_proto
                        .trigger_names
                        .iter()
                        .map(|name| Arc::downgrade(&context.config.triggers[name]))
                        .collect(),
                    selector_expression: self
                        .expression_factory
                        .create_from_node(selector_expression, context.expression_nodes),
                }
            } else {
                return Err(TriggerFactoryError::UnknownTriggerType);
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use sdvtelemetry_proto_rust::expression::{NewProtoNode, Node};
    use std::{collections::HashMap, sync::Mutex};

    use crate::metrics_config_orchestrator::{publisher::PublisherType, trigger::TriggerType};

    use super::*;

    struct MockExpressionFactory {
        nodes: Mutex<Vec<Node>>,
    }

    impl ExpressionFactory for MockExpressionFactory {
        fn create_from_node(&self, node: &Node, _other_nodes: &[Node]) -> ExpressionImpl {
            self.nodes.lock().unwrap().push(node.clone());
            ExpressionImpl {}
        }

        fn create_from_new_proto(
            &self,
            _node: &NewProtoNode,
            _other_nodes: &[Node],
        ) -> ExpressionImpl {
            ExpressionImpl {}
        }
    }

    fn create_factory() -> TriggerFactoryImpl<MockExpressionFactory> {
        TriggerFactoryImpl {
            expression_factory: MockExpressionFactory { nodes: Mutex::new(Vec::new()) },
        }
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
        init_nodes_func: Option<Box<dyn FnOnce(&mut Vec<Node>)>>,
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
        const TRIGGER_NAME: &str = "trigger";
        const PUBLISHER_NAME: &str = "publisher";
        const SIGNAL_NAME: &str = "signal";
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let mut data_trigger = trigger.mut_data_trigger();
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
                assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
            },
        );
    }

    #[test]
    fn create_periodic_trigger_success() {
        const TRIGGER_NAME: &str = "trigger";
        const INTERVAL: u64 = u64::MAX;
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let mut periodic_trigger = trigger.mut_periodic_trigger();
        periodic_trigger.interval_ms = INTERVAL;

        let res =
            run_using_empty_factory_context(|context| factory.create(&trigger, context)).unwrap();
        assert_eq!(res.name, TRIGGER_NAME);
        if let TriggerType::Periodic { interval_ms } = res.trigger_type {
            assert_eq!(interval_ms, INTERVAL);
        } else {
            panic!("Unexpected trigger type");
        }
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_conditional_trigger_no_parent_triggers_fails() {
        const TRIGGER_NAME: &str = "trigger";
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
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_conditional_trigger_missing_node_index_fails() {
        const TRIGGER_NAME: &str = "trigger";
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
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_conditional_trigger_incorrect_node_index_fails() {
        const TRIGGER_NAME: &str = "trigger";
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
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_conditional_trigger_success() {
        const TRIGGER_NAME: &str = "trigger";
        const PARENT_TRIGGER_NAME: &str = "parent_trigger";
        const INTERVAL: u64 = u64::MAX;
        let factory = create_factory();

        let mut trigger = new_trigger(TRIGGER_NAME);
        let mut conditional_trigger = trigger.mut_conditional_trigger();
        conditional_trigger.selector_node_index = Some(0);
        conditional_trigger.trigger_names.push(PARENT_TRIGGER_NAME.to_owned());

        let parent_trigger = Arc::new(Trigger {
            name: PARENT_TRIGGER_NAME.to_owned(),
            trigger_type: TriggerType::Periodic { interval_ms: INTERVAL },
        });
        run_using_factory_context(
            Some(Box::new(move |configs| {
                configs.triggers.insert(PARENT_TRIGGER_NAME.to_owned(), parent_trigger);
            })),
            Some(Box::new(|nodes| nodes.push(Node::new()))),
            |context| {
                let res = factory.create(&trigger, context).unwrap();
                assert_eq!(res.name, TRIGGER_NAME);
                if let TriggerType::Conditional { triggers, selector_expression: _ } =
                    res.trigger_type
                {
                    assert_eq!(triggers.len(), 1);
                    let parent_trigger = triggers[0].upgrade().unwrap();
                    assert_eq!(parent_trigger.name, PARENT_TRIGGER_NAME);
                    assert!(matches!(
                        parent_trigger.trigger_type,
                        TriggerType::Periodic { interval_ms: INTERVAL }
                    ));
                } else {
                    panic!("Unexpected trigger type");
                }
            },
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 1);
    }
}
