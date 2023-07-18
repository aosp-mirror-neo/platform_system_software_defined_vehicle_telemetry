// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use sdvtelemetry_proto_rust::{
    expression::Node,
    metrics_configuration::{
        declarative_aggregation::AggregationType,
        AggregationPublisher,
        {publisher::Publisher_type as PublisherTypeProto, Publisher as PublisherProto},
    },
};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Weak},
};
use thiserror::Error;

use crate::collection_extension::HashMapExtension;
use crate::expression_evaluation::{ExpressionFactory, ExpressionFactoryImpl, ExpressionImpl};
use crate::metrics_config_orchestrator::{trigger::Trigger, MetricsConfig};

pub trait MessageFactory {
    fn create(&self) -> Box<dyn protobuf::MessageDyn>;
}

pub trait PublisherFactory {
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext,
    ) -> Result<Publisher, PublisherFactoryError>;
}

#[allow(dead_code)]
pub struct DeclarativeAggregation {
    field_name: String,
    selector_expression: ExpressionImpl,
    aggregation_type: AggregationType,
    seed_expression: Option<ExpressionImpl>,
    aggregation_expression: Option<ExpressionImpl>,
}

pub struct MessageFactoryImpl {
    descriptor: MessageDescriptor,
}

pub struct Publisher {
    pub name: String,
    pub publisher_type: PublisherType,
}

pub enum PublisherType {
    Signal {
        signal_name: String,
    },
    Service {
        service_name: String,
    },
    Aggregation {
        triggers: Vec<Weak<Trigger>>,
        message_factory: MessageFactoryImpl,
        reset_on_get: bool,
        declarative_aggregations: Vec<DeclarativeAggregation>,
    },
}

pub struct PublisherFactoryContext<'a> {
    pub config: &'a MetricsConfig,
    pub expression_nodes: &'a [Node],
    pub file_descriptors: &'a Vec<FileDescriptor>,
}

#[derive(Error, Debug)]
pub enum PublisherFactoryError {
    #[error("Unknown message type {message_type} set for aggregation publisher")]
    UnknownMessageType { message_type: String },
    #[error("Field with name {field_name} doesn't exist in the message associated with aggregation publisher")]
    InvalidFieldName { field_name: String },
    #[error("Required field node_index is not set in declarative aggregation {field_name}")]
    MissingNodeIndex { field_name: String },
    #[error("Incorrect node index {node_index} in declarative aggregation {field_name}")]
    IncorrectNodeIndex { node_index: u32, field_name: String },
    #[error("Unknown aggregation type {aggregation_type} in declarative aggregation {field_name}")]
    UnknownAggregationType { aggregation_type: i32, field_name: String },
    #[error("Seed expression is not set in custom declarative aggregation {field_name}")]
    SeedExpressionNotSet { field_name: String },
    #[error("Aggregation expression is not set in custom declarative aggregation {field_name}")]
    AggregationExpressionNotSet { field_name: String },
    #[error("Field name {field_name} used more than once in declarative aggregation")]
    DuplicateFieldName { field_name: String },
    #[error("Unknown publisher type")]
    UnknownPublisherType,
}

pub struct PublisherFactoryImpl<ExprFactory: ExpressionFactory = ExpressionFactoryImpl> {
    expression_factory: ExprFactory,
}

impl PublisherFactoryImpl {
    pub fn new() -> Self {
        PublisherFactoryImpl { expression_factory: ExpressionFactoryImpl {} }
    }
}

impl<ExprFactory: ExpressionFactory> PublisherFactory for PublisherFactoryImpl<ExprFactory> {
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext,
    ) -> Result<Publisher, PublisherFactoryError> {
        Ok(Publisher {
            name: publisher_proto.name.clone(),
            publisher_type: {
                match &publisher_proto.publisher_type {
                    Some(PublisherTypeProto::SignalPublisher(signal_publisher)) => {
                        PublisherType::Signal { signal_name: signal_publisher.signal_name.clone() }
                    }
                    Some(PublisherTypeProto::ServicePublisher(service_publisher)) => {
                        PublisherType::Service {
                            service_name: service_publisher.service_name.clone(),
                        }
                    }
                    Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher)) => {
                        self.to_aggregation_publisher(aggregation_publisher, context)?
                    }
                    _ => return Err(PublisherFactoryError::UnknownPublisherType),
                }
            },
        })
    }
}

impl<ExprFactory: ExpressionFactory> PublisherFactoryImpl<ExprFactory> {
    fn to_aggregation_publisher(
        &self,
        aggregation_publisher: &AggregationPublisher,
        context: &PublisherFactoryContext,
    ) -> Result<PublisherType, PublisherFactoryError> {
        let descriptor = context
            .file_descriptors
            .iter()
            .find_map(|descriptor| {
                descriptor.message_by_full_name(&aggregation_publisher.message_type_name)
            })
            .ok_or_else(|| PublisherFactoryError::UnknownMessageType {
                message_type: aggregation_publisher.message_type_name.clone(),
            })?;
        let declarative_aggregations = aggregation_publisher
            .declarative_aggregations
            .iter()
            .try_fold(HashMap::new(), |mut acc, declarative_aggregation| {
                descriptor.field_by_name(&declarative_aggregation.field_name).ok_or(
                    PublisherFactoryError::InvalidFieldName {
                        field_name: declarative_aggregation.field_name.clone(),
                    },
                )?;
                let selector_node_index = declarative_aggregation.selector_node_index.ok_or(
                    PublisherFactoryError::MissingNodeIndex {
                        field_name: declarative_aggregation.field_name.clone(),
                    },
                )?;
                let selector_expression = context
                    .expression_nodes
                    .get(selector_node_index as usize)
                    .ok_or(PublisherFactoryError::IncorrectNodeIndex {
                        node_index: selector_node_index,
                        field_name: declarative_aggregation.field_name.clone(),
                    })?;
                let aggregation_type = declarative_aggregation
                    .aggregation_type
                    .enum_value()
                    .map_err(|value| PublisherFactoryError::UnknownAggregationType {
                        aggregation_type: value,
                        field_name: declarative_aggregation.field_name.clone(),
                    })?;

                let (seed_expression, aggregation_expression) =
                    if aggregation_type == AggregationType::CUSTOM {
                        if declarative_aggregation.seed_expression.is_none() {
                            return Err(PublisherFactoryError::SeedExpressionNotSet {
                                field_name: declarative_aggregation.field_name.clone(),
                            });
                        }
                        if declarative_aggregation.aggregation_expression.is_none() {
                            return Err(PublisherFactoryError::AggregationExpressionNotSet {
                                field_name: declarative_aggregation.field_name.clone(),
                            });
                        }
                        (
                            Some(self.expression_factory.create_from_new_proto(
                                &declarative_aggregation.seed_expression,
                                context.expression_nodes,
                            )),
                            Some(self.expression_factory.create_from_new_proto(
                                &declarative_aggregation.aggregation_expression,
                                context.expression_nodes,
                            )),
                        )
                    } else {
                        (None, None)
                    };

                acc.try_insert_value(
                    &declarative_aggregation.field_name,
                    DeclarativeAggregation {
                        field_name: declarative_aggregation.field_name.clone(),
                        selector_expression: self
                            .expression_factory
                            .create_from_node(selector_expression, context.expression_nodes),
                        aggregation_type,
                        seed_expression,
                        aggregation_expression,
                    },
                )
                .map_or_else(
                    |()| {
                        Err(PublisherFactoryError::DuplicateFieldName {
                            field_name: declarative_aggregation.field_name.clone(),
                        })
                    },
                    |()| Ok(acc),
                )
            })?;

        Ok(PublisherType::Aggregation {
            triggers: aggregation_publisher
                .trigger_names
                .iter()
                .map(|trigger_name| Arc::downgrade(&context.config.triggers[trigger_name]))
                .collect(),
            message_factory: MessageFactoryImpl { descriptor },
            reset_on_get: aggregation_publisher.reset_on_get,
            declarative_aggregations: declarative_aggregations.into_values().collect(),
        })
    }
}

impl MessageFactory for MessageFactoryImpl {
    fn create(&self) -> Box<dyn protobuf::MessageDyn> {
        self.descriptor.new_instance()
    }
}

#[cfg(test)]
mod tests {
    use protobuf::{
        descriptor::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto},
        EnumOrUnknown, MessageField,
    };
    use sdvtelemetry_proto_rust::{
        expression::{NewProtoNode, Node},
        metrics_configuration::{
            AggregationPublisher as AggregationPublisherProto,
            DeclarativeAggregation as DeclarativeAggregationProto, Publisher as PublisherProto,
        },
    };
    use std::{collections::HashMap, sync::Mutex};

    use crate::metrics_config_orchestrator::trigger::TriggerType;

    use super::*;

    const PUBLISHER_NAME: &str = "publisher";
    const FIELD_NAME: &str = "field";

    struct MockExpressionFactory {
        nodes: Mutex<Vec<Node>>,
        new_proto_nodes: Mutex<Vec<NewProtoNode>>,
    }

    impl ExpressionFactory for MockExpressionFactory {
        fn create_from_node(&self, node: &Node, _other_nodes: &[Node]) -> ExpressionImpl {
            self.nodes.lock().unwrap().push(node.clone());
            ExpressionImpl {}
        }

        fn create_from_new_proto(
            &self,
            node: &NewProtoNode,
            _other_nodes: &[Node],
        ) -> ExpressionImpl {
            self.new_proto_nodes.lock().unwrap().push(node.clone());
            ExpressionImpl {}
        }
    }

    fn create_factory() -> PublisherFactoryImpl<MockExpressionFactory> {
        PublisherFactoryImpl {
            expression_factory: MockExpressionFactory {
                nodes: Mutex::new(Vec::new()),
                new_proto_nodes: Mutex::new(Vec::new()),
            },
        }
    }

    fn create_file_descriptor(field_names: &[&str]) -> FileDescriptor {
        let mut descriptor_proto = DescriptorProto::new();
        descriptor_proto.name = Some("TestMessage".to_owned());
        for (i, field_name) in field_names.iter().enumerate() {
            let mut field_proto = FieldDescriptorProto::new();
            field_proto.set_name(field_name.to_string());
            field_proto.set_number(i as i32);
            descriptor_proto.field.push(field_proto);
        }
        let mut file_descriptor_proto = FileDescriptorProto::new();
        file_descriptor_proto.package = Some("android.sdv.telemetry.tests".to_owned());
        file_descriptor_proto.message_type.push(descriptor_proto);

        FileDescriptor::new_dynamic(file_descriptor_proto, &[]).unwrap()
    }

    fn new_declarative_aggregation(
        field_name: &str,
        selector_node_index: Option<u32>,
        aggregation_type: Option<AggregationType>,
    ) -> DeclarativeAggregationProto {
        let mut declarative_proto = DeclarativeAggregationProto::new();
        declarative_proto.field_name = field_name.to_owned();
        declarative_proto.selector_node_index = selector_node_index;
        if let Some(type_value) = aggregation_type {
            declarative_proto.aggregation_type = type_value.into();
        }
        declarative_proto
    }

    fn new_publisher(name: &str) -> PublisherProto {
        let mut publisher = PublisherProto::new();
        publisher.name = name.to_owned();
        publisher
    }

    fn set_aggregation_publisher<'a>(
        publisher: &'a mut PublisherProto,
        message_descriptor: &MessageDescriptor,
        declarative_aggregations: Vec<DeclarativeAggregationProto>,
    ) -> &'a mut AggregationPublisherProto {
        let mut aggregation_proto = publisher.mut_aggregation_publisher();
        aggregation_proto.message_type_name = to_full_name(message_descriptor);
        aggregation_proto.declarative_aggregations = declarative_aggregations;
        aggregation_proto
    }

    fn to_full_name(descriptor_proto: &MessageDescriptor) -> String {
        let mut full_name = descriptor_proto.full_name().to_owned();
        full_name.insert(0, '.');
        full_name
    }

    fn run_using_empty_factory_context<R, F: FnOnce(&PublisherFactoryContext) -> R>(
        execute_func: F,
    ) -> R {
        run_using_factory_context(None, None, None, execute_func)
    }

    fn run_using_factory_context<R, FE>(
        init_metrics_config_func: Option<Box<dyn FnOnce(&mut MetricsConfig)>>,
        init_nodes_func: Option<Box<dyn FnOnce(&mut Vec<Node>)>>,
        init_file_descriptors_func: Option<Box<dyn FnOnce(&mut Vec<FileDescriptor>)>>,
        execute_func: FE,
    ) -> R
    where
        FE: FnOnce(&PublisherFactoryContext) -> R,
    {
        let mut config = MetricsConfig { publishers: HashMap::new(), triggers: HashMap::new() };
        let mut expression_nodes = vec![];
        let mut file_descriptors = vec![];
        if let Some(func) = init_metrics_config_func {
            func(&mut config);
        }
        if let Some(func) = init_nodes_func {
            func(&mut expression_nodes);
        }
        if let Some(func) = init_file_descriptors_func {
            func(&mut file_descriptors);
        }
        let context = PublisherFactoryContext {
            config: &config,
            expression_nodes: &expression_nodes,
            file_descriptors: &file_descriptors,
        };
        execute_func(&context)
    }

    #[test]
    fn create_signal_publisher_success() {
        const SIGNAL_NAME: &str = "signal";
        let factory = create_factory();

        let mut publisher = new_publisher(PUBLISHER_NAME);
        let mut signal_publisher = publisher.mut_signal_publisher();
        signal_publisher.signal_name = SIGNAL_NAME.to_owned();

        let res =
            run_using_empty_factory_context(|context| factory.create(&publisher, context).unwrap());

        assert_eq!(res.name, PUBLISHER_NAME);
        if let PublisherType::Signal { signal_name } = res.publisher_type {
            assert_eq!(signal_name, SIGNAL_NAME);
        } else {
            panic!("Unexpected publisher type");
        }
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_service_publisher_success() {
        const SERVICE_NAME: &str = "service";
        let factory = create_factory();

        let mut publisher = new_publisher(PUBLISHER_NAME);
        let mut service_publisher = publisher.mut_service_publisher();
        service_publisher.service_name = SERVICE_NAME.to_owned();

        let res =
            run_using_empty_factory_context(|context| factory.create(&publisher, context).unwrap());

        assert_eq!(res.name, PUBLISHER_NAME);
        if let PublisherType::Service { service_name } = res.publisher_type {
            assert_eq!(service_name, SERVICE_NAME);
        } else {
            panic!("Unexpected publisher type");
        }
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_unknown_message_type_fails() {
        const MESSAGE_TYPE: &str = "messagetype";
        let factory = create_factory();

        let mut publisher = new_publisher(PUBLISHER_NAME);
        publisher.mut_aggregation_publisher().message_type_name = MESSAGE_TYPE.to_owned();

        let err = run_using_empty_factory_context(|context| factory.create(&publisher, context))
            .err()
            .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::UnknownMessageType { ref message_type } if message_type == MESSAGE_TYPE),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_invalid_field_name_fails() {
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let declarative_proto = new_declarative_aggregation(FIELD_NAME, None, None);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            None,
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::InvalidFieldName { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_missing_node_index_fails() {
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let declarative_proto = new_declarative_aggregation(FIELD_NAME, None, None);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            None,
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::MissingNodeIndex { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_incorrect_node_index_fails() {
        const NODE_INDEX: u32 = 42;
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let declarative_proto = new_declarative_aggregation(FIELD_NAME, Some(NODE_INDEX), None);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            None,
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::IncorrectNodeIndex { node_index:NODE_INDEX, ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_unknown_aggregation_type_fails() {
        const UNKNOWN_AGGREGATION_TYPE: i32 = i32::MAX;
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let mut declarative_proto = new_declarative_aggregation(FIELD_NAME, Some(0), None);
        declarative_proto.aggregation_type =
            EnumOrUnknown::<AggregationType>::from_i32(UNKNOWN_AGGREGATION_TYPE);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            Some(Box::new(|nodes| nodes.push(Node::new()))),
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::UnknownAggregationType { aggregation_type:UNKNOWN_AGGREGATION_TYPE, ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_duplicate_field_name_fails() {
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let declarative_proto1 =
            new_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::COUNT));
        let declarative_proto2 =
            new_declarative_aggregation(FIELD_NAME, Some(1), Some(AggregationType::MAX));
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto1, declarative_proto2],
        );

        let err = run_using_factory_context(
            None,
            Some(Box::new(|nodes| {
                nodes.push(Node::new());
                nodes.push(Node::new());
            })),
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::DuplicateFieldName { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 2);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_success() {
        const FIELD_NAME1: &str = FIELD_NAME;
        const FIELD_NAME2: &str = "field2";
        const TRIGGER_NAME: &str = "trigger";
        const INTERVAL_MS: u64 = 42;
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME1, FIELD_NAME2]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let declarative_proto1 =
            new_declarative_aggregation(FIELD_NAME1, Some(0), Some(AggregationType::COUNT));
        let declarative_proto2 =
            new_declarative_aggregation(FIELD_NAME2, Some(1), Some(AggregationType::VECTOR));
        let mut aggregation_proto = set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto1, declarative_proto2],
        );
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());
        aggregation_proto.reset_on_get = true;

        let trigger = Arc::new(Trigger {
            name: TRIGGER_NAME.to_owned(),
            trigger_type: TriggerType::Periodic { interval_ms: INTERVAL_MS },
        });
        run_using_factory_context(
            Some(Box::new(|configs| {
                configs.triggers.insert(TRIGGER_NAME.to_owned(), trigger);
            })),
            Some(Box::new(|nodes| {
                nodes.push(Node::new());
                nodes.push(Node::new());
            })),
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| {
                let res = factory.create(&publisher, context).unwrap();
                assert_eq!(res.name, PUBLISHER_NAME);
                if let PublisherType::Aggregation {
                    triggers,
                    message_factory: _,
                    reset_on_get,
                    declarative_aggregations,
                } = res.publisher_type
                {
                    assert_eq!(triggers.len(), 1);
                    let trigger = triggers.first().unwrap().upgrade().unwrap();
                    assert_eq!(trigger.name, TRIGGER_NAME);
                    assert!(matches!(
                        trigger.trigger_type,
                        TriggerType::Periodic { interval_ms: INTERVAL_MS }
                    ));
                    assert!(reset_on_get);
                    assert_eq!(declarative_aggregations.len(), 2);
                    for aggregation in declarative_aggregations {
                        match aggregation.field_name.as_str() {
                            FIELD_NAME1 => assert!(matches!(
                                aggregation.aggregation_type,
                                AggregationType::COUNT
                            )),
                            FIELD_NAME2 => assert!(matches!(
                                aggregation.aggregation_type,
                                AggregationType::VECTOR
                            )),
                            name => panic!("Unexpected field with name {}", name),
                        }
                    }
                } else {
                    panic!("Unexpected publisher type");
                }
            },
        );

        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 2);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_no_custom_expressions_expressions_not_returned(
    ) {
        const TRIGGER_NAME: &str = "trigger";
        const INTERVAL_MS: u64 = 42;
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let declarative_proto =
            new_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::AVG));
        let aggregation_proto = set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());

        let trigger = Arc::new(Trigger {
            name: TRIGGER_NAME.to_owned(),
            trigger_type: TriggerType::Periodic { interval_ms: INTERVAL_MS },
        });
        let res = run_using_factory_context(
            Some(Box::new(|configs| {
                configs.triggers.insert(TRIGGER_NAME.to_owned(), trigger);
            })),
            Some(Box::new(|nodes| nodes.push(Node::new()))),
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .unwrap();

        if let PublisherType::Aggregation { declarative_aggregations, .. } = res.publisher_type {
            assert_eq!(declarative_aggregations.len(), 1);
            let aggregation = &declarative_aggregations[0];
            assert!(aggregation.seed_expression.is_none());
            assert!(aggregation.aggregation_expression.is_none());
        } else {
            panic!("Unexpected publisher type");
        }

        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 1);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_custom_expressions_no_seed_expression_fails(
    ) {
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let declarative_proto =
            new_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::CUSTOM));
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            Some(Box::new(|nodes| {
                nodes.push(Node::new());
            })),
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::SeedExpressionNotSet { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_custom_expressions_no_aggregation_expression_fails(
    ) {
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let mut declarative_proto =
            new_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::CUSTOM));
        declarative_proto.seed_expression = MessageField::some(NewProtoNode::new());
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            Some(Box::new(|nodes| {
                nodes.push(Node::new());
            })),
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::AggregationExpressionNotSet { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 0);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 0);
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_custom_expressions_expressions_returned(
    ) {
        const TRIGGER_NAME: &str = "trigger";
        const INTERVAL_MS: u64 = 42;
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = new_publisher(PUBLISHER_NAME);
        let mut declarative_proto =
            new_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::CUSTOM));
        declarative_proto.seed_expression = MessageField::some(NewProtoNode::new());
        declarative_proto.aggregation_expression = MessageField::some(NewProtoNode::new());
        let aggregation_proto = set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());

        let trigger = Arc::new(Trigger {
            name: TRIGGER_NAME.to_owned(),
            trigger_type: TriggerType::Periodic { interval_ms: INTERVAL_MS },
        });
        let res = run_using_factory_context(
            Some(Box::new(|configs| {
                configs.triggers.insert(TRIGGER_NAME.to_owned(), trigger);
            })),
            Some(Box::new(|nodes| nodes.push(Node::new()))),
            Some(Box::new(|file_descriptors| file_descriptors.push(file_descriptor))),
            |context| factory.create(&publisher, context),
        )
        .unwrap();

        if let PublisherType::Aggregation { declarative_aggregations, .. } = res.publisher_type {
            assert_eq!(declarative_aggregations.len(), 1);
            let aggregation = &declarative_aggregations[0];
            assert!(aggregation.seed_expression.is_some());
            assert!(aggregation.aggregation_expression.is_some());
        } else {
            panic!("Unexpected publisher type");
        }

        assert_eq!(factory.expression_factory.nodes.lock().unwrap().len(), 1);
        assert_eq!(factory.expression_factory.new_proto_nodes.lock().unwrap().len(), 2);
    }
}
