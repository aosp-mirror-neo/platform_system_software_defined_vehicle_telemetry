// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use sdv_ast::Node;
use sdvtelemetry_proto_rust::metrics_configuration::{
    declarative_aggregation::AggregationType,
    AggregationPublisher as AggregationPublisherProto,
    DeclarativeAggregation as DeclarativeAggregationProto,
    {publisher::Publisher_type as PublisherTypeProto, Publisher as PublisherProto},
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::collection_extension::HashMapExtension;
use crate::metrics_config_orchestrator::{
    publisher::{DeclarativeAggregation, PublisherType},
    MetricsConfig, Publisher,
};

pub trait PublisherFactory {
    /// Returns a new publisher encapsulated object based on the protobuf message.
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext,
    ) -> Result<Publisher, PublisherFactoryError>;

    /// Verifies that the protobuf message is valid and can be used to create a publisher
    /// encapsulated object.
    fn validate(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext,
    ) -> Result<(), PublisherFactoryError>;
}

pub struct PublisherFactoryContext<'a> {
    pub config: &'a MetricsConfig,
    pub expression_nodes: &'a Vec<Arc<Node>>,
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

pub struct PublisherFactoryImpl {}

impl PublisherFactoryImpl {
    pub fn new() -> Self {
        PublisherFactoryImpl {}
    }
}

impl PublisherFactoryImpl {
    /// Returns the first descriptor from the vector that has the specified type name.
    fn get_message_descriptor(
        &self,
        message_type_name: &str,
        file_descriptors: &[FileDescriptor],
    ) -> Result<MessageDescriptor, PublisherFactoryError> {
        file_descriptors
            .iter()
            .find_map(|descriptor| descriptor.message_by_full_name(message_type_name))
            .ok_or_else(|| PublisherFactoryError::UnknownMessageType {
                message_type: message_type_name.to_owned(),
            })
    }

    /// Creates Aggregation Publisher struct based on the protobuf message and context.
    fn create_aggregation_publisher(
        &self,
        aggregation_publisher_proto: &AggregationPublisherProto,
        context: &PublisherFactoryContext,
    ) -> Result<PublisherType, PublisherFactoryError> {
        let descriptor = self.get_message_descriptor(
            &aggregation_publisher_proto.message_type_name,
            context.file_descriptors,
        )?;
        let declarative_aggregations = self.create_declarative_aggregations(
            &aggregation_publisher_proto.declarative_aggregations,
            &descriptor,
            context.expression_nodes,
        )?;
        Ok(PublisherType::Aggregation {
            triggers: aggregation_publisher_proto
                .trigger_names
                .iter()
                .map(|trigger_name| Arc::downgrade(&context.config.triggers[trigger_name]))
                .collect(),
            message_descriptor: descriptor,
            reset_on_get: aggregation_publisher_proto.reset_on_get,
            declarative_aggregations,
        })
    }

    /// Creates a vector of Declarative Aggregation structs based on the protobuf message and
    /// contextual information.
    fn create_declarative_aggregations(
        &self,
        declarative_aggregation_protos: &[DeclarativeAggregationProto],
        descriptor: &MessageDescriptor,
        expression_nodes: &[Arc<Node>],
    ) -> Result<Vec<DeclarativeAggregation>, PublisherFactoryError> {
        Ok(declarative_aggregation_protos
            .iter()
            .try_fold(HashMap::new(), |mut acc, declarative_aggregation_proto| {
                descriptor.field_by_name(&declarative_aggregation_proto.field_name).ok_or(
                    PublisherFactoryError::InvalidFieldName {
                        field_name: declarative_aggregation_proto.field_name.clone(),
                    },
                )?;
                let selector_node_index = declarative_aggregation_proto.selector_node_index.ok_or(
                    PublisherFactoryError::MissingNodeIndex {
                        field_name: declarative_aggregation_proto.field_name.clone(),
                    },
                )?;
                let selector_expression = expression_nodes
                    .get(selector_node_index as usize)
                    .ok_or(PublisherFactoryError::IncorrectNodeIndex {
                        node_index: selector_node_index,
                        field_name: declarative_aggregation_proto.field_name.clone(),
                    })?;
                let aggregation_type = declarative_aggregation_proto
                    .aggregation_type
                    .enum_value()
                    .map_err(|value| PublisherFactoryError::UnknownAggregationType {
                        aggregation_type: value,
                        field_name: declarative_aggregation_proto.field_name.clone(),
                    })?;

                let (seed_expression, aggregation_expression) = if aggregation_type
                    == AggregationType::CUSTOM
                {
                    if declarative_aggregation_proto.seed_expression.is_none() {
                        return Err(PublisherFactoryError::SeedExpressionNotSet {
                            field_name: declarative_aggregation_proto.field_name.clone(),
                        });
                    }
                    if declarative_aggregation_proto.aggregation_expression.is_none() {
                        return Err(PublisherFactoryError::AggregationExpressionNotSet {
                            field_name: declarative_aggregation_proto.field_name.clone(),
                        });
                    }
                    (
                        Some(declarative_aggregation_proto.seed_expression.clone().unwrap()),
                        Some(declarative_aggregation_proto.aggregation_expression.clone().unwrap()),
                    )
                } else {
                    (None, None)
                };

                acc.try_insert_value(
                    &declarative_aggregation_proto.field_name,
                    DeclarativeAggregation {
                        field_name: declarative_aggregation_proto.field_name.clone(),
                        selector_expression: selector_expression.clone(),
                        aggregation_type,
                        seed_expression,
                        aggregation_expression,
                    },
                )
                .map_or_else(
                    |()| {
                        Err(PublisherFactoryError::DuplicateFieldName {
                            field_name: declarative_aggregation_proto.field_name.clone(),
                        })
                    },
                    |()| Ok(acc),
                )
            })?
            .into_values()
            .collect())
    }
}

impl PublisherFactory for PublisherFactoryImpl {
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
                        self.create_aggregation_publisher(aggregation_publisher, context)?
                    }
                    _ => return Err(PublisherFactoryError::UnknownPublisherType),
                }
            },
        })
    }

    fn validate(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext,
    ) -> Result<(), PublisherFactoryError> {
        match &publisher_proto.publisher_type {
            Some(PublisherTypeProto::SignalPublisher(_))
            | Some(PublisherTypeProto::ServicePublisher(_)) => (),
            Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher)) => {
                let descriptor = self.get_message_descriptor(
                    &aggregation_publisher.message_type_name,
                    context.file_descriptors,
                )?;
                self.create_declarative_aggregations(
                    &aggregation_publisher.declarative_aggregations,
                    &descriptor,
                    context.expression_nodes,
                )?;
            }
            _ => return Err(PublisherFactoryError::UnknownPublisherType),
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use protobuf::{
        descriptor::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto},
        EnumOrUnknown, MessageField,
    };
    use sdv_ast::NodeValue;
    use sdvtelemetry_proto_rust::{
        expression::NewProtoNode,
        metrics_configuration::{
            AggregationPublisher as AggregationPublisherProto,
            DeclarativeAggregation as DeclarativeAggregationProto, Publisher as PublisherProto,
        },
    };
    use std::collections::HashMap;

    use super::*;
    use crate::collection_extension::VecExtension;
    use crate::metrics_config_orchestrator::{trigger::TriggerType, Trigger};

    const FIELD_NAME: &str = "field";
    const FIELD_NAME2: &str = "field2";
    const INCORRECT_NODE_INDEX: u32 = 42;
    const INTERVAL_MS: u64 = 42;
    const MESSAGE_TYPE: &str = "messagetype";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";
    const SIGNAL_NAME: &str = "signal";
    const TRIGGER_NAME: &str = "trigger";
    const UNKNOWN_AGGREGATION_TYPE: i32 = i32::MAX;

    fn create_factory() -> PublisherFactoryImpl {
        PublisherFactoryImpl {}
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

    fn create_declarative_aggregation(
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

    fn create_publisher(name: &str) -> PublisherProto {
        let mut publisher = PublisherProto::new();
        publisher.name = name.to_owned();
        publisher
    }

    fn run_using_empty_factory_context<R, F: FnOnce(&PublisherFactoryContext) -> R>(
        execute_func: F,
    ) -> R {
        run_using_factory_context(None, None, None, None, execute_func)
    }

    // Creates a PublisherFactoryContext with passed data and uses it to call `execute_func`.
    // This is needed because PublisherFactoryContext contains borrows and it's impossible to have
    // a function that creates and returns it - it can only be used in the function where it's
    // created.
    fn run_using_factory_context<R, FE>(
        publishers: Option<Vec<Arc<Publisher>>>,
        triggers: Option<Vec<Arc<Trigger>>>,
        nodes: Option<Vec<Arc<Node>>>,
        file_descriptors: Option<Vec<FileDescriptor>>,
        execute_func: FE,
    ) -> R
    where
        FE: FnOnce(&PublisherFactoryContext) -> R,
    {
        let config = MetricsConfig {
            publishers: publishers
                .map_or(HashMap::new(), |val| val.into_hash_map(|trigger| trigger.name.clone())),
            triggers: triggers.map_or(HashMap::new(), |val| {
                val.into_hash_map(|publisher| publisher.name.clone())
            }),
        };
        let expression_nodes = nodes.unwrap_or_default();
        let file_descriptors = file_descriptors.unwrap_or_default();
        let context = PublisherFactoryContext {
            config: &config,
            expression_nodes: &expression_nodes,
            file_descriptors: &file_descriptors,
        };
        execute_func(&context)
    }

    fn set_aggregation_publisher<'a>(
        publisher: &'a mut PublisherProto,
        message_descriptor: &MessageDescriptor,
        declarative_aggregations: Vec<DeclarativeAggregationProto>,
    ) -> &'a mut AggregationPublisherProto {
        let aggregation_proto = publisher.mut_aggregation_publisher();
        aggregation_proto.message_type_name = to_full_name(message_descriptor);
        aggregation_proto.declarative_aggregations = declarative_aggregations;
        aggregation_proto
    }

    fn to_full_name(descriptor_proto: &MessageDescriptor) -> String {
        let mut full_name = descriptor_proto.full_name().to_owned();
        full_name.insert(0, '.');
        full_name
    }

    fn create_node() -> Node {
        Node::ConstantLeaf { value: NodeValue::Bool(false) }
    }

    fn create_valid_service_publisher() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let service_publisher = publisher.mut_service_publisher();
        service_publisher.service_name = SERVICE_NAME.to_owned();
        publisher
    }

    fn create_valid_signal_publisher() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let signal_publisher = publisher.mut_signal_publisher();
        signal_publisher.signal_name = SIGNAL_NAME.to_owned();
        publisher
    }

    fn create_aggregation_publisher_with_unknown_message_type() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        publisher.mut_aggregation_publisher().message_type_name = MESSAGE_TYPE.to_owned();
        publisher
    }

    fn create_aggregation_publisher_with_invalid_field_name(
    ) -> (PublisherProto, Option<Vec<FileDescriptor>>) {
        let file_descriptor = create_file_descriptor(&[]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let declarative_proto = create_declarative_aggregation(FIELD_NAME, None, None);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        (publisher, Some(vec![file_descriptor]))
    }

    fn create_aggregation_publisher_with_missing_node_index(
    ) -> (PublisherProto, Option<Vec<FileDescriptor>>) {
        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let declarative_proto = create_declarative_aggregation(FIELD_NAME, None, None);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        (publisher, Some(vec![file_descriptor]))
    }

    fn create_aggregation_publisher_with_incorrect_node_index(
    ) -> (PublisherProto, Option<Vec<FileDescriptor>>) {
        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let declarative_proto =
            create_declarative_aggregation(FIELD_NAME, Some(INCORRECT_NODE_INDEX), None);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        (publisher, Some(vec![file_descriptor]))
    }

    fn create_aggregation_publisher_with_unknown_aggregation_type(
    ) -> (PublisherProto, Option<Vec<FileDescriptor>>) {
        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let mut declarative_proto = create_declarative_aggregation(FIELD_NAME, Some(0), None);
        declarative_proto.aggregation_type =
            EnumOrUnknown::<AggregationType>::from_i32(UNKNOWN_AGGREGATION_TYPE);
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        (publisher, Some(vec![file_descriptor]))
    }

    fn create_aggregation_publisher_with_duplicate_field_name(
    ) -> (PublisherProto, Option<Vec<FileDescriptor>>) {
        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let declarative_proto1 =
            create_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::COUNT));
        let declarative_proto2 =
            create_declarative_aggregation(FIELD_NAME, Some(1), Some(AggregationType::MAX));
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto1, declarative_proto2],
        );
        (publisher, Some(vec![file_descriptor]))
    }

    fn create_valid_aggregation_publisher(
    ) -> (PublisherProto, Option<Vec<FileDescriptor>>, Option<Vec<Arc<Trigger>>>) {
        let file_descriptor = create_file_descriptor(&[FIELD_NAME, FIELD_NAME2]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let declarative_proto1 =
            create_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::COUNT));
        let declarative_proto2 =
            create_declarative_aggregation(FIELD_NAME2, Some(1), Some(AggregationType::VECTOR));
        let aggregation_proto = set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto1, declarative_proto2],
        );
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());
        aggregation_proto.reset_on_get = true;

        let trigger = Arc::new(Trigger::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL_MS },
        ));

        (publisher, Some(vec![file_descriptor]), Some(vec![trigger]))
    }

    #[test]
    fn create_signal_publisher_success() {
        let factory = create_factory();
        let publisher = create_valid_signal_publisher();

        let res =
            run_using_empty_factory_context(|context| factory.create(&publisher, context)).unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        if let PublisherType::Signal { signal_name } = res.publisher_type {
            assert_eq!(signal_name, SIGNAL_NAME);
        } else {
            panic!("Unexpected publisher type");
        }
    }

    #[test]
    fn create_service_publisher_success() {
        let factory = create_factory();
        let publisher = create_valid_service_publisher();

        let res =
            run_using_empty_factory_context(|context| factory.create(&publisher, context)).unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        if let PublisherType::Service { service_name } = res.publisher_type {
            assert_eq!(service_name, SERVICE_NAME);
        } else {
            panic!("Unexpected publisher type");
        }
    }

    #[test]
    fn create_aggregation_publisher_unknown_message_type_fails() {
        let factory = create_factory();
        let publisher = create_aggregation_publisher_with_unknown_message_type();

        let err = run_using_empty_factory_context(|context| factory.create(&publisher, context))
            .err()
            .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::UnknownMessageType { ref message_type } if message_type == MESSAGE_TYPE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_invalid_field_name_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) = create_aggregation_publisher_with_invalid_field_name();

        let err = run_using_factory_context(None, None, None, file_descriptors, |context| {
            factory.create(&publisher, context)
        })
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::InvalidFieldName { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_missing_node_index_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) = create_aggregation_publisher_with_missing_node_index();

        let err = run_using_factory_context(None, None, None, file_descriptors, |context| {
            factory.create(&publisher, context)
        })
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::MissingNodeIndex { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_incorrect_node_index_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) =
            create_aggregation_publisher_with_incorrect_node_index();

        let err = run_using_factory_context(None, None, None, file_descriptors, |context| {
            factory.create(&publisher, context)
        })
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::IncorrectNodeIndex { node_index:INCORRECT_NODE_INDEX, ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_unknown_aggregation_type_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) =
            create_aggregation_publisher_with_unknown_aggregation_type();

        let err = run_using_factory_context(
            None,
            None,
            Some(vec![Arc::new(create_node())]),
            file_descriptors,
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::UnknownAggregationType { aggregation_type:UNKNOWN_AGGREGATION_TYPE, ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_duplicate_field_name_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) =
            create_aggregation_publisher_with_duplicate_field_name();

        let err = run_using_factory_context(
            None,
            None,
            Some(vec![Arc::new(create_node()), Arc::new(create_node())]),
            file_descriptors,
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::DuplicateFieldName { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_success() {
        let factory = create_factory();
        let (publisher, file_descriptors, triggers) = create_valid_aggregation_publisher();

        run_using_factory_context(
            None,
            triggers,
            Some(vec![Arc::new(create_node()), Arc::new(create_node())]),
            file_descriptors,
            |context| {
                let res = factory.create(&publisher, context).unwrap();
                assert_eq!(res.name, PUBLISHER_NAME);
                if let PublisherType::Aggregation {
                    triggers,
                    message_descriptor,
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
                    assert_eq!(
                        message_descriptor.full_name(),
                        "android.sdv.telemetry.tests.TestMessage"
                    );
                    assert!(reset_on_get);
                    assert_eq!(declarative_aggregations.len(), 2);
                    for aggregation in declarative_aggregations {
                        match aggregation.field_name.as_str() {
                            FIELD_NAME => assert!(matches!(
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
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_no_custom_expressions_expressions_not_returned(
    ) {
        const TRIGGER_NAME: &str = "trigger";
        const INTERVAL_MS: u64 = 42;
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let declarative_proto =
            create_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::AVG));
        let aggregation_proto = set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());

        let trigger = Arc::new(Trigger::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL_MS },
        ));
        let res = run_using_factory_context(
            None,
            Some(vec![trigger]),
            Some(vec![Arc::new(create_node())]),
            Some(vec![file_descriptor]),
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
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_custom_expressions_no_seed_expression_fails(
    ) {
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let declarative_proto =
            create_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::CUSTOM));
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            None,
            Some(vec![Arc::new(create_node())]),
            Some(vec![file_descriptor]),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::SeedExpressionNotSet { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_custom_expressions_no_aggregation_expression_fails(
    ) {
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let mut declarative_proto =
            create_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::CUSTOM));
        declarative_proto.seed_expression = MessageField::some(NewProtoNode::new());
        set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );

        let err = run_using_factory_context(
            None,
            None,
            Some(vec![Arc::new(create_node())]),
            Some(vec![file_descriptor]),
            |context| factory.create(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::AggregationExpressionNotSet { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_aggregation_publisher_declarative_aggregation_custom_expressions_expressions_returned(
    ) {
        const TRIGGER_NAME: &str = "trigger";
        const INTERVAL_MS: u64 = 42;
        let factory = create_factory();

        let file_descriptor = create_file_descriptor(&[FIELD_NAME]);
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let mut declarative_proto =
            create_declarative_aggregation(FIELD_NAME, Some(0), Some(AggregationType::CUSTOM));
        declarative_proto.seed_expression = MessageField::some(NewProtoNode::new());
        declarative_proto.aggregation_expression = MessageField::some(NewProtoNode::new());

        let aggregation_proto = set_aggregation_publisher(
            &mut publisher,
            &file_descriptor.messages().next().unwrap(),
            vec![declarative_proto],
        );
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());

        let trigger = Arc::new(Trigger::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL_MS },
        ));
        let res = run_using_factory_context(
            None,
            Some(vec![trigger]),
            Some(vec![Arc::new(create_node())]),
            Some(vec![file_descriptor]),
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
    }

    #[test]
    fn validate_signal_publisher_success() {
        let factory = create_factory();
        let publisher = create_valid_signal_publisher();

        run_using_empty_factory_context(|context| factory.validate(&publisher, context)).unwrap();
    }

    #[test]
    fn validate_service_publisher_success() {
        let factory = create_factory();
        let publisher = create_valid_service_publisher();

        run_using_empty_factory_context(|context| factory.validate(&publisher, context)).unwrap();
    }

    #[test]
    fn validate_aggregation_publisher_unknown_message_type_fails() {
        let factory = create_factory();
        let publisher = create_aggregation_publisher_with_unknown_message_type();

        let err = run_using_empty_factory_context(|context| factory.validate(&publisher, context))
            .err()
            .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::UnknownMessageType { ref message_type } if message_type == MESSAGE_TYPE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_invalid_field_name_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) = create_aggregation_publisher_with_invalid_field_name();

        let err = run_using_factory_context(None, None, None, file_descriptors, |context| {
            factory.validate(&publisher, context)
        })
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::InvalidFieldName { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_missing_node_index_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) = create_aggregation_publisher_with_missing_node_index();

        let err = run_using_factory_context(None, None, None, file_descriptors, |context| {
            factory.validate(&publisher, context)
        })
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::MissingNodeIndex { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_incorrect_node_index_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) =
            create_aggregation_publisher_with_incorrect_node_index();

        let err = run_using_factory_context(None, None, None, file_descriptors, |context| {
            factory.validate(&publisher, context)
        })
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::IncorrectNodeIndex { node_index:INCORRECT_NODE_INDEX, ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_unknown_aggregation_type_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) =
            create_aggregation_publisher_with_unknown_aggregation_type();

        let err = run_using_factory_context(
            None,
            None,
            Some(vec![Arc::new(create_node())]),
            file_descriptors,
            |context| factory.validate(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::UnknownAggregationType { aggregation_type:UNKNOWN_AGGREGATION_TYPE, ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_duplicate_field_name_fails() {
        let factory = create_factory();
        let (publisher, file_descriptors) =
            create_aggregation_publisher_with_duplicate_field_name();

        let err = run_using_factory_context(
            None,
            None,
            Some(vec![Arc::new(create_node()), Arc::new(create_node())]),
            file_descriptors,
            |context| factory.validate(&publisher, context),
        )
        .err()
        .unwrap();

        assert!(
            matches!(err, PublisherFactoryError::DuplicateFieldName { ref field_name } if field_name == FIELD_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_success() {
        let factory = create_factory();
        let (publisher, file_descriptors, triggers) = create_valid_aggregation_publisher();

        run_using_factory_context(
            None,
            triggers,
            Some(vec![Arc::new(create_node()), Arc::new(create_node())]),
            file_descriptors,
            |context| factory.validate(&publisher, context),
        )
        .unwrap();
    }
}
