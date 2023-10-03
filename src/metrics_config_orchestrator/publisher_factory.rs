// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use sdv_ast::Node;
use sdvtelemetry_proto_rust::metrics_configuration::{
    AggregationPublisher as AggregationPublisherProto,
    DeclarativeAggregation as DeclarativeAggregationProto,
    {publisher::Publisher_type as PublisherTypeProto, Publisher as PublisherProto},
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::collection_extension::HashMapExtension;
use crate::metrics_config_orchestrator::{
    publisher::{
        service_adapter::{ServiceAdapter, ServiceAdapterImpl},
        DeclarativeAggregation, PublisherType,
    },
    Publisher, PublisherImpl, Trigger, TriggerImpl,
};

pub(crate) trait PublisherFactory<
    TPublisher: Publisher = PublisherImpl,
    TTrigger: Trigger = TriggerImpl,
>
{
    /// Returns a new publisher encapsulated object based on the protobuf message.
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext<TTrigger>,
    ) -> Result<Arc<TPublisher>, PublisherFactoryError>;

    /// Verifies that the protobuf message is valid and can be used to create a publisher
    /// encapsulated object.
    fn validate(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext<TTrigger>,
    ) -> Result<(), PublisherFactoryError>;
}

pub(crate) struct PublisherFactoryContext<'a, TTrigger: Trigger = TriggerImpl> {
    pub expression_nodes: &'a Vec<Arc<Node>>,
    pub file_descriptors: &'a Vec<FileDescriptor>,
    pub triggers: &'a HashMap<String, Arc<TTrigger>>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
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
    #[error("Field name {field_name} used more than once in declarative aggregation")]
    DuplicateFieldName { field_name: String },
    #[error("Unknown publisher type")]
    UnknownPublisherType,
}

pub(crate) struct PublisherFactoryImpl {
    service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
}

impl PublisherFactory for PublisherFactoryImpl {
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryContext,
    ) -> Result<Arc<PublisherImpl>, PublisherFactoryError> {
        let name = publisher_proto.name.clone();
        let publisher_type = match &publisher_proto.publisher_type {
            Some(PublisherTypeProto::SignalPublisher(signal_publisher)) => {
                PublisherType::Signal { signal_name: signal_publisher.signal_name.clone() }
            }
            Some(PublisherTypeProto::ServicePublisher(service_publisher)) => {
                return Ok(Arc::new(PublisherImpl::new_service_publisher(
                    name,
                    service_publisher.service_name.clone(),
                    self.service_adapter.clone(),
                )));
            }
            Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher)) => {
                self.create_aggregation_publisher(aggregation_publisher, context)?
            }
            _ => return Err(PublisherFactoryError::UnknownPublisherType),
        };
        Ok(Arc::new(PublisherImpl::new(name, publisher_type)))
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

impl PublisherFactoryImpl {
    pub(super) fn new() -> Self {
        PublisherFactoryImpl { service_adapter: Arc::new(ServiceAdapterImpl::new()) }
    }

    /// Returns the first descriptor from the vector that has the specified type name.
    fn get_message_descriptor(
        &self,
        message_type_name: &str,
        file_descriptors: &[FileDescriptor],
    ) -> Result<MessageDescriptor, PublisherFactoryError> {
        // Add a leading dot because this is what message_by_full_name expects.
        let mut full_type_name = message_type_name.to_owned();
        full_type_name.insert(0, '.');
        file_descriptors
            .iter()
            .find_map(|descriptor| descriptor.message_by_full_name(&full_type_name))
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
                .map(|trigger_name| context.triggers[trigger_name].clone())
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

                match acc.try_insert_value(
                    &declarative_aggregation_proto.field_name,
                    DeclarativeAggregation {
                        field_name: declarative_aggregation_proto.field_name.clone(),
                        selector_expression: selector_expression.clone(),
                        aggregation_type,
                    },
                ) {
                    Ok(_) => Ok(acc),
                    Err(()) => Err(PublisherFactoryError::DuplicateFieldName {
                        field_name: declarative_aggregation_proto.field_name.clone(),
                    }),
                }
            })?
            .into_values()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use protobuf::{
        descriptor::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto},
        EnumOrUnknown, MessageDyn,
    };
    use sdv_ast::NodeValue;
    use sdvtelemetry_proto_rust::metrics_configuration::{
        declarative_aggregation::AggregationType,
        AggregationPublisher as AggregationPublisherProto,
        DeclarativeAggregation as DeclarativeAggregationProto, Publisher as PublisherProto,
    };
    use std::collections::HashMap;

    use super::*;
    use crate::collection_extension::VecExtension;
    use crate::metrics_config_orchestrator::{
        publisher::service_adapter::{ServiceAdapterError, ServiceSubscription},
        trigger::TriggerType,
    };

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

    struct MockServiceAdapter {}

    #[async_trait]
    impl ServiceAdapter for MockServiceAdapter {
        async fn subscribe(
            &self,
            _service_name: &str,
            _new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
        ) -> ServiceSubscription {
            panic!("The function should never be called")
        }

        async fn unsubscribe(
            &self,
            _subscription: ServiceSubscription,
        ) -> Result<(), ServiceAdapterError> {
            panic!("The function should never be called")
        }
    }

    fn create_factory() -> PublisherFactoryImpl {
        PublisherFactoryImpl { service_adapter: Arc::new(MockServiceAdapter {}) }
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

    fn run_using_empty_factory_context<R>(
        execute_func: impl FnOnce(&PublisherFactoryContext) -> R,
    ) -> R {
        run_using_factory_context(None, None, None, execute_func)
    }

    // Creates a PublisherFactoryContext with passed data and uses it to call `execute_func`.
    // This is needed because PublisherFactoryContext contains borrows and it's impossible to have
    // a function that creates and returns it - it can only be used in the function where it's
    // created.
    fn run_using_factory_context<R>(
        triggers: Option<Vec<Arc<TriggerImpl>>>,
        nodes: Option<Vec<Arc<Node>>>,
        file_descriptors: Option<Vec<FileDescriptor>>,
        execute_func: impl FnOnce(&PublisherFactoryContext) -> R,
    ) -> R {
        let expression_nodes = nodes.unwrap_or_default();
        let file_descriptors = file_descriptors.unwrap_or_default();
        let triggers = triggers
            .map_or(HashMap::new(), |val| val.into_hash_map(|publisher| publisher.name.clone()));
        let context = PublisherFactoryContext {
            expression_nodes: &expression_nodes,
            file_descriptors: &file_descriptors,
            triggers: &triggers,
        };
        execute_func(&context)
    }

    fn set_aggregation_publisher<'a>(
        publisher: &'a mut PublisherProto,
        message_descriptor: &MessageDescriptor,
        declarative_aggregations: Vec<DeclarativeAggregationProto>,
    ) -> &'a mut AggregationPublisherProto {
        let aggregation_proto = publisher.mut_aggregation_publisher();
        aggregation_proto.message_type_name = message_descriptor.full_name().to_owned();
        aggregation_proto.declarative_aggregations = declarative_aggregations;
        aggregation_proto
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
        declarative_proto.aggregation_type = EnumOrUnknown::from_i32(UNKNOWN_AGGREGATION_TYPE);
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
    ) -> (PublisherProto, Option<Vec<FileDescriptor>>, Option<Vec<Arc<TriggerImpl>>>) {
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

        let trigger = Arc::new(TriggerImpl::new(
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
        if let PublisherType::Signal { signal_name } = &res.publisher_type {
            assert_eq!(signal_name, SIGNAL_NAME);
        } else {
            panic!("Unexpected publisher type");
        }
    }

    #[tokio::test]
    async fn create_service_publisher_success() {
        let factory = create_factory();
        let publisher = create_valid_service_publisher();

        let res =
            run_using_empty_factory_context(|context| factory.create(&publisher, context)).unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        if let PublisherType::Service { service_name, service_adapter: _, service_subscription } =
            &res.publisher_type
        {
            assert_eq!(service_name, SERVICE_NAME);
            assert!(service_subscription.lock().unwrap().is_none());
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

        let err = run_using_factory_context(None, None, file_descriptors, |context| {
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

        let err = run_using_factory_context(None, None, file_descriptors, |context| {
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

        let err = run_using_factory_context(None, None, file_descriptors, |context| {
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

        let res = run_using_factory_context(
            triggers.clone(),
            Some(vec![Arc::new(create_node()), Arc::new(create_node())]),
            file_descriptors,
            |context| factory.create(&publisher, context),
        )
        .unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        if let PublisherType::Aggregation {
            triggers: res_triggers,
            message_descriptor,
            reset_on_get,
            declarative_aggregations,
        } = &res.publisher_type
        {
            assert_eq!(res_triggers.len(), 1);
            assert!(Arc::ptr_eq(&res_triggers[0], &triggers.unwrap()[0]));
            assert_eq!(message_descriptor.full_name(), "android.sdv.telemetry.tests.TestMessage");
            assert!(reset_on_get);
            assert_eq!(declarative_aggregations.len(), 2);
            for aggregation in declarative_aggregations {
                match aggregation.field_name.as_str() {
                    FIELD_NAME => {
                        assert!(matches!(aggregation.aggregation_type, AggregationType::COUNT))
                    }
                    FIELD_NAME2 => {
                        assert!(matches!(aggregation.aggregation_type, AggregationType::VECTOR))
                    }
                    name => panic!("Unexpected field with name {}", name),
                }
            }
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

        let err = run_using_factory_context(None, None, file_descriptors, |context| {
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

        let err = run_using_factory_context(None, None, file_descriptors, |context| {
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

        let err = run_using_factory_context(None, None, file_descriptors, |context| {
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
            triggers,
            Some(vec![Arc::new(create_node()), Arc::new(create_node())]),
            file_descriptors,
            |context| factory.validate(&publisher, context),
        )
        .unwrap();
    }
}
