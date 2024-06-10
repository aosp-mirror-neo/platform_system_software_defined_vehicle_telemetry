// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::{reflect::FileDescriptor, EnumOrUnknown};
use sdvtelemetry_proto_rust::metrics_configuration::service_publisher::ConnectionType as ConnectionTypeProto;
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto,
    AggregationPublisher as AggregationPublisherProto, Publisher as PublisherProto,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::expr::ast::Node;
use crate::metrics_config_orchestrator::message_builder_factory::{
    MessageBuilderFactory, MessageBuilderFactoryCreateContext, MessageBuilderFactoryError,
    MessageBuilderFactoryValidateContext,
};
use crate::metrics_config_orchestrator::publisher::ConnectionType;
use crate::metrics_config_orchestrator::service_adapter::{ServiceAdapter, ServiceAdapterImpl};
use crate::metrics_config_orchestrator::{
    NodeValueType, Publisher, PublisherImpl, Trigger, TriggerImpl,
};

// TODO(b/304793694): refactor tests and remove generic parameters.
pub(crate) trait PublisherFactory<
    TPublisher: Publisher = PublisherImpl,
    TTrigger: Trigger = TriggerImpl,
>
{
    /// Returns a new publisher encapsulated object based on the protobuf message.
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryCreateContext<TTrigger>,
    ) -> Result<Arc<TPublisher>, PublisherFactoryError>;

    /// Verifies that the protobuf message is valid and can be used to create a publisher
    /// encapsulated object.
    fn validate(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryValidateContext,
    ) -> Result<(), PublisherFactoryError>;
}

// TODO(b/304793694): refactor tests and remove generic parameters.
pub(crate) struct PublisherFactoryCreateContext<'a, TTrigger: Trigger = TriggerImpl> {
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
    pub node_types: &'a [Option<NodeValueType>],
    pub triggers: &'a HashMap<String, Arc<TTrigger>>,
}

pub(crate) struct PublisherFactoryValidateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub node_types: &'a [Option<NodeValueType>],
}

#[allow(dead_code)]
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PublisherFactoryError {
    #[error("ServicePublisher contains invalid connection type {connection_type}")]
    InvalidConnectionType { connection_type: i32 },
    #[error("AggregationPublisher contains invalid ProtoMessageBuilder: {e}")]
    InvalidProtoMessageBuilder {
        #[from]
        e: MessageBuilderFactoryError,
    },
    #[error("Publisher name not set")]
    NameNotSet,
    #[error("Unknown message type {message_type} set for aggregation publisher")]
    UnknownMessageType { message_type: String },
    #[error("Unknown publisher type")]
    UnknownPublisherType,
    #[error("Connection type for ServicePublisher is not specified")]
    UnspecifiedConnectionType,
}

pub(crate) struct PublisherFactoryImpl {
    message_builder_factory: Arc<dyn MessageBuilderFactory>,
    service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
}

impl PublisherFactory for PublisherFactoryImpl {
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryCreateContext,
    ) -> Result<Arc<PublisherImpl>, PublisherFactoryError> {
        Self::verify_name_set(publisher_proto)?;
        let publisher_name = publisher_proto.name.clone();
        let publisher = match &publisher_proto.publisher_type {
            Some(PublisherTypeProto::ServicePublisher(service_publisher)) => {
                return Ok(Arc::new(PublisherImpl::new_service_publisher(
                    publisher_name,
                    service_publisher.service_name.clone(),
                    service_publisher.configuration.as_ref().cloned(),
                    Self::get_connection_type(service_publisher.connection_type)?,
                    Arc::clone(&self.service_adapter),
                )));
            }
            Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher_proto)) => self
                .create_aggregation_publisher(
                    publisher_name,
                    aggregation_publisher_proto,
                    context,
                )?,
            _ => return Err(PublisherFactoryError::UnknownPublisherType),
        };
        Ok(Arc::new(publisher))
    }

    fn validate(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryValidateContext,
    ) -> Result<(), PublisherFactoryError> {
        Self::verify_name_set(publisher_proto)?;
        match &publisher_proto.publisher_type {
            Some(PublisherTypeProto::ServicePublisher(service_publisher)) => {
                Self::get_connection_type(service_publisher.connection_type)?;
            }
            Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher)) => {
                self.message_builder_factory.validate(
                    &aggregation_publisher.message_builder,
                    &MessageBuilderFactoryValidateContext {
                        file_descriptors: context.file_descriptors,
                        node_types: context.node_types,
                    },
                )?
            }
            _ => return Err(PublisherFactoryError::UnknownPublisherType),
        };
        Ok(())
    }
}

impl PublisherFactoryImpl {
    pub(super) fn new(
        message_builder_factory: Arc<dyn MessageBuilderFactory>,
        service_adapter: Arc<ServiceAdapterImpl>,
    ) -> Self {
        Self { message_builder_factory, service_adapter }
    }

    /// Creates Aggregation Publisher struct based on the protobuf message and context.
    fn create_aggregation_publisher(
        &self,
        publisher_name: String,
        aggregation_publisher_proto: &AggregationPublisherProto,
        context: &PublisherFactoryCreateContext,
    ) -> Result<PublisherImpl, PublisherFactoryError> {
        let message_builder = self.message_builder_factory.create(
            format!("Aggregation Publisher {}", publisher_name),
            &aggregation_publisher_proto.message_builder,
            &MessageBuilderFactoryCreateContext {
                file_descriptors: context.file_descriptors,
                nodes: context.nodes,
                node_types: context.node_types,
            },
        )?;
        let triggers = aggregation_publisher_proto
            .trigger_names
            .iter()
            .map(|trigger_name| Arc::clone(&context.triggers[trigger_name]) as Arc<dyn Trigger>)
            .collect();
        let publisher = PublisherImpl::new_aggregation_publisher(
            publisher_name,
            message_builder,
            aggregation_publisher_proto.reset_on_get,
            triggers,
        );
        Ok(publisher)
    }

    fn get_connection_type(
        connection_type_proto: EnumOrUnknown<ConnectionTypeProto>,
    ) -> Result<ConnectionType, PublisherFactoryError> {
        let connection_type_proto = connection_type_proto
            .enum_value()
            .map_err(|i| PublisherFactoryError::InvalidConnectionType { connection_type: i })?;

        let connection_type = match connection_type_proto {
            ConnectionTypeProto::CONNECTION_TYPE_UNSPECIFIED => {
                return Err(PublisherFactoryError::UnspecifiedConnectionType)
            }
            ConnectionTypeProto::SUBSCRIPTION => ConnectionType::Subscription,
            ConnectionTypeProto::GETTER => ConnectionType::Getter,
        };

        Ok(connection_type)
    }

    fn verify_name_set(publisher_proto: &PublisherProto) -> Result<(), PublisherFactoryError> {
        if publisher_proto.name.is_empty() {
            Err(PublisherFactoryError::NameNotSet)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use protobuf::well_known_types::{any::Any, empty::Empty};
    use protobuf::{Message, MessageDyn, MessageField};
    use sdv_telemetry_utils::collection_extension::VecExtension;
    use sdvtelemetry_proto_rust::metrics_configuration::Publisher as PublisherProto;
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::metrics_config_orchestrator::message_builder_factory::mock::MockMessageBuilderFactory;
    use crate::metrics_config_orchestrator::publisher::PublisherType;
    use crate::metrics_config_orchestrator::service_adapter::{
        ServiceMessageProvider, ServiceSubscription,
    };
    use crate::metrics_config_orchestrator::trigger::TriggerType;

    const INTERVAL: Duration = Duration::from_millis(42);
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";
    const TRIGGER_NAME: &str = "trigger";

    struct MockServiceAdapter {}

    #[async_trait]
    impl ServiceAdapter for MockServiceAdapter {
        fn get_message_provider(
            &self,
            _service_name: &str,
        ) -> anyhow::Result<Option<Box<dyn ServiceMessageProvider>>> {
            unreachable!()
        }

        async fn subscribe(
            &self,
            _service_name: &str,
            _configuration: &Option<Any>,
            _new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
        ) -> anyhow::Result<ServiceSubscription> {
            unreachable!()
        }

        async fn unsubscribe(&self, _subscription: ServiceSubscription) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    fn create_default_factory() -> PublisherFactoryImpl {
        create_factory(None)
    }

    fn create_factory(
        message_builder_factory: Option<MockMessageBuilderFactory>,
    ) -> PublisherFactoryImpl {
        let message_builder_factory =
            Arc::new(message_builder_factory.unwrap_or_else(MockMessageBuilderFactory::new));
        PublisherFactoryImpl {
            service_adapter: Arc::new(MockServiceAdapter {}),
            message_builder_factory,
        }
    }

    fn create_publisher(name: &str) -> PublisherProto {
        PublisherProto { name: name.to_owned(), ..Default::default() }
    }

    fn create_service_publisher_with_invalid_connection_type() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        publisher.mut_service_publisher().connection_type = EnumOrUnknown::from_i32(-1);
        publisher
    }

    fn create_service_publisher_with_unspecified_connection_type() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        publisher.mut_service_publisher();
        publisher
    }

    fn create_valid_service_publisher() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let service_publisher = publisher.mut_service_publisher();
        service_publisher.configuration =
            MessageField::some(Any::pack(Empty::default_instance()).unwrap());
        service_publisher.connection_type = ConnectionTypeProto::SUBSCRIPTION.into();
        SERVICE_NAME.clone_into(&mut service_publisher.service_name);
        publisher
    }

    fn create_valid_aggregation_publisher() -> (PublisherProto, Arc<TriggerImpl>) {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let aggregation_proto = publisher.mut_aggregation_publisher();
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());
        aggregation_proto.reset_on_get = true;

        let trigger =
            TriggerImpl::new(TRIGGER_NAME.to_owned(), TriggerType::Periodic { interval: INTERVAL });

        (publisher, trigger)
    }

    impl PublisherFactoryImpl {
        // The following functions create a context with passed data and use it to call `create`
        // or `validate`. This is needed because context structs contain borrows and it's impossible
        // to have a function that creates and returns it - it can only be used in the function
        // where it's created.
        fn create_using_default_context(
            &self,
            publisher_proto: &PublisherProto,
        ) -> Result<Arc<PublisherImpl>, PublisherFactoryError> {
            self.create_using_context_with(publisher_proto, None)
        }

        fn create_using_context_with(
            &self,
            publisher_proto: &PublisherProto,
            triggers: Option<Vec<Arc<TriggerImpl>>>,
        ) -> Result<Arc<PublisherImpl>, PublisherFactoryError> {
            let file_descriptors = vec![];
            let nodes = vec![];
            let node_types = vec![];
            let triggers = triggers.map_or(HashMap::new(), |val| {
                val.into_hash_map(|publisher| publisher.name.clone())
            });
            let context = PublisherFactoryCreateContext {
                file_descriptors: &file_descriptors,
                nodes: &nodes,
                node_types: &node_types,
                triggers: &triggers,
            };
            self.create(publisher_proto, &context)
        }

        fn validate_using_default_context(
            &self,
            publisher_proto: &PublisherProto,
        ) -> Result<(), PublisherFactoryError> {
            let file_descriptors = vec![];
            let node_types = vec![];
            let context = PublisherFactoryValidateContext {
                file_descriptors: &file_descriptors,
                node_types: &node_types,
            };
            self.validate(publisher_proto, &context)
        }
    }

    #[test]
    fn create_no_name_fails() {
        let factory = create_default_factory();

        let err = factory.create_using_default_context(&PublisherProto::default()).err().unwrap();

        assert!(matches!(&err, PublisherFactoryError::NameNotSet), "Unexpected error: {:?}", err);
    }

    #[test]
    fn create_service_publisher_invalid_connection_type_fails() {
        let factory = create_default_factory();
        let publisher = create_service_publisher_with_invalid_connection_type();

        let err = factory.create_using_default_context(&publisher).err().unwrap();

        assert!(
            matches!(&err, PublisherFactoryError::InvalidConnectionType { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_service_publisher_unspecified_connection_type_fails() {
        let factory = create_default_factory();
        let publisher = create_service_publisher_with_unspecified_connection_type();

        let err = factory.create_using_default_context(&publisher).err().unwrap();

        assert!(
            matches!(&err, PublisherFactoryError::UnspecifiedConnectionType),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_service_publisher_success() {
        let factory = create_default_factory();
        let publisher = create_valid_service_publisher();

        let res = factory.create_using_default_context(&publisher).unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        let PublisherType::Service {
            configuration,
            connection_type,
            service_name,
            service_adapter: _,
            service_subscription,
        } = &res.publisher_type
        else {
            panic!("Unexpected publisher type");
        };
        let configuration_value = configuration.as_ref().expect("Configuration should be set");
        let any_value = configuration_value
            .unpack::<Empty>()
            .unwrap()
            .expect("The value of Any should be an Empty");
        assert_eq!(&any_value, Empty::default_instance());
        assert!(matches!(connection_type, ConnectionType::Subscription));
        assert_eq!(service_name, SERVICE_NAME);
        assert!(service_subscription.lock().unwrap().is_none());
    }

    #[test]
    fn create_aggregation_publisher_message_builder_factory_error_fails() {
        let mut message_builder_factory = MockMessageBuilderFactory::new();
        message_builder_factory.fail_create();
        let factory = create_factory(Some(message_builder_factory));
        let (publisher, trigger) = create_valid_aggregation_publisher();

        let err = factory.create_using_context_with(&publisher, Some(vec![trigger])).err().unwrap();

        assert!(matches!(&err, PublisherFactoryError::InvalidProtoMessageBuilder { .. }));
    }

    #[test]
    fn create_aggregation_publisher_success() {
        let factory = create_default_factory();
        let (publisher, trigger) = create_valid_aggregation_publisher();

        let res = factory
            .create_using_context_with(&publisher, Some(vec![Arc::clone(&trigger)]))
            .unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        let PublisherType::Aggregation { triggers: res_triggers, reset_on_get, .. } =
            &res.publisher_type
        else {
            panic!("Unexpected publisher type");
        };
        assert_eq!(res_triggers.len(), 1);
        assert!(Arc::ptr_eq(&res_triggers[0], &(trigger as Arc<dyn Trigger>)));
        assert!(reset_on_get);
    }

    #[test]
    fn validate_no_name_fails() {
        let factory = create_default_factory();

        let err = factory.validate_using_default_context(&PublisherProto::default()).unwrap_err();

        assert!(matches!(&err, PublisherFactoryError::NameNotSet), "Unexpected error: {:?}", err);
    }

    #[test]
    fn validate_service_publisher_invalid_connection_type_fails() {
        let factory = create_default_factory();
        let publisher = create_service_publisher_with_invalid_connection_type();

        let err = factory.validate_using_default_context(&publisher).unwrap_err();

        assert!(
            matches!(&err, PublisherFactoryError::InvalidConnectionType { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_service_publisher_unspecified_connection_type_fails() {
        let factory = create_default_factory();
        let publisher = create_service_publisher_with_unspecified_connection_type();

        let err = factory.validate_using_default_context(&publisher).unwrap_err();

        assert!(
            matches!(&err, PublisherFactoryError::UnspecifiedConnectionType),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_service_publisher_success() {
        let factory = create_default_factory();
        let publisher = create_valid_service_publisher();

        factory.validate_using_default_context(&publisher).unwrap();
    }

    #[test]
    fn validate_aggregation_publisher_message_builder_factory_error_fails() {
        let mut message_builder_factory = MockMessageBuilderFactory::new();
        message_builder_factory.fail_validate();
        let factory = create_factory(Some(message_builder_factory));
        let (publisher, _) = create_valid_aggregation_publisher();

        let err = factory.validate_using_default_context(&publisher).unwrap_err();

        assert!(matches!(&err, PublisherFactoryError::InvalidProtoMessageBuilder { .. }));
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_success() {
        let factory = create_default_factory();
        let (publisher, _) = create_valid_aggregation_publisher();

        factory.validate_using_default_context(&publisher).unwrap();
    }
}
