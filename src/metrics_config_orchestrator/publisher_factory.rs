// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::FileDescriptor;
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto,
    AggregationPublisher as AggregationPublisherProto, Publisher as PublisherProto,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::expr::ast::Node;
use crate::metrics_config_orchestrator::{
    message_builder_factory::{
        MessageBuilderFactory, MessageBuilderFactoryCreateContext, MessageBuilderFactoryError,
        MessageBuilderFactoryValidateContext,
    },
    publisher::{
        service_adapter::{ServiceAdapter, ServiceAdapterImpl},
        PublisherType,
    },
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
    pub node_types: &'a [NodeValueType],
    pub triggers: &'a HashMap<String, Arc<TTrigger>>,
}

pub(crate) struct PublisherFactoryValidateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub node_types: &'a [NodeValueType],
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PublisherFactoryError {
    #[error("Publisher name not set")]
    NameNotSet,
    #[error("Unknown message type {message_type} set for aggregation publisher")]
    UnknownMessageType { message_type: String },
    #[error("AggregationPublisher contains invalid ProtoMessageBuilder: {e}")]
    InvalidProtoMessageBuilder {
        #[from]
        e: MessageBuilderFactoryError,
    },
    #[error("Unknown publisher type")]
    UnknownPublisherType,
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
            Some(PublisherTypeProto::SignalPublisher(signal_publisher)) => PublisherImpl::new(
                publisher_name,
                PublisherType::Signal { signal_name: signal_publisher.signal_name.clone() },
            ),
            Some(PublisherTypeProto::ServicePublisher(service_publisher)) => {
                return Ok(Arc::new(PublisherImpl::new_service_publisher(
                    publisher_name,
                    service_publisher.service_name.clone(),
                    self.service_adapter.clone(),
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
            Some(PublisherTypeProto::SignalPublisher(_))
            | Some(PublisherTypeProto::ServicePublisher(_)) => (),
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
    pub(super) fn new(message_builder_factory: Arc<dyn MessageBuilderFactory>) -> Self {
        PublisherFactoryImpl {
            message_builder_factory,
            service_adapter: Arc::new(ServiceAdapterImpl::new()),
        }
    }

    /// Creates Aggregation Publisher struct based on the protobuf message and context.
    fn create_aggregation_publisher(
        &self,
        publisher_name: String,
        aggregation_publisher_proto: &AggregationPublisherProto,
        context: &PublisherFactoryCreateContext,
    ) -> Result<PublisherImpl, PublisherFactoryError> {
        let message_builder = self.message_builder_factory.create(
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
            .map(|trigger_name| context.triggers[trigger_name].clone() as Arc<dyn Trigger>)
            .collect();
        let publisher = PublisherImpl::new_aggregation_publisher(
            publisher_name,
            message_builder,
            aggregation_publisher_proto.reset_on_get,
            triggers,
        );
        Ok(publisher)
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
    use protobuf::MessageDyn;
    use sdvtelemetry_proto_rust::metrics_configuration::Publisher as PublisherProto;
    use std::collections::HashMap;

    use super::*;
    use crate::collection_extension::VecExtension;
    use crate::metrics_config_orchestrator::{
        message_builder_factory::mock::MockMessageBuilderFactory,
        publisher::service_adapter::{ServiceAdapterError, ServiceSubscription},
        trigger::TriggerType,
    };

    const INTERVAL_MS: u64 = 42;
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";
    const SIGNAL_NAME: &str = "signal";
    const TRIGGER_NAME: &str = "trigger";

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
        let mut publisher = PublisherProto::new();
        publisher.name = name.to_owned();
        publisher
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

    fn create_valid_aggregation_publisher() -> (PublisherProto, Arc<TriggerImpl>) {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let aggregation_proto = publisher.mut_aggregation_publisher();
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());
        aggregation_proto.reset_on_get = true;

        let trigger = Arc::new(TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL_MS },
        ));

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

        let err = factory.create_using_default_context(&PublisherProto::new()).err().unwrap();

        assert!(matches!(&err, PublisherFactoryError::NameNotSet), "Unexpected error: {:?}", err);
    }

    #[test]
    fn create_signal_publisher_success() {
        let factory = create_default_factory();
        let publisher = create_valid_signal_publisher();

        let res = factory.create_using_default_context(&publisher).unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        let PublisherType::Signal { signal_name } = &res.publisher_type else {
            panic!("Unexpected publisher type");
        };
        assert_eq!(signal_name, SIGNAL_NAME);
    }

    #[tokio::test]
    async fn create_service_publisher_success() {
        let factory = create_default_factory();
        let publisher = create_valid_service_publisher();

        let res = factory.create_using_default_context(&publisher).unwrap();

        assert_eq!(res.name, PUBLISHER_NAME);
        let PublisherType::Service { service_name, service_adapter: _, service_subscription } =
            &res.publisher_type
        else {
            panic!("Unexpected publisher type");
        };
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

        let res =
            factory.create_using_context_with(&publisher, Some(vec![trigger.clone()])).unwrap();

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

        let err = factory.validate_using_default_context(&PublisherProto::new()).err().unwrap();

        assert!(matches!(&err, PublisherFactoryError::NameNotSet), "Unexpected error: {:?}", err);
    }

    #[test]
    fn validate_signal_publisher_success() {
        let factory = create_default_factory();
        let publisher = create_valid_signal_publisher();

        factory.validate_using_default_context(&publisher).unwrap();
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

        let err = factory.validate_using_default_context(&publisher).err().unwrap();

        assert!(matches!(&err, PublisherFactoryError::InvalidProtoMessageBuilder { .. }));
    }

    #[test]
    fn validate_aggregation_publisher_declarative_aggregation_success() {
        let factory = create_default_factory();
        let (publisher, _) = create_valid_aggregation_publisher();

        factory.validate_using_default_context(&publisher).unwrap();
    }
}
