// Copyright 2023 Google LLC

use anyhow::anyhow;
use either::Either;
use protobuf::{reflect::FileDescriptor, EnumOrUnknown};
use sdv_telemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto,
    service_publisher::ConnectionType as ConnectionTypeProto,
    AggregationPublisher as AggregationPublisherProto, Publisher as PublisherProto,
    ServicePublisher as ServicePublisherProto,
};
use sdv_telemetry_service_publisher_accessor::SubSamplingInterval;
use sdv_telemetry_utils::duration_utils::try_into_duration;
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::expr::node::Node;
use crate::metrics_config_orchestrator::message_builder_factory::{
    MessageBuilderFactory, MessageBuilderFactoryCreateContext, MessageBuilderFactoryError,
    MessageBuilderFactoryValidateContext,
};
use crate::metrics_config_orchestrator::publisher::aggregation::AggregationPublisher;
use crate::metrics_config_orchestrator::publisher::service::{ConnectionType, ServicePublisher};
use crate::metrics_config_orchestrator::service_adapter::ServiceAdapter;
use crate::metrics_config_orchestrator::{NodeValueType, Publisher, Trigger};
use crate::metrics_config_runtime_error::{
    AggregationPublisherRuntimeError, ServicePublisherRuntimeError,
};

#[cfg_attr(test, sdv_telemetry_utils_macros::with_super_trait(downcast_rs::Downcast))]
pub(crate) trait PublisherFactory: Send + Sync {
    /// Returns a new publisher encapsulated object based on the protobuf message.
    fn create(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryCreateContext,
    ) -> Result<Arc<dyn Publisher>, PublisherFactoryError>;

    /// Verifies that the protobuf message is valid and can be used to create a publisher
    /// encapsulated object.
    fn validate(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryValidateContext,
    ) -> Result<(), PublisherFactoryError>;
}

#[cfg(test)]
downcast_rs::impl_downcast!(PublisherFactory);

pub(crate) struct PublisherFactoryCreateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
    pub node_types: &'a [Option<NodeValueType>],
    pub triggers: &'a HashMap<String, Arc<dyn Trigger>>,
    pub error_callback: Arc<
        dyn Fn(Either<ServicePublisherRuntimeError, AggregationPublisherRuntimeError>)
            + Send
            + Sync,
    >,
}

pub(crate) struct PublisherFactoryValidateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub node_types: &'a [Option<NodeValueType>],
}

#[derive(Error, Debug)]
pub(crate) enum PublisherFactoryError {
    #[error("ServicePublisher contains invalid connection type {connection_type}")]
    InvalidConnectionType { connection_type: i32 },
    #[error("AggregationPublisher contains invalid ProtoMessageBuilder: {e}")]
    InvalidProtoMessageBuilder {
        #[from]
        e: MessageBuilderFactoryError,
    },
    #[error("Publisher name not set")]
    NameNotSet,
    #[error("Unknown publisher type")]
    UnknownPublisherType,
    #[error("Sub-sampling interval is invalid: {err:#}")]
    InvalidSubSamplingInterval { err: anyhow::Error },
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
    ) -> Result<Arc<dyn Publisher>, PublisherFactoryError> {
        Self::verify_name_set(publisher_proto)?;
        let publisher_name = publisher_proto.name.clone();
        match &publisher_proto.publisher_type {
            Some(PublisherTypeProto::ServicePublisher(service_publisher)) => {
                let error_callback = Arc::clone(&context.error_callback);
                Ok(Arc::new(ServicePublisher::new(
                    publisher_name,
                    service_publisher.service_name.clone(),
                    service_publisher.configuration.as_ref().cloned(),
                    Self::get_sub_sampling_interval(service_publisher)?,
                    Self::get_connection_type(service_publisher.connection_type)?,
                    Arc::clone(&self.service_adapter),
                    Arc::new(move |err| error_callback(Either::Left(err))),
                )))
            }
            Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher_proto)) => Ok(self
                .create_aggregation_publisher(publisher_name, aggregation_publisher_proto, context)?
                .into()),
            _ => Err(PublisherFactoryError::UnknownPublisherType),
        }
    }

    fn validate(
        &self,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryValidateContext,
    ) -> Result<(), PublisherFactoryError> {
        Self::verify_name_set(publisher_proto)?;
        match &publisher_proto.publisher_type {
            Some(PublisherTypeProto::ServicePublisher(service_publisher)) => {
                Self::get_sub_sampling_interval(service_publisher)?;
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
        service_adapter: Arc<dyn ServiceAdapter>,
    ) -> Self {
        Self { message_builder_factory, service_adapter }
    }

    /// Creates Aggregation Publisher struct based on the protobuf message and context.
    fn create_aggregation_publisher(
        &self,
        publisher_name: String,
        aggregation_publisher_proto: &AggregationPublisherProto,
        context: &PublisherFactoryCreateContext,
    ) -> Result<Box<dyn Publisher>, PublisherFactoryError> {
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
        let error_callback = Arc::clone(&context.error_callback);
        let publisher = AggregationPublisher::new(
            publisher_name,
            message_builder.into(),
            aggregation_publisher_proto.reset_on_get,
            triggers,
            Arc::new(move |err| error_callback(Either::Right(err))),
        );
        Ok(Box::new(publisher))
    }

    fn get_connection_type(
        connection_type_proto: EnumOrUnknown<ConnectionTypeProto>,
    ) -> Result<ConnectionType, PublisherFactoryError> {
        let connection_type_proto = connection_type_proto
            .enum_value()
            .map_err(|i| PublisherFactoryError::InvalidConnectionType { connection_type: i })?;

        let connection_type = match connection_type_proto {
            ConnectionTypeProto::CONNECTION_TYPE_UNSPECIFIED
            | ConnectionTypeProto::SUBSCRIPTION => ConnectionType::Subscription,
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

    fn get_sub_sampling_interval(
        service_publisher_proto: &ServicePublisherProto,
    ) -> Result<SubSamplingInterval, PublisherFactoryError> {
        let duration =
            try_into_duration(&service_publisher_proto.sub_sampling_interval).map_err(|_| {
                PublisherFactoryError::InvalidSubSamplingInterval {
                    err: anyhow!("Sub-sampling interval must be positive."),
                }
            })?;
        SubSamplingInterval::try_from(duration)
            .map_err(|err| PublisherFactoryError::InvalidSubSamplingInterval { err })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use protobuf::well_known_types::duration::Duration as DurationProto;
    use protobuf::well_known_types::{any::Any, empty::Empty};
    use protobuf::{Message, MessageDyn, MessageField};
    use sdv_telemetry_proto_rust::metrics_configuration::Publisher as PublisherProto;
    use sdv_telemetry_service_publisher_accessor::{
        AvailabilityCallbackRegistration, CreateSubscriptionError, GetMessageProviderError,
    };
    use sdv_telemetry_test_utils::parameterized_test;
    use sdv_telemetry_utils::collection_extension::VecExtension;
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::metrics_config_orchestrator::message_builder_factory::mock::MockMessageBuilderFactory;
    use crate::metrics_config_orchestrator::service_adapter::{
        GetMessageProviderOutcome, ServiceSubscription,
    };
    use crate::metrics_config_orchestrator::trigger::periodic::PeriodicTrigger;

    const INTERVAL: Duration = Duration::from_millis(42);
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";
    const TRIGGER_NAME: &str = "trigger";

    struct MockServiceAdapter {}

    #[async_trait]
    impl ServiceAdapter for MockServiceAdapter {
        async fn get_message_provider(
            &self,
            _service_name: &str,
        ) -> Result<GetMessageProviderOutcome, GetMessageProviderError> {
            unreachable!()
        }

        async fn subscribe(
            self: Arc<Self>,
            _service_name: &str,
            _configuration: Option<&Any>,
            _sub_sampling_interval: &SubSamplingInterval,
            _new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
            _cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
        ) -> Result<Option<ServiceSubscription>, CreateSubscriptionError> {
            unreachable!()
        }

        async fn unsubscribe(&self, _subscription: ServiceSubscription) -> anyhow::Result<()> {
            unreachable!()
        }

        async fn register_availability_callback(
            &self,
            _service_name: &str,
            _callback: Box<dyn Fn() + Send + Sync>,
        ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
            unreachable!()
        }
    }

    fn create_default_factory() -> PublisherFactoryImpl {
        create_factory(None)
    }

    fn create_factory(
        message_builder_factory: Option<MockMessageBuilderFactory>,
    ) -> PublisherFactoryImpl {
        PublisherFactoryImpl::new(
            Arc::new(message_builder_factory.unwrap_or_else(MockMessageBuilderFactory::new)),
            Arc::new(MockServiceAdapter {}),
        )
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

    fn create_service_publisher_with_negative_sub_sampling_interval() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        publisher.mut_service_publisher().sub_sampling_interval =
            Some(DurationProto { seconds: -1, ..Default::default() }).into();
        publisher
    }

    fn create_service_publisher_with_too_big_sub_sampling_interval() -> PublisherProto {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        publisher.mut_service_publisher().sub_sampling_interval =
            Some(DurationProto { seconds: i64::MAX, ..Default::default() }).into();
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

    fn create_valid_aggregation_publisher() -> (PublisherProto, Arc<dyn Trigger>) {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let aggregation_proto = publisher.mut_aggregation_publisher();
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());
        aggregation_proto.reset_on_get = true;

        let trigger = PeriodicTrigger::new(TRIGGER_NAME.to_owned(), INTERVAL);

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
        ) -> Result<Arc<dyn Publisher>, PublisherFactoryError> {
            self.create_using_context_with(publisher_proto, None)
        }

        fn create_using_context_with(
            &self,
            publisher_proto: &PublisherProto,
            triggers: Option<Vec<Arc<dyn Trigger>>>,
        ) -> Result<Arc<dyn Publisher>, PublisherFactoryError> {
            let file_descriptors = vec![];
            let nodes = vec![];
            let node_types = vec![];
            let triggers = triggers.map_or(HashMap::new(), |triggers| {
                triggers.into_hash_map(|trigger| trigger.get_name().to_owned())
            });
            let context = PublisherFactoryCreateContext {
                file_descriptors: &file_descriptors,
                nodes: &nodes,
                node_types: &node_types,
                triggers: &triggers,
                error_callback: Arc::new(|err| {
                    panic!("Unexpected error: {err:?}");
                }),
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
    fn create_service_publisher_unspecified_connection_type_maps_to_subscription() {
        let factory = create_default_factory();
        let publisher = create_service_publisher_with_unspecified_connection_type();

        let res = factory.create_using_default_context(&publisher).unwrap();

        let res_publisher = &res.downcast_arc::<ServicePublisher>().ok().unwrap();
        assert!(matches!(res_publisher.connection_type, ConnectionType::Subscription));
    }

    parameterized_test!(
        #[test]
        create_service_publisher,

        |publisher| {
            let factory = create_default_factory();
            let err = factory.create_using_default_context(&publisher).err().unwrap();

            assert!(
                matches!(&err, PublisherFactoryError::InvalidSubSamplingInterval { .. }),
                "Unexpected error: {:?}",
                err
            );
        },

        negative_sub_sampling_interval_fails: (create_service_publisher_with_negative_sub_sampling_interval(),),
        too_big_sub_sampling_interval_fails: (create_service_publisher_with_too_big_sub_sampling_interval(),),
    );

    #[test]
    fn create_service_publisher_success() {
        let factory = create_default_factory();
        let publisher = create_valid_service_publisher();

        let res = factory.create_using_default_context(&publisher).unwrap();

        assert_eq!(res.get_name(), PUBLISHER_NAME);
        let res_publisher = res.downcast_arc::<ServicePublisher>().ok().unwrap();

        let configuration_value =
            res_publisher.configuration.as_ref().expect("Configuration should be set");
        let any_value = configuration_value
            .unpack::<Empty>()
            .unwrap()
            .expect("The value of Any should be an Empty");
        assert_eq!(&any_value, Empty::default_instance());
        assert!(matches!(res_publisher.connection_type, ConnectionType::Subscription));
        assert_eq!(res_publisher.service_name, SERVICE_NAME);
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

        assert_eq!(res.get_name(), PUBLISHER_NAME);
        let res_publisher = res.downcast_arc::<AggregationPublisher>().ok().unwrap();
        assert_eq!(res_publisher.triggers.len(), 1);
        assert!(Arc::ptr_eq(&res_publisher.triggers[0], &(trigger as Arc<dyn Trigger>)));
        assert!(res_publisher.reset_on_get);
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
    fn validate_service_publisher_unspecified_connection_type_success() {
        let factory = create_default_factory();
        let publisher = create_service_publisher_with_unspecified_connection_type();

        factory.validate_using_default_context(&publisher).unwrap();
    }

    parameterized_test!(
        #[test]
        validate_service_publisher,

        |publisher| {
            let factory = create_default_factory();
            let err = factory.validate_using_default_context(&publisher).unwrap_err();

            assert!(
                matches!(&err, PublisherFactoryError::InvalidSubSamplingInterval { .. }),
                "Unexpected error: {:?}",
                err
            );
        },

        negative_sub_sampling_interval_fails: (create_service_publisher_with_negative_sub_sampling_interval(),),
        too_big_sub_sampling_interval_fails: (create_service_publisher_with_too_big_sub_sampling_interval(),),
    );

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
