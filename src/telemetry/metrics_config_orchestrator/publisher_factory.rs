// Copyright 2023 Google LLC

use anyhow::anyhow;
use protobuf::{reflect::FileDescriptor, well_known_types::any::Any, EnumOrUnknown};
use sdv_telemetry_expr::node::Node;
use sdv_telemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto,
    service_publisher::ConnectionType as ConnectionTypeProto,
    AggregationPublisher as AggregationPublisherProto, Publisher as PublisherProto,
    ServicePublisher as ServicePublisherProto,
};
use sdv_telemetry_service_publisher_accessor::SubSamplingInterval;
use sdv_telemetry_utils::duration_utils::try_into_duration;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::metrics_config_orchestrator::message_builder_factory::{
    MessageBuilderFactory, MessageBuilderFactoryCreateContext, MessageBuilderFactoryError,
    MessageBuilderFactoryValidateContext,
};
use crate::metrics_config_orchestrator::publisher::aggregation::AggregationPublisher;
use crate::metrics_config_orchestrator::publisher::service::{ConnectionType, ServicePublisher};
use crate::metrics_config_orchestrator::{
    message_builder::MessageBuilder, node_evaluation_context_factory::NodeEvaluationContextFactory,
    service_adapter::ServiceAdapter, NodeValueType, Publisher, Trigger,
};
use crate::status_reporter::StatusReporter;

#[cfg_attr(test, sdv_telemetry_utils::with_super_trait(downcast_rs::Downcast))]
pub(crate) trait PublisherFactory: Send + Sync {
    /// Returns a new publisher encapsulated object based on the protobuf message.
    fn create(
        &self,
        metrics_config_uuid: String,
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
}

pub(crate) struct PublisherFactoryValidateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub node_types: &'a [Option<NodeValueType>],
}

#[derive(thiserror::Error, Debug)]
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
    aggregation_publisher_new_func: Box<
        dyn Fn(
                String,
                Arc<dyn MessageBuilder>,
                String,
                bool,
                Vec<Arc<dyn Trigger>>,
                Arc<dyn StatusReporter>,
            ) -> Box<dyn Publisher>
            + Send
            + Sync,
    >,
    message_builder_factory: Arc<dyn MessageBuilderFactory>,
    node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    service_publisher_new_func: Box<
        dyn Fn(
                String,
                bool,
                Option<Any>,
                ConnectionType,
                String,
                String,
                SubSamplingInterval,
                Arc<dyn NodeEvaluationContextFactory>,
                Arc<dyn ServiceAdapter + Send + Sync>,
                Arc<dyn StatusReporter>,
            ) -> Box<dyn Publisher>
            + Send
            + Sync,
    >,
    status_reporter: Arc<dyn StatusReporter>,
}

impl PublisherFactory for PublisherFactoryImpl {
    fn create(
        &self,
        metrics_config_uuid: String,
        publisher_proto: &PublisherProto,
        context: &PublisherFactoryCreateContext,
    ) -> Result<Arc<dyn Publisher>, PublisherFactoryError> {
        Self::verify_name_set(publisher_proto)?;
        let publisher_name = publisher_proto.name.clone();
        match &publisher_proto.publisher_type {
            Some(PublisherTypeProto::ServicePublisher(service_publisher)) => Ok((self
                .service_publisher_new_func)(
                publisher_name,
                service_publisher.fetch_last_message,
                service_publisher.configuration.as_ref().cloned(),
                Self::get_connection_type(service_publisher.connection_type)?,
                metrics_config_uuid,
                service_publisher.service_name.clone(),
                Self::get_sub_sampling_interval(service_publisher)?,
                Arc::clone(&self.node_evaluation_context_factory),
                Arc::clone(&self.service_adapter),
                Arc::clone(&self.status_reporter),
            )
            .into()),
            Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher_proto)) => Ok(self
                .create_aggregation_publisher(
                    publisher_name,
                    metrics_config_uuid,
                    aggregation_publisher_proto,
                    context,
                )?
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
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
        status_reporter: Arc<dyn StatusReporter>,
    ) -> Self {
        Self {
            aggregation_publisher_new_func: Box::new(
                |name,
                 message_builder,
                 metrics_config_uuid,
                 reset_on_get,
                 triggers,
                 status_reporter| {
                    Box::new(AggregationPublisher::new(
                        name,
                        message_builder,
                        metrics_config_uuid,
                        reset_on_get,
                        triggers,
                        status_reporter,
                    ))
                },
            ),
            message_builder_factory,
            node_evaluation_context_factory,
            service_adapter,
            service_publisher_new_func: Box::new(
                |name,
                 fetch_last_message,
                 configuration,
                 connection_type,
                 metrics_config_uuid,
                 service_name,
                 sub_sampling_interval,
                 node_evaluation_context_factory,
                 service_adapter,
                 status_reporter| {
                    Box::new(ServicePublisher::new(
                        name,
                        configuration,
                        connection_type,
                        fetch_last_message,
                        metrics_config_uuid,
                        service_name,
                        sub_sampling_interval,
                        node_evaluation_context_factory,
                        service_adapter,
                        status_reporter,
                    ))
                },
            ),
            status_reporter,
        }
    }

    /// Creates Aggregation Publisher struct based on the protobuf message and context.
    fn create_aggregation_publisher(
        &self,
        publisher_name: String,
        metrics_config_uuid: String,
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
        let publisher = (self.aggregation_publisher_new_func)(
            publisher_name,
            message_builder.into(),
            metrics_config_uuid,
            aggregation_publisher_proto.reset_on_get,
            triggers,
            Arc::clone(&self.status_reporter),
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
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_proto_rust::metrics_configuration::Publisher as PublisherProto;
    use sdv_telemetry_service_publisher_accessor::{
        AvailabilityCallbackRegistration, ServicePublisherAccessorError,
    };
    use sdv_telemetry_test_utils::parameterized_test;
    use sdv_telemetry_utils::collection_extension::VecExtension;
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::time::Duration;

    use crate::metrics_config_orchestrator::service_adapter::{
        GetMessageProviderOutcome, ServiceSubscription,
    };
    use crate::metrics_config_orchestrator::{
        message_builder_factory::mock::MockMessageBuilderFactory,
        node_evaluation_context_factory::NodeEvaluationContextFactoryImpl,
        publisher::mock::MockPublisher, trigger::mock::MockTrigger,
    };
    use crate::StatusReporterImpl;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";
    const TRIGGER_NAME: &str = "trigger";

    struct MockServiceAdapter {}

    #[async_trait]
    impl ServiceAdapter for MockServiceAdapter {
        async fn get_message_provider(
            &self,
            _service_name: &str,
        ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
            unreachable!()
        }

        async fn subscribe(
            self: Arc<Self>,
            _service_name: &str,
            _configuration: Option<&Any>,
            _fetch_last_message: bool,
            _sub_sampling_interval: &SubSamplingInterval,
            _new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
            _cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
        ) -> Result<Option<ServiceSubscription>, ServicePublisherAccessorError> {
            unreachable!()
        }

        async fn unsubscribe(&self, _subscription: ServiceSubscription) {
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

    struct AggregationPublisherData {
        reset_on_get: bool,
        triggers: Vec<Arc<dyn Trigger>>,
    }

    struct ServicePublisherData {
        configuration: Option<Any>,
        connection_type: ConnectionType,
        fetch_last_message: bool,
        service_name: String,
        sub_sampling_interval: SubSamplingInterval,
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
            Arc::new(NodeEvaluationContextFactoryImpl::new(new_stub_pal())),
            Arc::new(StatusReporterImpl::new(|err| {
                panic!("Unexpected error: {err:?}");
            })),
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
        service_publisher.fetch_last_message = true;
        SERVICE_NAME.clone_into(&mut service_publisher.service_name);
        service_publisher.sub_sampling_interval =
            Some(DurationProto { seconds: 1, ..Default::default() }).into();
        publisher
    }

    fn create_valid_aggregation_publisher() -> (PublisherProto, Arc<dyn Trigger>) {
        let mut publisher = create_publisher(PUBLISHER_NAME);
        let aggregation_proto = publisher.mut_aggregation_publisher();
        aggregation_proto.trigger_names.push(TRIGGER_NAME.to_owned());
        aggregation_proto.reset_on_get = true;

        let trigger = Arc::new(MockTrigger::new(TRIGGER_NAME.to_owned(), CONFIG_UUID.to_owned()));

        (publisher, trigger)
    }

    impl PublisherFactoryImpl {
        fn capture_aggregation_publisher(&mut self) -> Arc<OnceLock<AggregationPublisherData>> {
            let data_holder = Arc::new(OnceLock::new());
            let data_holder_clone = Arc::clone(&data_holder);
            self.aggregation_publisher_new_func = Box::new(
                move |name,
                      _message_builder,
                      metrics_config_uuid,
                      reset_on_get,
                      triggers,
                      _status_reporter| {
                    assert!(data_holder_clone
                        .set(AggregationPublisherData { reset_on_get, triggers })
                        .is_ok());
                    Box::new(MockPublisher::new(name, metrics_config_uuid))
                },
            );
            data_holder
        }

        fn capture_service_publisher(&mut self) -> Arc<OnceLock<ServicePublisherData>> {
            let data_holder = Arc::new(OnceLock::new());
            let data_holder_clone = Arc::clone(&data_holder);
            self.service_publisher_new_func = Box::new(
                move |name,
                      fetch_last_message,
                      configuration,
                      connection_type,
                      metrics_config_uuid,
                      service_name,
                      sub_sampling_interval,
                      _node_evaluation_context_factory,
                      _service_adapter,
                      _status_reporter| {
                    assert!(data_holder_clone
                        .set(ServicePublisherData {
                            fetch_last_message,
                            configuration,
                            connection_type,
                            service_name,
                            sub_sampling_interval,
                        })
                        .is_ok());
                    Box::new(MockPublisher::new(name, metrics_config_uuid))
                },
            );
            data_holder
        }

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
            };
            self.create(CONFIG_UUID.to_owned(), publisher_proto, &context)
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
        let mut factory = create_default_factory();
        let data_holder = factory.capture_service_publisher();
        let publisher = create_service_publisher_with_unspecified_connection_type();

        let _res = factory.create_using_default_context(&publisher).unwrap();

        assert!(matches!(data_holder.get().unwrap().connection_type, ConnectionType::Subscription));
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
        let mut factory = create_default_factory();
        let data_holder = factory.capture_service_publisher();
        let publisher = create_valid_service_publisher();

        let res = factory.create_using_default_context(&publisher).unwrap();

        assert_eq!(res.get_name(), PUBLISHER_NAME);
        assert_eq!(res.get_metrics_config_uuid(), CONFIG_UUID);

        let data_holder_value = data_holder.get().unwrap();
        let configuration_value =
            data_holder_value.configuration.as_ref().expect("Configuration should be set");
        let any_value = configuration_value
            .unpack::<Empty>()
            .unwrap()
            .expect("The value of Any should be an Empty");
        assert_eq!(&any_value, Empty::default_instance());
        assert!(matches!(data_holder_value.connection_type, ConnectionType::Subscription));
        assert!(data_holder_value.fetch_last_message);
        assert_eq!(data_holder_value.service_name, SERVICE_NAME);
        assert_eq!(
            data_holder_value.sub_sampling_interval,
            SubSamplingInterval::try_from(Duration::from_secs(1)).unwrap()
        )
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
        let mut factory = create_default_factory();
        let data_holder = factory.capture_aggregation_publisher();
        let (publisher, trigger) = create_valid_aggregation_publisher();

        let res = factory
            .create_using_context_with(&publisher, Some(vec![Arc::clone(&trigger)]))
            .unwrap();

        assert_eq!(res.get_name(), PUBLISHER_NAME);
        assert_eq!(res.get_metrics_config_uuid(), CONFIG_UUID);
        let data_holder_value = data_holder.get().unwrap();
        assert!(data_holder_value.reset_on_get);
        assert_eq!(data_holder_value.triggers.len(), 1);
        assert!(Arc::ptr_eq(&data_holder_value.triggers[0], &(trigger as Arc<dyn Trigger>)));
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
