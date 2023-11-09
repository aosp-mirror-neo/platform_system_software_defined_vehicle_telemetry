use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::empty::Empty;
use protobuf::MessageFull;
use sdv_telemetry_configurable_publisher::configurable_publisher_registry::ConfigurablePublisherRegistry;
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto, Publisher as PublisherProto,
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::{ast::DescriptorProvider, node_errors::DescriptorProviderError};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum DescriptorProviderConstructorError {
    #[error("AggregationPublisher {pub_name} is invalid because it did not specify a ProtoMessageBuilder")]
    InvalidAggregationPublisher { pub_name: String },
    #[error(
        "AggregationPublisher {pub_name} is invalid because its field assignments \
    use missing message descriptor of type {msg_type}"
    )]
    MissingMessageDescriptor { pub_name: String, msg_type: String },
    #[error("No service publisher is registered for service {service_name} topic {topic_name}")]
    NoServicePublisherRegistration { service_name: String, topic_name: String },
}

pub struct DescriptorProviderImpl {
    // key = publisher name, value = MessageDescriptor for the data it publishes
    publisher_message_descriptor: HashMap<String, MessageDescriptor>,
}

impl DescriptorProvider for DescriptorProviderImpl {
    fn get_descriptor(
        &self,
        publisher_name: &str,
    ) -> Result<MessageDescriptor, DescriptorProviderError> {
        if let Some(msg_descriptor) = self.publisher_message_descriptor.get(publisher_name) {
            Ok(msg_descriptor.clone())
        } else {
            Err(DescriptorProviderError::InvalidPublisherName)
        }
    }
}

impl DescriptorProviderImpl {
    pub fn new(
        fds: &[FileDescriptor],
        publishers: &[PublisherProto],
        configurable_publisher_registry: &Arc<dyn ConfigurablePublisherRegistry>,
    ) -> Result<Self, DescriptorProviderConstructorError> {
        let publisher_message_descriptor =
            publishers.iter().try_fold(HashMap::new(), |mut map, publisher| {
                let descriptor = match &publisher.publisher_type {
                    Some(PublisherTypeProto::SignalPublisher(_)) => Empty::descriptor(),
                    Some(PublisherTypeProto::ServicePublisher(service_publisher_proto)) => {
                        configurable_publisher_registry
                            .get_message_descriptor(
                                service_publisher_proto.service_name.clone(),
                                service_publisher_proto.topic_name.clone(),
                            )
                            .ok_or_else(|| {
                                DescriptorProviderConstructorError::NoServicePublisherRegistration {
                                    service_name: service_publisher_proto.service_name.clone(),
                                    topic_name: service_publisher_proto.topic_name.clone(),
                                }
                            })?
                    }
                    Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher_proto)) => {
                        let message_builder =
                            aggregation_publisher_proto.message_builder.as_ref().ok_or(
                                DescriptorProviderConstructorError::InvalidAggregationPublisher {
                                    pub_name: publisher.name.clone(),
                                },
                            )?;
                        if let Some(msg_descriptor) = fds
                            .iter()
                            .find_map(|fd| fd.message_by_full_name(&message_builder.message_type))
                        {
                            msg_descriptor
                        } else {
                            return Err(
                                DescriptorProviderConstructorError::MissingMessageDescriptor {
                                    pub_name: publisher.name.clone(),
                                    msg_type: message_builder.message_type.clone(),
                                },
                            );
                        }
                    }

                    // Skip unknown publisher types.
                    _ => return Ok(map),
                };
                map.insert(publisher.name.clone(), descriptor);
                Ok(map)
            })?;
        Ok(DescriptorProviderImpl { publisher_message_descriptor })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use protobuf::reflect::MessageDescriptor;
    use protobuf::{MessageDyn, MessageFull};
    use sdv_telemetry_configurable_publisher::configurable_publisher_registry::ConfigurablePublisherRegistrySubscribeError;
    use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};
    use sdvtelemetry_proto_rust::metrics_configuration::{
        publisher::Publisher_type as PublisherTypeProto, AggregationPublisher, ProtoMessageBuilder,
    };
    use std::sync::Mutex;

    use super::*;
    use crate::metrics_config_orchestrator::message_builder_factory::mock::get_test_file_descriptors;

    const PUBLISHER_1: &str = "pub_1";
    const PUBLISHER_2: &str = "pub_2";
    const PUBLISHER_3: &str = "pub_3";
    const SERVICE_NAME: &str = "service";
    const TOPIC_NAME: &str = "topic";
    const TEST_MESSAGE_TYPE_NAME: &str = ".android.sdv.telemetry.test.TestMessage";
    const TEST_NESTED_MESSAGE_TYPE_NAME: &str = ".android.sdv.telemetry.test.TestNestedMessage";

    struct MockConfigurablePublisherRegistry {
        next_descriptor: Mutex<Option<MessageDescriptor>>,
    }

    #[async_trait]
    impl ConfigurablePublisherRegistry for MockConfigurablePublisherRegistry {
        fn get_message_descriptor(
            &self,
            _service_name: String,
            _topic_name: String,
        ) -> Option<MessageDescriptor> {
            self.next_descriptor.lock().unwrap().take()
        }

        async fn subscribe(
            &self,
            _service_name: String,
            _topic_name: String,
            _callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
            _configuration: Option<&[u8]>,
        ) -> Result<(), ConfigurablePublisherRegistrySubscribeError> {
            Ok(())
        }
    }

    fn create_default_publisher_registry() -> Arc<dyn ConfigurablePublisherRegistry> {
        create_publisher_registry(None)
    }

    fn create_publisher_registry(
        next_descriptor: Option<MessageDescriptor>,
    ) -> Arc<dyn ConfigurablePublisherRegistry> {
        Arc::new(MockConfigurablePublisherRegistry { next_descriptor: next_descriptor.into() })
    }

    fn create_aggregation_publisher_with_no_proto_message_builder(
        pub_name: &str,
    ) -> PublisherProto {
        let mut publisher = PublisherProto::new();
        publisher.name = pub_name.to_string();
        publisher.publisher_type =
            Some(PublisherTypeProto::AggregationPublisher(AggregationPublisher::new()));
        publisher
    }

    fn create_aggregation_publisher_with_msg_type(
        pub_name: &str,
        msg_type: &str,
    ) -> PublisherProto {
        let mut msg_builder = ProtoMessageBuilder::new();
        msg_builder.message_type = msg_type.to_string();

        let mut aggr_publisher = AggregationPublisher::new();
        aggr_publisher.message_builder = protobuf::MessageField::some(msg_builder);
        let pub_type = PublisherTypeProto::AggregationPublisher(aggr_publisher);

        let mut publisher = PublisherProto::new();
        publisher.name = pub_name.to_string();
        publisher.publisher_type = Some(pub_type);
        publisher
    }

    #[test]
    fn new_descriptor_provider_service_publisher_no_message_descriptor_fails() {
        let mut publisher = PublisherProto::new();
        let service_publisher = publisher.mut_service_publisher();
        service_publisher.service_name = SERVICE_NAME.to_owned();
        service_publisher.topic_name = TOPIC_NAME.to_owned();

        let err = DescriptorProviderImpl::new(
            get_test_file_descriptors(),
            &[publisher],
            &create_default_publisher_registry(),
        )
        .err()
        .unwrap();

        assert!(
            matches!(&err, DescriptorProviderConstructorError::NoServicePublisherRegistration { service_name, topic_name } if service_name == SERVICE_NAME && topic_name == TOPIC_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn new_descriptor_provider_aggregation_publisher_no_proto_message_builder_fails() {
        let publisher = create_aggregation_publisher_with_no_proto_message_builder(PUBLISHER_1);

        let err = DescriptorProviderImpl::new(
            get_test_file_descriptors(),
            &[publisher],
            &create_default_publisher_registry(),
        )
        .err()
        .unwrap();

        assert!(
            matches!(&err, DescriptorProviderConstructorError::InvalidAggregationPublisher { pub_name } if pub_name == PUBLISHER_1),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn get_descriptor_invalid_publisher_name_fails() {
        let descriptor_provider = DescriptorProviderImpl::new(
            get_test_file_descriptors(),
            &[],
            &create_default_publisher_registry(),
        )
        .unwrap();

        let err = descriptor_provider.get_descriptor(PUBLISHER_3).err().unwrap();

        assert!(
            matches!(&err, DescriptorProviderError::InvalidPublisherName),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn get_descriptor_service_publisher_success() {
        let mut publisher = PublisherProto::new();
        publisher.name = PUBLISHER_1.to_owned();
        publisher.mut_service_publisher();

        let descriptor_provider = DescriptorProviderImpl::new(
            get_test_file_descriptors(),
            &[publisher],
            &create_publisher_registry(Some(TestMessage::descriptor())),
        )
        .unwrap();

        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().proto(),
            TestMessage::descriptor().proto()
        )
    }

    #[test]
    fn get_descriptor_aggregation_publisher_success() {
        let publisher_1 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_1, TEST_MESSAGE_TYPE_NAME);
        let publisher_2 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_2, TEST_NESTED_MESSAGE_TYPE_NAME);

        let descriptor_provider = DescriptorProviderImpl::new(
            get_test_file_descriptors(),
            &[publisher_1, publisher_2],
            &create_default_publisher_registry(),
        )
        .unwrap();

        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().proto(),
            TestMessage::descriptor().proto()
        );
        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_2).unwrap().proto(),
            TestNestedMessage::descriptor().proto()
        );
    }
}
