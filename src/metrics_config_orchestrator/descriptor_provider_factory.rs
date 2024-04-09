// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::FileDescriptor;
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto, Publisher as PublisherProto,
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::ast::DescriptorProvider;
use crate::metrics_config_orchestrator::descriptor_provider::DescriptorProviderImpl;
use crate::service_publisher_manager::ServicePublisherManager;

pub(crate) trait DescriptorProviderFactory: Send + Sync {
    fn create(
        &self,
        fds: &[FileDescriptor],
        publishers: &[PublisherProto],
        service_publisher_manager: &Arc<dyn ServicePublisherManager>,
    ) -> Result<Box<dyn DescriptorProvider>, DescriptorProviderFactoryError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum DescriptorProviderFactoryError {
    #[error("AggregationPublisher {pub_name} is invalid because it did not specify a ProtoMessageBuilder")]
    InvalidAggregationPublisher { pub_name: String },
    #[error(
        "AggregationPublisher {pub_name} is invalid because its field assignments \
    use missing message descriptor of type {msg_type}"
    )]
    MissingMessageDescriptor { pub_name: String, msg_type: String },
    #[error("Error when calling ServicePublisherManager: {error}")]
    ServicePublisherManagerError {
        #[from]
        error: anyhow::Error,
    },
}

pub(crate) struct DescriptorProviderFactoryImpl;

impl DescriptorProviderFactory for DescriptorProviderFactoryImpl {
    fn create(
        &self,
        fds: &[FileDescriptor],
        publishers: &[PublisherProto],
        service_publisher_manager: &Arc<dyn ServicePublisherManager>,
    ) -> Result<Box<dyn DescriptorProvider>, DescriptorProviderFactoryError> {
        let publisher_message_descriptor =
            publishers.iter().try_fold(HashMap::new(), |mut map, publisher| {
                let descriptor = match &publisher.publisher_type {
                    Some(PublisherTypeProto::ServicePublisher(service_publisher_proto)) => {
                        service_publisher_manager.get_message_descriptor(
                            &service_publisher_proto.service_name,
                            &service_publisher_proto.topic_name,
                        )?
                    }
                    Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher_proto)) => {
                        let message_builder = aggregation_publisher_proto
                            .message_builder
                            .as_ref()
                            .ok_or(DescriptorProviderFactoryError::InvalidAggregationPublisher {
                                pub_name: publisher.name.clone(),
                            })?;
                        if let Some(msg_descriptor) = fds
                            .iter()
                            .find_map(|fd| fd.message_by_full_name(&message_builder.message_type))
                        {
                            Some(msg_descriptor)
                        } else {
                            return Err(DescriptorProviderFactoryError::MissingMessageDescriptor {
                                pub_name: publisher.name.clone(),
                                msg_type: message_builder.message_type.clone(),
                            });
                        }
                    }
                    // Skip unknown publisher types.
                    _ => return Ok(map),
                };
                map.insert(publisher.name.clone(), descriptor);
                Ok(map)
            })?;
        Ok(Box::new(DescriptorProviderImpl { publisher_message_descriptor }))
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use protobuf::reflect::MessageDescriptor;
    use protobuf::MessageFull;
    use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};
    use sdvtelemetry_proto_rust::metrics_configuration::{
        publisher::Publisher_type as PublisherTypeProto, AggregationPublisher, ProtoMessageBuilder,
    };

    use super::*;
    use crate::expr::node_errors::DescriptorProviderError;
    use crate::metrics_config_orchestrator::message_builder_factory::mock::get_test_file_descriptors;
    use crate::service_publisher_manager::mock::MockServicePublisherManager;

    const PUBLISHER_1: &str = "pub_1";
    const PUBLISHER_2: &str = "pub_2";
    const PUBLISHER_3: &str = "pub_3";
    const SERVICE_NAME: &str = "service";
    const TOPIC_NAME: &str = "topic";
    const TEST_MESSAGE_TYPE_NAME: &str = ".google.sdv.telemetry.test.TestMessage";
    const TEST_NESTED_MESSAGE_TYPE_NAME: &str = ".google.sdv.telemetry.test.TestNestedMessage";

    fn create_default_service_publisher_manager() -> Arc<dyn ServicePublisherManager> {
        create_service_publisher_manager(None)
    }

    fn create_service_publisher_manager(
        next_descriptor: Option<MessageDescriptor>,
    ) -> Arc<dyn ServicePublisherManager> {
        Arc::new(MockServicePublisherManager::new(next_descriptor))
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
    fn create_descriptor_provider_service_publisher_manager_error_fails() {
        let service_publisher_manager = Arc::new(MockServicePublisherManager::new(None));
        service_publisher_manager.next_error.lock().unwrap().replace(anyhow!("Error"));

        let mut publisher = PublisherProto::new();
        let service_publisher = publisher.mut_service_publisher();
        service_publisher.service_name = SERVICE_NAME.to_owned();
        service_publisher.topic_name = TOPIC_NAME.to_owned();

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let err = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher],
                &(service_publisher_manager as Arc<dyn ServicePublisherManager>),
            )
            .err()
            .unwrap();

        assert!(
            matches!(&err, DescriptorProviderFactoryError::ServicePublisherManagerError { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_descriptor_provider_aggregation_publisher_no_proto_message_builder_fails() {
        let publisher = create_aggregation_publisher_with_no_proto_message_builder(PUBLISHER_1);

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let err = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher],
                &create_default_service_publisher_manager(),
            )
            .err()
            .unwrap();

        assert!(
            matches!(&err, DescriptorProviderFactoryError::InvalidAggregationPublisher { pub_name } if pub_name == PUBLISHER_1),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn get_descriptor_invalid_publisher_name_fails() {
        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let descriptor_provider = descriptor_provider_factory
            .create(get_test_file_descriptors(), &[], &create_default_service_publisher_manager())
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

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let descriptor_provider = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher],
                &create_service_publisher_manager(Some(TestMessage::descriptor())),
            )
            .unwrap();

        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().unwrap().proto(),
            TestMessage::descriptor().proto()
        )
    }

    #[test]
    fn get_descriptor_aggregation_publisher_success() {
        let publisher_1 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_1, TEST_MESSAGE_TYPE_NAME);
        let publisher_2 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_2, TEST_NESTED_MESSAGE_TYPE_NAME);

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let descriptor_provider = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher_1, publisher_2],
                &create_default_service_publisher_manager(),
            )
            .unwrap();

        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().unwrap().proto(),
            TestMessage::descriptor().proto()
        );
        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_2).unwrap().unwrap().proto(),
            TestNestedMessage::descriptor().proto()
        );
    }
}
