// Copyright 2023 Google LLC

use async_trait::async_trait;
use protobuf::reflect::FileDescriptor;
use sdv_telemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto, Publisher as PublisherProto,
};
use sdv_telemetry_service_publisher_accessor::{
    ServicePublisherAccessor, ServicePublisherAccessorError,
};
use sdv_telemetry_utils::descriptor_utils;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::node_factory::DescriptorProvider;
use crate::metrics_config_orchestrator::descriptor_provider::DescriptorProviderImpl;

#[async_trait]
pub(crate) trait DescriptorProviderFactory: Send + Sync {
    async fn create(
        &self,
        fds: &[FileDescriptor],
        publishers: &[PublisherProto],
        service_publisher_manager: &Arc<dyn ServicePublisherAccessor>,
    ) -> Result<Box<dyn DescriptorProvider>, DescriptorProviderFactoryError>;
}

#[derive(Error, Debug)]
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
        error: ServicePublisherAccessorError,
    },
}

pub(crate) struct DescriptorProviderFactoryImpl;

#[async_trait]
impl DescriptorProviderFactory for DescriptorProviderFactoryImpl {
    async fn create(
        &self,
        fds: &[FileDescriptor],
        publishers: &[PublisherProto],
        service_publisher_manager: &Arc<dyn ServicePublisherAccessor>,
    ) -> Result<Box<dyn DescriptorProvider>, DescriptorProviderFactoryError> {
        let mut publisher_message_descriptor = HashMap::new();
        for publisher in publishers {
            let descriptor = match &publisher.publisher_type {
                Some(PublisherTypeProto::ServicePublisher(service_publisher_proto)) => {
                    service_publisher_manager
                        .get_metadata(&service_publisher_proto.service_name)
                        .await?
                        .map(|metadata| metadata.message_descriptor)
                }
                Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher_proto)) => {
                    let message_builder = aggregation_publisher_proto
                        .message_builder
                        .as_ref()
                        .ok_or(DescriptorProviderFactoryError::InvalidAggregationPublisher {
                            pub_name: publisher.name.clone(),
                        })?;
                    let descriptor = descriptor_utils::find_message_by_full_name(
                        fds,
                        &message_builder.message_type,
                    )
                    .ok_or(
                        DescriptorProviderFactoryError::MissingMessageDescriptor {
                            pub_name: publisher.name.clone(),
                            msg_type: message_builder.message_type.clone(),
                        },
                    )?;
                    Some(descriptor)
                }
                // Skip unknown publisher types.
                _ => continue,
            };
            publisher_message_descriptor.insert(publisher.name.clone(), descriptor);
        }
        Ok(Box::new(DescriptorProviderImpl { publisher_message_descriptor }))
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use protobuf::reflect::MessageDescriptor;
    use protobuf::MessageFull;
    use sdv_telemetry_proto_rust::metrics_configuration::{
        publisher::Publisher_type as PublisherTypeProto, AggregationPublisher, ProtoMessageBuilder,
    };
    use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};

    use crate::expr::node_error::DescriptorProviderError;
    use crate::metrics_config_orchestrator::message_builder_factory::mock::get_test_file_descriptors;
    use crate::mock_service_publisher_accessor::MockServicePublisherAccessor;

    const PUBLISHER_1: &str = "pub_1";
    const PUBLISHER_2: &str = "pub_2";
    const PUBLISHER_3: &str = "pub_3";
    const SERVICE_NAME: &str = "service";
    const TEST_MESSAGE_TYPE_NAME: &str = ".google.sdv.telemetry.test.TestMessage";
    const TEST_NESTED_MESSAGE_TYPE_NAME: &str = ".google.sdv.telemetry.test.TestNestedMessage";

    fn create_default_service_publisher_accessor() -> Arc<dyn ServicePublisherAccessor> {
        Arc::new(MockServicePublisherAccessor::new())
    }

    fn create_service_publisher_accessor(
        next_descriptor: MessageDescriptor,
    ) -> Arc<dyn ServicePublisherAccessor> {
        Arc::new(MockServicePublisherAccessor::new().with_next_descriptor(next_descriptor))
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_descriptor_provider_service_publisher_manager_error_fails() {
        let service_publisher_manager =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!("Error")));

        let mut publisher = PublisherProto::new();
        let service_publisher = publisher.mut_service_publisher();
        SERVICE_NAME.clone_into(&mut service_publisher.service_name);

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let err = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher],
                &(service_publisher_manager as Arc<dyn ServicePublisherAccessor>),
            )
            .await
            .err()
            .unwrap();

        assert!(
            matches!(&err, DescriptorProviderFactoryError::ServicePublisherManagerError { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_descriptor_provider_aggregation_publisher_no_proto_message_builder_fails() {
        let publisher = create_aggregation_publisher_with_no_proto_message_builder(PUBLISHER_1);

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let err = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher],
                &create_default_service_publisher_accessor(),
            )
            .await
            .err()
            .unwrap();

        assert!(
            matches!(&err, DescriptorProviderFactoryError::InvalidAggregationPublisher { pub_name } if pub_name == PUBLISHER_1),
            "Unexpected error: {:?}",
            err
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_descriptor_invalid_publisher_name_fails() {
        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let descriptor_provider = descriptor_provider_factory
            .create(get_test_file_descriptors(), &[], &create_default_service_publisher_accessor())
            .await
            .unwrap();

        let err = descriptor_provider.get_descriptor(PUBLISHER_3).unwrap_err();

        assert!(
            matches!(&err, DescriptorProviderError::InvalidPublisherName),
            "Unexpected error: {}",
            err
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_descriptor_service_publisher_success() {
        let mut publisher = PublisherProto::new();
        PUBLISHER_1.clone_into(&mut publisher.name);
        publisher.mut_service_publisher();

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let descriptor_provider = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher],
                &create_service_publisher_accessor(TestMessage::descriptor()),
            )
            .await
            .unwrap();

        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().unwrap().proto(),
            TestMessage::descriptor().proto()
        )
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_descriptor_aggregation_publisher_success() {
        let publisher_1 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_1, TEST_MESSAGE_TYPE_NAME);
        let publisher_2 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_2, TEST_NESTED_MESSAGE_TYPE_NAME);

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let descriptor_provider = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher_1, publisher_2],
                &create_default_service_publisher_accessor(),
            )
            .await
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_descriptor_aggregation_publisher_no_leading_dot_success() {
        let publisher = create_aggregation_publisher_with_msg_type(
            PUBLISHER_1,
            TEST_MESSAGE_TYPE_NAME.strip_prefix('.').unwrap(),
        );

        let descriptor_provider_factory = DescriptorProviderFactoryImpl;
        let descriptor_provider = descriptor_provider_factory
            .create(
                get_test_file_descriptors(),
                &[publisher],
                &create_default_service_publisher_accessor(),
            )
            .await
            .unwrap();

        let res = descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().unwrap();

        assert_eq!(res.proto(), TestMessage::descriptor().proto());
    }
}
