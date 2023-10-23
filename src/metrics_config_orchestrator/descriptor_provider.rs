use protobuf::{
    reflect::{FileDescriptor, MessageDescriptor},
    well_known_types::wrappers::Int32Value,
    MessageDyn,
};
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto, Publisher as PublisherProto,
};
use std::collections::HashMap;
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
            return Ok(msg_descriptor.clone());
        }
        // service publisher publishing mocked data of Int32Value message
        let msg = Int32Value::new();
        Ok(msg.descriptor_dyn())
    }
}

impl DescriptorProviderImpl {
    pub fn new(
        fds: &[FileDescriptor],
        publishers: &[PublisherProto],
    ) -> Result<Self, DescriptorProviderConstructorError> {
        let mut publisher_message_descriptor = HashMap::new();
        for publisher in publishers {
            match &publisher.publisher_type {
                Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher_proto)) => {
                    let message_builder = aggregation_publisher_proto
                        .message_builder
                        .as_ref()
                        .ok_or(DescriptorProviderConstructorError::InvalidAggregationPublisher {
                            pub_name: publisher.name.clone(),
                        })?;
                    if let Some(msg_descriptor) = fds
                        .iter()
                        .find_map(|fd| fd.message_by_full_name(&message_builder.message_type))
                    {
                        publisher_message_descriptor
                            .insert(publisher.name.clone(), msg_descriptor.clone());
                    } else {
                        return Err(DescriptorProviderConstructorError::MissingMessageDescriptor {
                            pub_name: publisher.name.clone(),
                            msg_type: message_builder.message_type.clone(),
                        });
                    }
                }
                // currently only aggregation publishers have known descriptors
                _ => continue,
            };
        }
        Ok(DescriptorProviderImpl { publisher_message_descriptor })
    }
}

#[cfg(test)]
mod tests {
    use protobuf::MessageFull;
    use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};
    use sdvtelemetry_proto_rust::metrics_configuration::{
        publisher::Publisher_type as PublisherTypeProto, AggregationPublisher, ProtoMessageBuilder,
    };

    use super::*;
    use crate::metrics_config_orchestrator::message_builder_factory::mock::get_test_file_descriptors;

    const PUBLISHER_1: &str = "pub_1";
    const PUBLISHER_2: &str = "pub_2";
    const PUBLISHER_3: &str = "pub_3";
    const TEST_MESSAGE_TYPE_NAME: &str = ".android.sdv.telemetry.test.TestMessage";
    const TEST_NESTED_MESSAGE_TYPE_NAME: &str = ".android.sdv.telemetry.test.TestNestedMessage";

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
    fn test_constructor_and_get_descriptor_ok() {
        let publisher_1 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_1, TEST_MESSAGE_TYPE_NAME);
        let publisher_2 =
            create_aggregation_publisher_with_msg_type(PUBLISHER_2, TEST_NESTED_MESSAGE_TYPE_NAME);

        let descriptor_provider =
            DescriptorProviderImpl::new(get_test_file_descriptors(), &[publisher_1, publisher_2])
                .unwrap();

        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().proto(),
            TestMessage::descriptor().proto()
        );
        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_2).unwrap().proto(),
            TestNestedMessage::descriptor().proto()
        );
        // a temporary default descriptor is provided for all other publisher names,
        // because the toy service publisher publishes an integer
        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_3).unwrap().proto(),
            Int32Value::descriptor().proto()
        );
    }

    #[test]
    fn test_constructor_fails_when_aggregation_publisher_has_no_proto_message_builder() {
        let publisher = create_aggregation_publisher_with_no_proto_message_builder(PUBLISHER_1);

        let err =
            DescriptorProviderImpl::new(get_test_file_descriptors(), &[publisher]).err().unwrap();

        assert!(
            matches!(&err, DescriptorProviderConstructorError::InvalidAggregationPublisher { pub_name } if pub_name == PUBLISHER_1),
            "Unexpected error: {:?}",
            err
        );
    }
}
