// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::MessageDescriptor;
use std::collections::HashMap;
use thiserror::Error;

use crate::expr::{ast::DescriptorProvider, node_errors::DescriptorProviderError};
use crate::service_publisher_manager::ServicePublisherManagerSubscribeError;

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
    #[error("Unable to validate the Configurable Publisher: {error}")]
    PublisherRegistryError {
        #[from]
        error: ServicePublisherManagerSubscribeError,
    },
}

pub struct DescriptorProviderImpl {
    // key = publisher name, value = MessageDescriptor for the data it publishes
    pub(super) publisher_message_descriptor: HashMap<String, MessageDescriptor>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::MessageFull;
    use sdv_telemetry_test_proto::test_message::TestMessage;

    const PUBLISHER_1: &str = "pub_1";
    const PUBLISHER_2: &str = "pub_2";

    // Function to create DescriptorProviderImpl instance for testing
    fn create_descriptor_provider() -> DescriptorProviderImpl {
        // Add publishers and publisher_message_descriptor to the map
        let publisher_message_descriptor =
            HashMap::from([(PUBLISHER_1.to_owned(), TestMessage::descriptor())]);

        // Return the instance
        DescriptorProviderImpl { publisher_message_descriptor }
    }

    #[test]
    fn get_descriptor_valid_publisher_name_success() {
        let descriptor_provider = create_descriptor_provider();

        assert_eq!(
            descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().proto(),
            TestMessage::descriptor().proto()
        )
    }

    #[test]
    fn get_descriptor_invalid_publisher_name_fails() {
        let descriptor_provider = create_descriptor_provider();
        let err = descriptor_provider.get_descriptor(PUBLISHER_2).err().unwrap();

        assert!(
            matches!(&err, DescriptorProviderError::InvalidPublisherName),
            "Unexpected error: {}",
            err
        );
    }
}
