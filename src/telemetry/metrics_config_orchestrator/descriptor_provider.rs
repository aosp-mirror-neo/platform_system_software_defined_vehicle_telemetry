// Copyright 2023 Google LLC

use protobuf::reflect::MessageDescriptor;
use sdv_telemetry_expr::{node_error::DescriptorProviderError, node_factory::DescriptorProvider};
use std::collections::HashMap;

pub(crate) struct DescriptorProviderImpl {
    // key = publisher name, value = MessageDescriptor for the data it publishes
    // value may be None if runtime type information for the publisher is not available yet.
    pub(super) publisher_message_descriptor: HashMap<String, Option<MessageDescriptor>>,
}

impl DescriptorProvider for DescriptorProviderImpl {
    fn get_descriptor(
        &self,
        publisher_name: &str,
    ) -> Result<Option<&MessageDescriptor>, DescriptorProviderError> {
        if let Some(descriptor) = self.publisher_message_descriptor.get(publisher_name) {
            Ok(descriptor.as_ref())
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
            HashMap::from([(PUBLISHER_1.to_owned(), Some(TestMessage::descriptor()))]);

        // Return the instance
        DescriptorProviderImpl { publisher_message_descriptor }
    }

    #[test]
    fn get_descriptor_valid_publisher_name_success() {
        let descriptor_provider = create_descriptor_provider();

        let res = descriptor_provider.get_descriptor(PUBLISHER_1).unwrap().unwrap().proto();

        assert_eq!(res, TestMessage::descriptor().proto())
    }

    #[test]
    fn get_descriptor_invalid_publisher_name_fails() {
        let descriptor_provider = create_descriptor_provider();

        let err = descriptor_provider.get_descriptor(PUBLISHER_2).unwrap_err();

        assert!(
            matches!(&err, DescriptorProviderError::InvalidPublisherName),
            "Unexpected error: {}",
            err
        );
    }
}
