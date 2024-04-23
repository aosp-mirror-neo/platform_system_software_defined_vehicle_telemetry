// Copyright 2023 Google LLC

//! Implementation of ServicePublisherAccessor trait that delegates execution
//! to the underlying implementations.

use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::result::Result;
use std::sync::Arc;

use crate::service_publisher_accessor::{ServicePublisherAccessor, ServicePublisherSubscription};

/// Struct for managing all implementations of ServicePublisherAccessor trait.
pub struct ServicePublisherManager {
    /// Hashmap to store Service Publisher Accessors.
    service_publisher_registry: Vec<Arc<dyn ServicePublisherAccessor>>,
}

impl ServicePublisherManager {
    /// Creates a new instance of the struct.
    pub fn new(service_publisher_registry: Vec<Arc<dyn ServicePublisherAccessor>>) -> Self {
        Self { service_publisher_registry }
    }
}

#[async_trait]
impl ServicePublisherAccessor for ServicePublisherManager {
    fn get_message_descriptor(
        &self,
        service_name: &str,
    ) -> Result<Option<MessageDescriptor>, anyhow::Error> {
        for service_publisher_manager_impl in &self.service_publisher_registry {
            let msg_descriptor =
                service_publisher_manager_impl.get_message_descriptor(service_name)?;

            if msg_descriptor.is_some() {
                return Ok(msg_descriptor);
            }
        }

        Ok(None)
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> Result<Option<Box<dyn ServicePublisherSubscription>>, anyhow::Error> {
        for service_publisher_manager_impl in &self.service_publisher_registry {
            let maybe_subscription = service_publisher_manager_impl
                .create_subscription(service_name, configuration, Arc::clone(&callback))
                .await?;
            if maybe_subscription.is_some() {
                return Ok(maybe_subscription);
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use protobuf::well_known_types::empty::Empty;
    use protobuf::MessageFull;
    use std::sync::Arc;

    use super::*;
    use crate::service_publisher_accessor::mock::MockServicePublisherAccessor;

    const ERROR_MESSAGE: &str = "ERROR_MESSAGE";
    const SERVICE_NAME: &str = "TEST_SERVICE_NAME";
    const SERVICE_NAME2: &str = "TEST_SERVICE_NAME2";

    #[tokio::test]
    async fn get_message_descriptor_success() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_descriptor(Empty::descriptor()));
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res =
            service_publisher_manager_impl.get_message_descriptor(SERVICE_NAME).unwrap().unwrap();

        assert_eq!(service_publisher.get_num_get_message_descriptor_calls(), 1);
        assert_eq!(res, Empty::descriptor());
    }

    #[tokio::test]
    async fn get_message_descriptor_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl.get_message_descriptor(SERVICE_NAME2).unwrap_err();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[tokio::test]
    async fn get_message_descriptor_no_match_returns_none() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl.get_message_descriptor(SERVICE_NAME2).unwrap();

        assert!(res.is_none(), "Unexpected result: {:?}", res);
    }

    #[tokio::test]
    async fn create_subscription_success() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl
            .create_subscription(SERVICE_NAME, None, Arc::new(|_| {}))
            .await
            .unwrap();

        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert!(res.is_some());
    }

    #[tokio::test]
    async fn create_subscription_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl
            .create_subscription(SERVICE_NAME2, None, Arc::new(|_| {}))
            .await
            .err()
            .unwrap();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[tokio::test]
    async fn create_subscription_returns_valid_subscription() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());

        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];

        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let subscription = service_publisher_manager_impl
            .create_subscription(SERVICE_NAME, None, Arc::new(|_| {}))
            .await
            .unwrap()
            .unwrap();
        let res = subscription.unsubscribe().await;

        assert!(res.is_ok(), "Unexpected result: {:?}", res);
        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(service_publisher.get_num_unsubscribe_calls(), 1);
    }
}
