// Copyright 2023 Google LLC

//! Implementation of ServicePublisherAccessor trait that delegates execution
//! to the underlying implementations.

use async_trait::async_trait;
use protobuf::MessageDyn;
use std::sync::Arc;

use crate::service_publisher_accessor::{
    CreateSubscriptionOutcome, ServicePublisherAccessor, ServicePublisherMessageProvider,
    ServicePublisherMetadata,
};

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
    fn get_metadata(&self, service_name: &str) -> anyhow::Result<Option<ServicePublisherMetadata>> {
        for accessor in &self.service_publisher_registry {
            let metadata = accessor.get_metadata(service_name)?;

            if metadata.is_some() {
                return Ok(metadata);
            }
        }

        Ok(None)
    }

    fn get_message_provider(
        &self,
        service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServicePublisherMessageProvider>>> {
        for accessor in &self.service_publisher_registry {
            let provider = accessor.get_message_provider(service_name)?;

            if provider.is_some() {
                return Ok(provider);
            }
        }

        Ok(None)
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
        mut callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        for service_publisher_manager_impl in &self.service_publisher_registry {
            callback = match service_publisher_manager_impl
                .create_subscription(service_name, configuration, callback)
                .await?
            {
                outcome @ CreateSubscriptionOutcome::Subscribed(_) => return Ok(outcome),
                CreateSubscriptionOutcome::NotRegistered(callback) => callback,
            };
        }

        Ok(CreateSubscriptionOutcome::NotRegistered(callback))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use protobuf::well_known_types::empty::Empty;
    use protobuf::MessageFull;
    use sdv_telemetry_utils::test_utils::create_empty_fn_box;
    use std::sync::Arc;

    use super::*;
    use crate::service_publisher_accessor::mock::MockServicePublisherAccessor;

    const ERROR_MESSAGE: &str = "ERROR_MESSAGE";
    const SERVICE_NAME: &str = "TEST_SERVICE_NAME";
    const SERVICE_NAME2: &str = "TEST_SERVICE_NAME2";

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_success() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_descriptor(Empty::descriptor()));
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl.get_metadata(SERVICE_NAME).unwrap().unwrap();

        assert_eq!(service_publisher.get_num_get_metadata_calls(), 1);
        assert_eq!(res.message_descriptor, Empty::descriptor());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl.get_metadata(SERVICE_NAME2).err().unwrap();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_no_match_returns_none() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl.get_metadata(SERVICE_NAME2).unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_success() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        service_publisher_manager_impl.get_message_provider(SERVICE_NAME).unwrap().unwrap();

        assert_eq!(service_publisher.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl.get_message_provider(SERVICE_NAME2).err().unwrap();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_no_match_returns_none() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl.get_message_provider(SERVICE_NAME2).unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_success() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        service_publisher_manager_impl
            .create_subscription(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap();

        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl
            .create_subscription(SERVICE_NAME2, None, create_empty_fn_box())
            .await
            .err()
            .unwrap();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_no_match_returns_not_available() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl
            .create_subscription(SERVICE_NAME2, None, create_empty_fn_box())
            .await
            .unwrap();

        assert!(matches!(res, CreateSubscriptionOutcome::NotRegistered(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_returns_valid_subscription() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());

        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];

        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let subscription = service_publisher_manager_impl
            .create_subscription(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap()
            .unwrap();
        let res = subscription.unsubscribe().await;

        assert!(res.is_ok(), "Unexpected result: {:?}", res);
        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(service_publisher.get_num_unsubscribe_calls(), 1);
    }
}
