// Copyright 2023 Google LLC

//! Implementation of ServicePublisherAccessor trait that delegates execution
//! to the underlying implementations.

use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::result::Result;
use std::sync::{Arc, Mutex};

use crate::service_publisher_accessor::ServicePublisherAccessor;

/// Struct for managing all implementations of ServicePublisherAccessor trait.
pub struct ServicePublisherManager {
    /// Hashmap to store Service Publisher Accessors.
    service_publisher_registry: Vec<Arc<dyn ServicePublisherAccessor>>,
    /// Hashmap to store subscriptions.
    service_publisher_subscriptions: Arc<Mutex<HashMap<String, Arc<dyn ServicePublisherAccessor>>>>,
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl ServicePublisherManager {
    /// Creates a new instance of the struct.
    pub fn new(service_publisher_registry: Vec<Arc<dyn ServicePublisherAccessor>>) -> Self {
        // Create struct to interact with Data Tunnel Apis
        Self {
            service_publisher_registry,
            service_publisher_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
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

    async fn try_subscribe(
        &self,
        service_name: &str,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<bool, anyhow::Error> {
        for service_publisher_manager_impl in &self.service_publisher_registry {
            if service_publisher_manager_impl
                .try_subscribe(service_name, callback.clone(), configuration)
                .await?
            {
                self.service_publisher_subscriptions
                    .lock()
                    .unwrap()
                    .try_insert_value(
                        service_name.to_owned(),
                        service_publisher_manager_impl.clone(),
                    )
                    .unwrap_or_else(|_| {
                        panic!("Only one subscription per service name is supported")
                    });
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn unsubscribe(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
    ) -> Result<(), anyhow::Error> {
        let service_publisher = self
            .service_publisher_subscriptions
            .lock()
            .unwrap()
            .remove(service_name)
            .unwrap_or_else(|| {
                panic!("Publisher with service name {} does not exist", service_name)
            });

        service_publisher.unsubscribe(service_name, configuration).await?;
        Ok(())
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
            Arc::new(MockServicePublisherAccessor::new_with_descriptor(Empty::descriptor()));
        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res =
            service_publisher_manager_impl.get_message_descriptor(SERVICE_NAME).unwrap().unwrap();

        assert_eq!(service_publisher.get_num_get_message_descriptor_calls(), 1);
        assert_eq!(res, Empty::descriptor());
    }

    #[tokio::test]
    async fn get_message_descriptor_propagates_error() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::default());
        service_publisher.next_error.lock().unwrap().replace(anyhow!(ERROR_MESSAGE.to_owned()));
        let service_publishers = vec![service_publisher as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl.get_message_descriptor(SERVICE_NAME2).unwrap_err();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[tokio::test]
    async fn get_message_descriptor_no_match_returns_none() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::default());
        let service_publishers = vec![service_publisher as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl.get_message_descriptor(SERVICE_NAME2).unwrap();

        assert!(res.is_none(), "Unexpected result: {:?}", res);
    }

    #[tokio::test]
    async fn subscribe_success() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::default());
        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();

        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert!(res);
    }

    #[tokio::test]
    async fn subscribe_propagates_error() {
        let service_publisher = MockServicePublisherAccessor::default();
        service_publisher.next_error.lock().unwrap().replace(anyhow!(ERROR_MESSAGE.to_owned()));
        let service_publisher = Arc::new(service_publisher);

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherAccessor>];

        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME2, Arc::new(|_| {}), None)
            .await
            .err()
            .unwrap();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherAccessor>];

        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();
        let res = service_publisher_manager_impl.unsubscribe(SERVICE_NAME, None).await;

        assert!(res.is_ok());
        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(service_publisher.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_incorrect_service_name_fails() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherAccessor>];

        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();
        service_publisher_manager_impl.unsubscribe(SERVICE_NAME2, None).await.unwrap();
    }
}
