// Copyright 2023 Google LLC

//! Sample Wrapper for DT.

use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::collections::HashMap;
use std::result::Result;
use std::sync::{Arc, Mutex};

// use sdv_telemetry::{ServicePublisherManager, ServicePublisherManagerSubscribeError};
use crate::{ServicePublisherManager, ServicePublisherManagerSubscribeError};

/// Struct for managing all implementations of ServicePublisherManager trait
pub struct ServicePublisherManagerImpl {
    /// Hashmap to store Service Publisher Manager Implementation
    service_publisher_registry: Vec<Arc<dyn ServicePublisherManager>>,
    /// Hashmap to store subscriptions
    service_publisher_subscriptions:
        Arc<Mutex<HashMap<(String, String), Arc<dyn ServicePublisherManager>>>>,
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl ServicePublisherManagerImpl {
    /// Creates a new instance of the struct.
    pub fn new(service_publisher_registry: Vec<Arc<dyn ServicePublisherManager>>) -> Self {
        // Create struct to interact with Data Tunnel Apis
        Self {
            service_publisher_registry,
            service_publisher_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ServicePublisherManager for ServicePublisherManagerImpl {
    /// Subscribe to a Data Tunnel topic sample.
    /// The method returns a struct containing the topic information and the subscription topic handle.
    /// Returns a DT reader implementation that can be used to read messages.
    async fn try_subscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<bool, ServicePublisherManagerSubscribeError> {
        for service_publisher_manager_impl in &self.service_publisher_registry {
            match service_publisher_manager_impl
                .try_subscribe(service_name, topic_name, callback.clone(), configuration)
                .await
            {
                Ok(true) => {
                    let _ = self.service_publisher_subscriptions.lock().unwrap().insert(
                        (service_name.to_string(), topic_name.to_string()),
                        service_publisher_manager_impl.clone(),
                    );
                    return Ok(true);
                }
                Ok(false) => (),
                Err(err) => return Err(err),
            }
        }

        Err(ServicePublisherManagerSubscribeError::TopicNotFound {
            service_name: service_name.to_string(),
            topic_name: topic_name.to_string(),
        })
    }

    /// Unsubscribes from the specified topic and service name.
    async fn unsubscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        configuration: Option<&[u8]>,
    ) -> Result<(), ServicePublisherManagerSubscribeError> {
        let service_publisher = self
            .service_publisher_subscriptions
            .lock()
            .unwrap()
            .get(&(service_name.to_string(), topic_name.to_string()))
            .unwrap_or_else(|| {
                panic!("Publisher with {} and {} does not exist", service_name, topic_name)
            })
            .clone();

        service_publisher.unsubscribe(service_name, topic_name, configuration).await?;

        self.service_publisher_subscriptions
            .lock()
            .unwrap()
            .remove(&(service_name.to_string(), topic_name.to_string()));
        Ok(())
    }

    /// Checks if topic with given name exists in data tunnel
    fn get_message_descriptor(
        &self,
        service_name: &str,
        topic_name: &str,
    ) -> Result<MessageDescriptor, ServicePublisherManagerSubscribeError> {
        for service_publisher_manager_impl in &self.service_publisher_registry {
            if let Ok(msg_descriptor) =
                service_publisher_manager_impl.get_message_descriptor(service_name, topic_name)
            {
                return Ok(msg_descriptor);
            }
        }

        Err(ServicePublisherManagerSubscribeError::TopicNotFound {
            service_name: service_name.to_string(),
            topic_name: topic_name.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {

    use protobuf::well_known_types::empty::Empty;
    use protobuf::MessageFull;
    use std::sync::Arc;

    use super::*;
    use crate::service_publisher_manager::mock::MockServicePublisherManager;

    const SERVICE_NAME: &str = "TEST_SERVICE_NAME";
    const TOPIC_NAME: &str = "TEST_TOPIC_NAME";

    const SERVICE_NAME2: &str = "TEST_SERVICE_NAME2";
    const TOPIC_NAME2: &str = "TEST_TOPIC_NAME2";

    #[tokio::test]
    async fn get_message_descriptor_success() {
        let service_publisher =
            Arc::new(MockServicePublisherManager::new_with_descriptor(Empty::descriptor()));

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        let res = service_publisher_manager_impl
            .get_message_descriptor(SERVICE_NAME, TOPIC_NAME)
            .unwrap();

        assert_eq!(service_publisher.get_num_get_message_descriptor_calls(), 1);
        assert_eq!(res, Empty::descriptor());
    }

    #[tokio::test]
    async fn get_message_descriptor_incorrect_service_name_fails() {
        let service_publisher = Arc::new(MockServicePublisherManager::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        let err = service_publisher_manager_impl
            .get_message_descriptor(SERVICE_NAME2, TOPIC_NAME)
            .err()
            .unwrap();

        assert!(
            matches!(&err, ServicePublisherManagerSubscribeError::TopicNotFound { service_name, topic_name } if service_name == SERVICE_NAME2 && topic_name == TOPIC_NAME),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn get_message_descriptor_incorrect_topic_name_fails() {
        let service_publisher = Arc::new(MockServicePublisherManager::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        let err = service_publisher_manager_impl
            .get_message_descriptor(SERVICE_NAME, TOPIC_NAME2)
            .err()
            .unwrap();

        assert!(
            matches!(&err, ServicePublisherManagerSubscribeError::TopicNotFound { service_name, topic_name } if service_name == SERVICE_NAME && topic_name == TOPIC_NAME2),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn subscribe_success() {
        let service_publisher = Arc::new(MockServicePublisherManager::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        let res = service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, TOPIC_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();

        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert!(res);
    }

    #[tokio::test]
    async fn subscribe_incorrect_service_name_fails() {
        let service_publisher = MockServicePublisherManager::default();
        service_publisher.next_error.lock().unwrap().replace(
            ServicePublisherManagerSubscribeError::TopicNotFound {
                service_name: SERVICE_NAME2.to_owned(),
                topic_name: TOPIC_NAME.to_owned(),
            },
        );
        let service_publisher = Arc::new(service_publisher);

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        let err = service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME2, TOPIC_NAME, Arc::new(|_| {}), None)
            .await
            .err()
            .unwrap();

        assert!(
            matches!(&err, ServicePublisherManagerSubscribeError::TopicNotFound { service_name, topic_name } if service_name == SERVICE_NAME2 && topic_name == TOPIC_NAME),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn subscribe_incorrect_topic_name_fails() {
        let service_publisher = MockServicePublisherManager::default();
        service_publisher.next_error.lock().unwrap().replace(
            ServicePublisherManagerSubscribeError::TopicNotFound {
                service_name: SERVICE_NAME.to_owned(),
                topic_name: TOPIC_NAME2.to_owned(),
            },
        );
        let service_publisher = Arc::new(service_publisher);

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        let err = service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, TOPIC_NAME2, Arc::new(|_| {}), None)
            .await
            .err()
            .unwrap();

        assert!(
            matches!(&err, ServicePublisherManagerSubscribeError::TopicNotFound { service_name, topic_name } if service_name == SERVICE_NAME && topic_name == TOPIC_NAME2),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let service_publisher = Arc::new(MockServicePublisherManager::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, TOPIC_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();
        let res = service_publisher_manager_impl.unsubscribe(SERVICE_NAME, TOPIC_NAME, None).await;

        assert!(res.is_ok());
        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(service_publisher.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_incorrect_service_name_fails() {
        let service_publisher = Arc::new(MockServicePublisherManager::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, TOPIC_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();

        service_publisher_manager_impl.unsubscribe(SERVICE_NAME2, TOPIC_NAME, None).await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_incorrect_topic_name_fails() {
        let service_publisher = Arc::new(MockServicePublisherManager::default());

        let service_publishers =
            vec![service_publisher.clone() as Arc<dyn ServicePublisherManager>];

        let service_publisher_manager_impl = ServicePublisherManagerImpl::new(service_publishers);

        service_publisher_manager_impl
            .try_subscribe(SERVICE_NAME, TOPIC_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();
        let err = service_publisher_manager_impl
            .unsubscribe(SERVICE_NAME, TOPIC_NAME2, None)
            .await
            .err()
            .unwrap();

        assert!(
            matches!(&err, ServicePublisherManagerSubscribeError::TopicNotFound { service_name, topic_name } if service_name == SERVICE_NAME && topic_name == TOPIC_NAME2),
            "Unexpected error: {}",
            err
        );
    }
}
