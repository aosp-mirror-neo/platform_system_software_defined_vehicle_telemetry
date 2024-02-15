// Copyright 2023 Google LLC

//! Sample Wrapper for DT.

use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::collections::HashMap;
use std::result::Result;
use std::sync::{Arc, Mutex};

use sdv_telemetry::{ServicePublisherManager, ServicePublisherManagerSubscribeError};

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
        ServicePublisherManagerImpl {
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

        service_publisher.unsubscribe(topic_name, service_name, configuration).await?;

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
