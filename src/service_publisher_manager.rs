// Copyright 2023 Google LLC

//! Implementation of ServicePublisherAccessor trait that delegates execution
//! to the underlying implementations.

use async_trait::async_trait;
use futures::future;
use protobuf::{well_known_types::any::Any, MessageDyn};
use std::sync::Arc;

use crate::metrics_config_runtime_error::ServiceRuntimeError;
use crate::service_publisher_accessor::{
    AvailabilityCallbackRegistration, CreateSubscriptionError, CreateSubscriptionOutcome,
    GetMessageProviderError, GetMessageProviderOutcome, GetMetadataError, ServicePublisherAccessor,
    ServicePublisherMetadata,
};
use crate::sub_sampling_interval::SubSamplingInterval;

/// Struct for managing all implementations of ServicePublisherAccessor trait.
pub struct ServicePublisherManager {
    /// Hashmap to store Service Publisher Accessors.
    service_publisher_registry: Vec<Arc<dyn ServicePublisherAccessor>>,
}

struct AggregateAvailabilityCallbackRegistration {
    registrations: Vec<Box<dyn AvailabilityCallbackRegistration>>,
}

impl ServicePublisherManager {
    /// Creates a new instance of the struct.
    pub fn new(service_publisher_registry: Vec<Arc<dyn ServicePublisherAccessor>>) -> Self {
        Self { service_publisher_registry }
    }
}

#[async_trait]
impl ServicePublisherAccessor for ServicePublisherManager {
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> Result<Option<ServicePublisherMetadata>, GetMetadataError> {
        for accessor in &self.service_publisher_registry {
            let metadata = accessor.get_metadata(service_name).await?;

            if metadata.is_some() {
                return Ok(metadata);
            }
        }

        Ok(None)
    }

    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, GetMessageProviderError> {
        let mut get_not_supported = false;

        for accessor in &self.service_publisher_registry {
            match accessor.get_message_provider(service_name).await? {
                outcome @ GetMessageProviderOutcome::Success(_) => return Ok(outcome),
                GetMessageProviderOutcome::NotRegistered => {}
                GetMessageProviderOutcome::GetNotSupported => get_not_supported = true,
            }
        }

        Ok(if get_not_supported {
            GetMessageProviderOutcome::GetNotSupported
        } else {
            GetMessageProviderOutcome::NotRegistered
        })
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        mut new_message_callback: Box<
            dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync,
        >,
        mut cancellation_callback: Box<dyn Fn() + Send + Sync>,
    ) -> Result<CreateSubscriptionOutcome, CreateSubscriptionError> {
        for accessor in &self.service_publisher_registry {
            (new_message_callback, cancellation_callback) = match accessor
                .create_subscription(
                    service_name,
                    configuration,
                    sub_sampling_interval,
                    new_message_callback,
                    cancellation_callback,
                )
                .await?
            {
                outcome @ CreateSubscriptionOutcome::Subscribed(_) => return Ok(outcome),
                CreateSubscriptionOutcome::NotRegistered {
                    new_message_callback,
                    cancellation_callback,
                } => (new_message_callback, cancellation_callback),
            };
        }

        Ok(CreateSubscriptionOutcome::NotRegistered { new_message_callback, cancellation_callback })
    }

    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: &Arc<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        let mut registrations = Vec::new();

        for accessor in &self.service_publisher_registry {
            match accessor.register_availability_callback(service_name, callback).await {
                Ok(None) => {}
                Ok(Some(registration)) => {
                    registrations.push(registration);
                }
                Err(err) => {
                    for registration in registrations {
                        registration.unregister().await;
                    }
                    return Err(err);
                }
            }
        }

        if registrations.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Box::new(AggregateAvailabilityCallbackRegistration { registrations })))
        }
    }
}

#[async_trait]
impl AvailabilityCallbackRegistration for AggregateAvailabilityCallbackRegistration {
    async fn unregister(self: Box<Self>) {
        let unregister_tasks =
            self.registrations.into_iter().map(|registration| registration.unregister());
        future::join_all(unregister_tasks).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use protobuf::well_known_types::{empty::Empty, wrappers::Int32Value};
    use protobuf::MessageFull;
    use std::sync::Arc;

    use crate::service_publisher_accessor::mock::MockServicePublisherAccessor;

    const ERROR_MESSAGE: &str = "ERROR_MESSAGE";
    const SERVICE_NAME: &str = "TEST_SERVICE_NAME";
    const SERVICE_NAME2: &str = "TEST_SERVICE_NAME2";

    fn create_empty_fn_box() -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| unreachable!())
    }

    fn create_empty_message_handler(
    ) -> Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync> {
        Box::new(|_| unreachable!())
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_success() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_descriptor(Empty::descriptor()));
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl.get_metadata(SERVICE_NAME).await.unwrap().unwrap();

        assert_eq!(service_publisher.get_num_get_metadata_calls(), 1);
        assert_eq!(res.message_descriptor, Empty::descriptor());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl.get_metadata(SERVICE_NAME2).await.err().unwrap();

        assert_eq!(err.to_string(), format!("failed to parse metadata: {ERROR_MESSAGE}"));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_no_match_returns_none() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl.get_metadata(SERVICE_NAME2).await.unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_success() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let outcome = service_publisher_manager_impl.get_message_provider(SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::Success(_))),
            "Unexpected outcome: {outcome:?}"
        );

        assert_eq!(service_publisher.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err =
            service_publisher_manager_impl.get_message_provider(SERVICE_NAME2).await.err().unwrap();

        assert_eq!(err.to_string(), format!("failed to parse metadata: {ERROR_MESSAGE}"));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_no_match_success() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let outcome = service_publisher_manager_impl.get_message_provider(SERVICE_NAME2).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::NotRegistered)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_get_not_supported_success() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_get_not_supported());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let outcome = service_publisher_manager_impl.get_message_provider(SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::GetNotSupported)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_multiple_accessors_get_not_supported_success() {
        let service_publishers = vec![
            Arc::new(MockServicePublisherAccessor::new().with_next_get_not_supported())
                as Arc<dyn ServicePublisherAccessor>,
            Arc::new(MockServicePublisherAccessor::new().with_next_not_available())
                as Arc<dyn ServicePublisherAccessor>,
        ];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let outcome = service_publisher_manager_impl.get_message_provider(SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::GetNotSupported)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_success() {
        let configuration_msg =
            Some(Any::pack(&Int32Value { value: 42, ..Default::default() }).unwrap());
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        service_publisher_manager_impl
            .create_subscription(
                SERVICE_NAME,
                configuration_msg.as_ref(),
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(service_publisher.take_last_configuration(), configuration_msg);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_propagates_error() {
        let service_publisher = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl
            .create_subscription(
                SERVICE_NAME2,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .err()
            .unwrap();

        assert_eq!(err.to_string(), format!("failed to parse metadata: {ERROR_MESSAGE}"));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_no_match_returns_not_available() {
        let service_publisher =
            Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let res = service_publisher_manager_impl
            .create_subscription(
                SERVICE_NAME2,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert!(matches!(res, CreateSubscriptionOutcome::NotRegistered { .. }));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_returns_valid_subscription() {
        let service_publisher = Arc::new(MockServicePublisherAccessor::new());

        let service_publishers =
            vec![Arc::clone(&service_publisher) as Arc<dyn ServicePublisherAccessor>];

        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let subscription = service_publisher_manager_impl
            .create_subscription(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .unwrap();
        let res = subscription.unsubscribe().await;

        assert!(res.is_ok(), "Unexpected result: {:?}", res);
        assert_eq!(service_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(service_publisher.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_availability_callback_success() {
        let service_publisher1 = Arc::new(MockServicePublisherAccessor::new());
        let service_publisher2 =
            Arc::new(MockServicePublisherAccessor::new().with_availability_callback());
        let service_publishers = vec![
            Arc::clone(&service_publisher1) as Arc<dyn ServicePublisherAccessor>,
            Arc::clone(&service_publisher2) as Arc<dyn ServicePublisherAccessor>,
        ];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let registration = service_publisher_manager_impl
            .register_availability_callback(
                SERVICE_NAME,
                &(Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>),
            )
            .await
            .unwrap();
        registration.unwrap().unregister().await;

        assert_eq!(service_publisher1.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_publisher1.get_num_unregister_availability_callback_calls(), 0);
        assert_eq!(service_publisher2.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_publisher2.get_num_unregister_availability_callback_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_availability_callback_no_callbacks_success() {
        let service_publisher1 = Arc::new(MockServicePublisherAccessor::new());
        let service_publisher2 = Arc::new(MockServicePublisherAccessor::new());
        let service_publishers = vec![
            Arc::clone(&service_publisher1) as Arc<dyn ServicePublisherAccessor>,
            Arc::clone(&service_publisher2) as Arc<dyn ServicePublisherAccessor>,
        ];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let registration = service_publisher_manager_impl
            .register_availability_callback(
                SERVICE_NAME,
                &(Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>),
            )
            .await
            .unwrap();

        assert!(registration.is_none());
        assert_eq!(service_publisher1.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_publisher1.get_num_unregister_availability_callback_calls(), 0);
        assert_eq!(service_publisher2.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_publisher1.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_availability_callback_propagates_error_and_unsubscribes() {
        let service_publisher1 =
            Arc::new(MockServicePublisherAccessor::new().with_availability_callback());
        let service_publisher2 = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_publishers = vec![
            Arc::clone(&service_publisher1) as Arc<dyn ServicePublisherAccessor>,
            Arc::clone(&service_publisher2) as Arc<dyn ServicePublisherAccessor>,
        ];
        let service_publisher_manager_impl = ServicePublisherManager::new(service_publishers);

        let err = service_publisher_manager_impl
            .register_availability_callback(
                SERVICE_NAME2,
                &(Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>),
            )
            .await
            .err()
            .unwrap();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
        assert_eq!(service_publisher1.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_publisher1.get_num_unregister_availability_callback_calls(), 1);
        assert_eq!(service_publisher2.get_num_register_availability_callback_calls(), 1);
    }
}
