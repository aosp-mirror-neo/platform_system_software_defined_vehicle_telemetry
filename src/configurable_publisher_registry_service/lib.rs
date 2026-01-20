// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module implements the IConfigurablePublisherRegistry AIDL interface

mod configurable_publisher_registry;
mod subscriber_service;

// External dependencies.
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    ConfigurablePublisherStatusCodes::ConfigurablePublisherStatusCodes as StatusCodes,
    IConfigurablePublisher::IConfigurablePublisher,
    IConfigurablePublisherRegistry::IConfigurablePublisherRegistryAsyncServer,
    PublisherInfo::PublisherInfo,
};
use async_trait::async_trait;
use log::info;
use std::sync::Arc;

// Child module dependencies.
pub use configurable_publisher_registry::ConfigurablePublisherRegistry;
use configurable_publisher_registry::ConfigurablePublisherRegistryRegisterError;

/// Implementation of IConfigurablePublisherRegistryAsyncServer binder interface
/// that uses ConfigurablePublisherRegistryImpl to maintain the registry in memory.
pub struct ConfigurablePublisherRegistryService {
    configurable_publisher_registry: Arc<ConfigurablePublisherRegistry>,
}

/// Convert a Result<T, E> to BinderResult<T> to allow it to be returned from a binder RPC,
/// preserving the content as far as possible.
/// Also log the error if there is one.
fn to_binder_result<T>(
    result: Result<T, ConfigurablePublisherRegistryRegisterError>,
) -> binder::Result<T> {
    result.map_err(|e| {
        let message = format!("{e:?}");
        info!("Returning binder error: {}", &message);

        match e {
            ConfigurablePublisherRegistryRegisterError::BinderError { .. } => {
                binder::Status::new_service_specific_error_str(
                    StatusCodes::REGISTER_PUBLISHER_FAILED_BINDER_ERROR.0,
                    Some(message),
                )
            }
            ConfigurablePublisherRegistryRegisterError::FileDescriptorParseError { .. } => {
                binder::Status::new_service_specific_error_str(
                    StatusCodes::REGISTER_PUBLISHER_FAILED_FILE_DESCRIPTOR_PROTO_PARSE_ERROR.0,
                    Some(message),
                )
            }
            ConfigurablePublisherRegistryRegisterError::InvalidMessageTypeName { .. } => {
                binder::Status::new_service_specific_error_str(
                    StatusCodes::REGISTER_PUBLISHER_FAILED_INVALID_MESSAGE_TYPE_NAME.0,
                    Some(message),
                )
            }
            ConfigurablePublisherRegistryRegisterError::ServiceAlreadyRegistered { .. } => {
                binder::Status::new_service_specific_error_str(
                    StatusCodes::REGISTER_PUBLISHER_FAILED_SERVICE_ALREADY_REGISTERED.0,
                    Some(message),
                )
            }
            ConfigurablePublisherRegistryRegisterError::ConflictingFileDescriptorSources => {
                binder::Status::new_service_specific_error_str(
                    StatusCodes::REGISTER_PUBLISHER_FAILED_CONFLICTING_FILE_DESCRIPTOR_SOURCES.0,
                    Some(message),
                )
            }
        }
    })
}

impl binder::Interface for ConfigurablePublisherRegistryService {}

#[async_trait]
impl IConfigurablePublisherRegistryAsyncServer for ConfigurablePublisherRegistryService {
    async fn registerPublisher(
        &self,
        publisher_info: &PublisherInfo,
        publisher: &binder::Strong<dyn IConfigurablePublisher>,
    ) -> binder::Result<()> {
        info!("Registering publisher with telemetry service name {}", publisher_info.serviceName);
        let res = self
            .configurable_publisher_registry
            .register_publisher(publisher_info, publisher)
            .await;

        to_binder_result(res)
    }

    async fn unregisterPublisher(&self, service_name: &str) -> binder::Result<()> {
        info!("Unregistering publisher with telemetry service name {}", service_name);
        self.configurable_publisher_registry.unregister_publisher(service_name).await
    }
}

impl ConfigurablePublisherRegistryService {
    /// Instantiates an instance of ConfigurablePublisherRegistryService
    pub fn new(configurable_publisher_registry: Arc<ConfigurablePublisherRegistry>) -> Self {
        Self { configurable_publisher_registry }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::{IConfigurablePublisherRegistry, BpConfigurablePublisherRegistry};

    use binder::BinderFeatures;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
        IConfigurablePublisher::BnConfigurablePublisher,
        ISubscriber::ISubscriber,
        PublisherInfo::PublisherInfo,
    };
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::{
        IConfigurablePublisherSubscription,
        BnConfigurablePublisherSubscription,
    };
    use protobuf::descriptor::FileDescriptorSet;
    use protobuf::{Message, MessageFull};
    use protobuf::well_known_types::wrappers::Int64Value;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    const SERVICE_NAME: &str = "CONFIGURABLE_PUB_SERVICE";
    const SERVICE_NAME2: &str = "CONFIGURABLE_PUB_SERVICE2";
    const SERVICE_NAME3: &str = "CONFIGURABLE_PUB_SERVICE3";
    const SERVICE_NAME4: &str = "CONFIGURABLE_PUB_SERVICE4";

    struct MockConfigurablePublisher {
        /// Specifies whether the mock supports gets or returns unsupported error when called.
        supports_get: bool,
    }

    impl binder::Interface for MockConfigurablePublisher {}

    impl IConfigurablePublisher for MockConfigurablePublisher {
        fn createSubscription(
            &self,
            _subscriber: &binder::Strong<dyn ISubscriber>,
            _configuration: Option<&[u8]>,
        ) -> binder::Result<binder::Strong<dyn IConfigurablePublisherSubscription>> {
            let binder_subscription = BnConfigurablePublisherSubscription::new_binder(
                MockConfigurablePublisherSubscription,
                BinderFeatures::default(),
            );
            Ok(binder_subscription)
        }

        fn getLatestMessage(
            &self,
            _configuration: Option<&[u8]>,
        ) -> binder::Result<Option<Vec<u8>>> {
            if self.supports_get {
                Ok(Some(Vec::<u8>::new()))
            } else {
                Err(binder::Status::new_exception(
                    binder::ExceptionCode::UNSUPPORTED_OPERATION,
                    None,
                ))
            }
        }
    }

    impl MockConfigurablePublisher {
        pub fn new(supports_get: bool) -> Self {
            Self { supports_get }
        }
    }

    struct MockConfigurablePublisherSubscription;

    impl binder::Interface for MockConfigurablePublisherSubscription {}

    impl IConfigurablePublisherSubscription for MockConfigurablePublisherSubscription {
        fn unsubscribe(&self) -> binder::Result<()> {
            Ok(())
        }
    }

    fn connect_to_service(
    ) -> Result<binder::Strong<dyn IConfigurablePublisherRegistry>, binder::StatusCode> {
        let configurable_publisher_registry_descriptor = format!(
            "{}/default",
            <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
        );
        binder::get_interface(&configurable_publisher_registry_descriptor)
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_publisher_with_deprecated_file_descriptor_proto_success() {
        let service =
            connect_to_service().expect("Failed to connect to IConfigurablePublisherRegistry");
        let configurable_publisher = MockConfigurablePublisher::new(false);

        let message_descriptor = Int64Value::descriptor();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME.to_string(),
            fileDescriptorProto: Some(
                message_descriptor.file_descriptor_proto().write_to_bytes().unwrap(),
            ),
            messageTypeName: format!(".{}", message_descriptor.full_name()),
            supportsGet: configurable_publisher.supports_get,
            fileDescriptorSet: None,
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        let res = service.registerPublisher(&publisher_info, &binder_publisher);

        assert!(res.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_publisher_with_file_descriptor_set_success() {
        let service =
            connect_to_service().expect("Failed to connect to IConfigurablePublisherRegistry");
        let configurable_publisher = MockConfigurablePublisher::new(false);

        let message_descriptor = Int64Value::descriptor();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME4.to_string(),
            fileDescriptorProto: None,
            messageTypeName: format!(".{}", message_descriptor.full_name()),
            supportsGet: configurable_publisher.supports_get,
            fileDescriptorSet: Some(
                FileDescriptorSet {
                    file: vec![message_descriptor.file_descriptor_proto().to_owned()],
                    ..Default::default()
                }
                .write_to_bytes()
                .unwrap(),
            ),
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        let result = service.registerPublisher(&publisher_info, &binder_publisher);
        assert!(result.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn invalid_file_descriptor_proto_register_publisher_failed() {
        let service =
            connect_to_service().expect("Failed to connect to IConfigurablePublisherRegistry");
        let configurable_publisher = MockConfigurablePublisher::new(false);

        let message_descriptor = Int64Value::descriptor();

        let file_descriptor_proto = message_descriptor.file_descriptor_proto();

        let mut invalid_file_descriptor_proto = file_descriptor_proto.write_to_bytes().unwrap();
        let slice = invalid_file_descriptor_proto.as_mut_slice();
        let mut rng = thread_rng();
        slice.shuffle(&mut rng);

        let descriptor = Int64Value::descriptor();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME.to_string(),
            fileDescriptorProto: Some(invalid_file_descriptor_proto),
            messageTypeName: format!(".{}", descriptor.full_name()),
            supportsGet: configurable_publisher.supports_get,
            fileDescriptorSet: None,
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        let err_binder = service.registerPublisher(&publisher_info, &binder_publisher).unwrap_err();

        assert_eq!(
            err_binder.service_specific_error(),
            StatusCodes::REGISTER_PUBLISHER_FAILED_FILE_DESCRIPTOR_PROTO_PARSE_ERROR.0,
            "Unexpected error: {err_binder:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn invalid_file_descriptor_set_register_publisher_failed() {
        let service =
            connect_to_service().expect("Failed to connect to IConfigurablePublisherRegistry");
        let configurable_publisher = MockConfigurablePublisher::new(false);

        let message_descriptor = Int64Value::descriptor();

        let file_descriptor_set = FileDescriptorSet {
            file: vec![message_descriptor.file_descriptor_proto().to_owned()],
            ..Default::default()
        };

        let mut invalid_file_descriptor_set = file_descriptor_set.write_to_bytes().unwrap();
        let slice = invalid_file_descriptor_set.as_mut_slice();
        let mut rng = thread_rng();
        slice.shuffle(&mut rng);

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME.to_string(),
            messageTypeName: format!(".{}", message_descriptor.full_name()),
            supportsGet: configurable_publisher.supports_get,

            fileDescriptorSet: Some(invalid_file_descriptor_set),
            fileDescriptorProto: None,
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        let err_binder = service.registerPublisher(&publisher_info, &binder_publisher).unwrap_err();

        assert_eq!(
            err_binder.service_specific_error(),
            StatusCodes::REGISTER_PUBLISHER_FAILED_FILE_DESCRIPTOR_PROTO_PARSE_ERROR.0,
            "Unexpected error: {err_binder:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn invalid_message_type_name_register_publisher_failed() {
        let service =
            connect_to_service().expect("Failed to connect to IConfigurablePublisherRegistry");
        let configurable_publisher = MockConfigurablePublisher::new(false);

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME.to_string(),
            fileDescriptorProto: Some(
                Int64Value::descriptor().file_descriptor_proto().write_to_bytes().unwrap(),
            ),
            messageTypeName: ".InvalidMessageTypeName".to_string(),
            supportsGet: configurable_publisher.supports_get,
            fileDescriptorSet: None,
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        let err_binder = service.registerPublisher(&publisher_info, &binder_publisher).unwrap_err();

        assert_eq!(
            err_binder.service_specific_error(),
            StatusCodes::REGISTER_PUBLISHER_FAILED_INVALID_MESSAGE_TYPE_NAME.0,
            "Unexpected error: {err_binder:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn service_already_registered_register_publisher_failed() {
        let service =
            connect_to_service().expect("Failed to connect to IConfigurablePublisherRegistry");
        let configurable_publisher = MockConfigurablePublisher::new(false);

        let message_descriptor = Int64Value::descriptor();
        let file_descriptor_proto = message_descriptor.file_descriptor_proto();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME2.to_string(),
            fileDescriptorProto: Some(file_descriptor_proto.write_to_bytes().unwrap()),
            messageTypeName: format!(".{}", message_descriptor.full_name()),
            supportsGet: configurable_publisher.supports_get,
            fileDescriptorSet: None,
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        let res = service.registerPublisher(&publisher_info, &binder_publisher);
        let err_binder = service.registerPublisher(&publisher_info, &binder_publisher).unwrap_err();

        assert!(res.is_ok());
        assert_eq!(
            err_binder.service_specific_error(),
            StatusCodes::REGISTER_PUBLISHER_FAILED_SERVICE_ALREADY_REGISTERED.0,
            "Unexpected error: {err_binder:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unregister_publisher_success() {
        let service =
            connect_to_service().expect("Failed to connect to IConfigurablePublisherRegistry");
        let configurable_publisher = MockConfigurablePublisher::new(false);

        let message_descriptor = Int64Value::descriptor();
        let file_descriptor_proto = message_descriptor.file_descriptor_proto();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME3.to_string(),
            fileDescriptorProto: Some(file_descriptor_proto.write_to_bytes().unwrap()),
            messageTypeName: format!(".{}", message_descriptor.full_name()),
            supportsGet: configurable_publisher.supports_get,
            fileDescriptorSet: None,
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        let res1 = service.registerPublisher(&publisher_info, &binder_publisher);
        let res2 = service.unregisterPublisher(SERVICE_NAME3);

        assert!(res1.is_ok());
        assert!(res2.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_publisher_conflicting_file_descriptor_sources_fails() {
        let registry = Arc::new(ConfigurablePublisherRegistry::new());
        let service = ConfigurablePublisherRegistryService::new(Arc::clone(&registry));

        let publisher_info = PublisherInfo {
            serviceName: "test_service".to_string(),
            messageTypeName: "test_message".to_string(),
            supportsGet: false,

            // Conflicting sources
            fileDescriptorSet: Some(vec![1, 2, 3]),
            fileDescriptorProto: Some(vec![4, 5, 6]),
        };
        let publisher = BnConfigurablePublisher::new_binder(
            MockConfigurablePublisher::new(false),
            BinderFeatures::default(),
        );

        let result = service.registerPublisher(&publisher_info, &publisher).await;

        let err = result.unwrap_err();
        assert_eq!(
            err.service_specific_error(),
            StatusCodes::REGISTER_PUBLISHER_FAILED_CONFLICTING_FILE_DESCRIPTOR_SOURCES.0
        );
    }
}
