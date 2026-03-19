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

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisher::BnConfigurablePublisher;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::IConfigurablePublisherRegistry;
#[cfg(test)]
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::MockIConfigurablePublisherRegistry;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::BpConfigurablePublisherRegistry;
use binder::BinderFeatures;
use binder::Strong;

use crate::publisher::PublisherBinder;
use crate::Publisher;
use crate::Registration;

/// Proxy of the Configurable Publisher Registry Binder service.
#[non_exhaustive]
pub struct Registry {
    registry_binder: Strong<dyn IConfigurablePublisherRegistry>,
}

impl Registry {
    /// Connects to the Configurable Publisher Registry Binder service and creates a new instance of
    /// `Self`.
    pub fn new() -> binder::Result<Self> {
        let service_name = format!(
            "{}/default",
            <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
        );
        let registry_binder =
            binder::get_interface::<dyn IConfigurablePublisherRegistry>(&service_name)?;

        Ok(Registry { registry_binder })
    }

    #[cfg(test)]
    pub(crate) fn new_mocked(registry_binder: MockIConfigurablePublisherRegistry) -> Self {
        Registry { registry_binder: Strong::new(Box::new(registry_binder)) }
    }

    /// Registers the publisher with the registry.
    pub fn register(&mut self, mut publisher: impl Publisher) -> binder::Result<Registration> {
        let metadata = publisher.get_metadata();
        let publisher = BnConfigurablePublisher::new_binder(
            PublisherBinder::new(publisher),
            BinderFeatures::default(),
        );

        let name = metadata.name.clone();
        let publisher_info = metadata.into_aidl();

        self.registry_binder.registerPublisher(&publisher_info, &publisher)?;
        Ok(Registration::new(name))
    }

    /// Unregisters the publisher from the registry.
    pub fn unregister(&mut self, registration: Registration) -> binder::Result<()> {
        self.registry_binder.unregisterPublisher(&registration.name.0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::BoxStream;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::MockIConfigurablePublisherRegistry;
    use googletest::expect_that;
    use googletest::matchers::eq;
    use googletest::matchers::ok;
    use googletest::matchers::pat;
    use mockall::mock;
    use mockall::predicate as m;
    use protobuf::MessageFull;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::well_known_types::wrappers::StringValue;
    use std::sync::LazyLock;

    use crate::codec::ProtobufCodec;
    use crate::PublisherMetadata;
    use crate::PublisherName;

    static METADATA: LazyLock<PublisherMetadata> = LazyLock::new(|| PublisherMetadata {
        name: "test_publisher".parse::<PublisherName>().unwrap(),
        file_descriptor_set: vec![],
        message_full_name: StringValue::descriptor().full_name().to_owned(),
        supports_on_demand_fetching: true,
    });

    mock! {
        PublisherImpl {}

        impl Publisher for PublisherImpl {
            type MessageCodec = ProtobufCodec<StringValue>;
            type ConfigCodec = ProtobufCodec<Int32Value>;

            fn get_metadata(&mut self) -> PublisherMetadata;
            fn subscribe(&mut self, config: Option<Int32Value>) -> BoxStream<'static, StringValue>;
            fn get_on_demand(&mut self, config: Option<Int32Value>) -> Option<StringValue>;
        }
    }

    #[googletest::test]
    fn register_calls_register_on_registry_binder() {
        let mut registry_binder_mock = MockIConfigurablePublisherRegistry::new();
        registry_binder_mock
            .expect_registerPublisher()
            .once()
            .withf(|publisher_info, _publisher_binder| {
                publisher_info.serviceName == METADATA.name.0
            })
            .return_once(move |_, _| Ok(()));
        let mut registry = Registry::new_mocked(registry_binder_mock);

        let mut publisher = MockPublisherImpl::new();
        publisher.expect_get_metadata().return_once(|| METADATA.clone());

        let result = registry.register(publisher);

        expect_that!(
            result,
            ok(
                pat!(&Registration { name: ref eq(&PublisherName("test_publisher".to_owned())), .. })
            )
        );
    }

    #[googletest::test]
    fn unregister_calls_unregister_on_registry_binder() {
        let mut registry_binder_mock = MockIConfigurablePublisherRegistry::new();
        registry_binder_mock
            .expect_unregisterPublisher()
            .once()
            .with(m::eq("test_publisher"))
            .return_once(|_| Ok(()));
        let mut registry = Registry::new_mocked(registry_binder_mock);

        let registration = Registration::new("test_publisher".parse::<PublisherName>().unwrap());

        let result = registry.unregister(registration);

        expect_that!(result, ok(eq(&())));
    }
}
