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

//! Defines the strategy of publishers using the Configurable Publisher Registry.

mod publisher;
mod subscription;

use anyhow::{bail, Context};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    PublisherInfo::PublisherInfo,
    IConfigurablePublisher::BnConfigurablePublisher,
    IConfigurablePublisherRegistry::{IConfigurablePublisherRegistry, BpConfigurablePublisherRegistry}
};
use binder::BinderFeatures;
use std::sync::Arc;
use protobuf::descriptor::{FileDescriptorProto, FileDescriptorSet};
use protobuf::Message;
use sdv_telemetry_simulator_publishing_strategy::{
    PublishingStrategy, PublishingStrategyFactory, PublishingStrategyType,
};
use std::cell::LazyCell;

use crate::publisher::{Publisher, PublisherWrapper};

/// Implements `PublishingStrategyFactory` for publishers using the Configurable Publisher Registry.
pub struct PublishingStrategyFactoryImpl {
    registry:
        LazyCell<Result<binder::Strong<dyn IConfigurablePublisherRegistry>, binder::StatusCode>>,
}

impl Default for PublishingStrategyFactoryImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl PublishingStrategyFactory for PublishingStrategyFactoryImpl {
    async fn create(
        &mut self,
        publishing_strategy_type: &PublishingStrategyType,
    ) -> Option<anyhow::Result<Box<dyn PublishingStrategy>>> {
        let PublishingStrategyType::ConfigurablePublisherRegistry = publishing_strategy_type else {
            return None;
        };

        Some(self.registry.clone().map_err(Into::into).map(|registry| {
            Box::new(PublishingStrategyImpl::new(registry)) as Box<dyn PublishingStrategy>
        }))
    }
}

impl PublishingStrategyFactoryImpl {
    /// Creates a new `PublishingStrategyFactoryImpl` instance.
    pub fn new() -> Self {
        Self {
            registry: LazyCell::new(|| {
                let registry_descriptor = format!(
                    "{}/default",
                    <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
                );
                binder::get_interface::<dyn IConfigurablePublisherRegistry>(&registry_descriptor)
            }),
        }
    }
}

/// Implements `PublishingStrategy` for publishers using the Configurable Publisher Registry.
#[derive(Debug)]
pub struct PublishingStrategyImpl {
    registry: binder::Strong<dyn IConfigurablePublisherRegistry>,
    publisher_and_service_name: Option<(Arc<Publisher>, String)>,
}

#[async_trait::async_trait]
impl PublishingStrategy for PublishingStrategyImpl {
    async fn register(
        &mut self,
        service_name: &str,
        message_name: &str,
        file_descriptor_protos: &[FileDescriptorProto],
    ) -> anyhow::Result<()> {
        let publisher = Arc::new(Publisher::new());
        let publisher_wrapper = PublisherWrapper { publisher: Arc::clone(&publisher) };
        let binder_publisher =
            BnConfigurablePublisher::new_binder(publisher_wrapper, BinderFeatures::default());

        let file_descriptor_set =
            FileDescriptorSet { file: file_descriptor_protos.to_owned(), ..Default::default() };
        let publisher_info = PublisherInfo {
            serviceName: service_name.to_owned(),
            fileDescriptorSet: Some(
                file_descriptor_set.write_to_bytes().with_context(|| {
                    format!(
                        "Failed to write file descriptor proto for publisher \"{service_name}\" to bytes",
                    )
                })?
            ),
            messageTypeName: format!(".{message_name}"),
            supportsGet: true,
            // Deprecated
            fileDescriptorProto: None,
        };

        self.registry.registerPublisher(&publisher_info, &binder_publisher)?;
        self.publisher_and_service_name = Some((publisher, service_name.to_owned()));

        Ok(())
    }

    async fn unregister(&mut self) -> anyhow::Result<()> {
        let Some((_, service_name)) = self.publisher_and_service_name.take() else {
            bail!("Publisher not registered");
        };

        self.registry.unregisterPublisher(&service_name).with_context(|| {
            format!("Failed to unregister publisher {service_name} from configurable publisher registry")
        })?;

        Ok(())
    }

    fn publish(&mut self, message: &[u8]) -> anyhow::Result<()> {
        let Some((publisher, _)) = &self.publisher_and_service_name else {
            bail!("Publisher is not yet registered.");
        };
        publisher.publish(message);

        Ok(())
    }
}

impl PublishingStrategyImpl {
    /// Creates a new `PublishingStrategyImpl` instance.
    pub fn new(registry: binder::Strong<dyn IConfigurablePublisherRegistry>) -> Self {
        Self { registry, publisher_and_service_name: None }
    }
}
