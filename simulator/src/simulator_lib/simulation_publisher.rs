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

use log::{debug, error};
use sdv_telemetry_simulator_publishing_strategy::PublishingStrategy;

use crate::publisher_config::PublisherConfig;

pub(crate) struct SimulationPublisher {
    pub(crate) config: PublisherConfig,
    // The strategy-specific implementation for registering the publisher and sending messages.
    publishing_strategy: Box<dyn PublishingStrategy>,
}

impl SimulationPublisher {
    pub fn new(config: PublisherConfig, publishing_strategy: Box<dyn PublishingStrategy>) -> Self {
        Self { publishing_strategy, config }
    }

    pub async fn register(
        mut self,
    ) -> anyhow::Result<scopeguard::ScopeGuard<Self, impl FnOnce(Self)>> {
        let service_name = self.config.service_name.to_owned();

        debug!("Registering publisher {service_name}.");
        self.publishing_strategy
            .register(&service_name, &self.config.message_name, &self.config.file_descriptor_protos)
            .await?;
        Ok(scopeguard::guard(self, move |mut publisher| {
            tokio::spawn(async move {
                debug!("Unregistering publisher {service_name}.");
                if let Err(err) = publisher.publishing_strategy.unregister().await {
                    error!("Failed to unregister publisher {service_name}: {err:?}");
                }
            });
        }))
    }

    pub async fn publish_data(&mut self) {
        loop {
            if self.publish_data_once().await.is_err() {
                break;
            }

            if !self.config.loop_data {
                break;
            }
        }
    }

    async fn publish_data_once(&mut self) -> Result<(), ()> {
        for data_entry in &self.config.parsed_data {
            tokio::time::sleep(data_entry.delay).await;

            let _ = self.publishing_strategy.publish(&data_entry.message_bytes);
        }

        Ok(())
    }
}
