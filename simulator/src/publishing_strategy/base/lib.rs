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

//! Defines the interface for different publishing strategies.

use protobuf::descriptor::FileDescriptorProto;
use std::fmt::Debug;

/// The type of publishing strategy of the publisher.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PublishingStrategyType {
    /// Publishing strategy via Telemetry's Configurable Publisher Registry
    ConfigurablePublisherRegistry,
    /// Publishing strategy via SDV Comms Data Tunnel
    SdvCommsDataTunnel {
        /// Maximum buffered message count
        message_count: u32,
        /// Maximum serialized message size
        message_size: u32,
    },
    /// Publishing strategy via SDV Comms RPC
    SdvCommsRpc,
}

/// A strategy for publishing messages.
#[async_trait::async_trait]
pub trait PublishingStrategy: Send + Debug + downcast_rs::Downcast {
    /// Register the publisher.
    async fn register(
        &mut self,
        service_name: &str,
        message_name: &str,
        file_descriptor_protos: &[FileDescriptorProto],
    ) -> anyhow::Result<()>;

    /// Unregister the publisher.
    async fn unregister(&mut self) -> anyhow::Result<()>;

    /// Publish the provided serialized protobuf message.
    fn publish(&mut self, message: &[u8]) -> anyhow::Result<()>;
}

downcast_rs::impl_downcast!(PublishingStrategy);

/// Factory for publishing strategies.
#[async_trait::async_trait]
pub trait PublishingStrategyFactory {
    /// Attempts to build a publishing strategy for the provided `PublishingStrategyType`. If this
    /// factory supports the provided `PublishingStrategyType`, returns `Some`, else `None`.
    async fn create(
        &mut self,
        publishing_strategy_type: &PublishingStrategyType,
    ) -> Option<anyhow::Result<Box<dyn PublishingStrategy>>>;
}
