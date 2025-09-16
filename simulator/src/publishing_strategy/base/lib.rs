// Copyright 2025 Google LLC

//! Defines the interface for different publishing strategies.

use protobuf::descriptor::FileDescriptorProto;
use std::fmt::Debug;

/// The type of publishing strategy of the publisher.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PublishingStrategyType {
    /// Publishing strategy via Telemetry's Configurable Publisher Registry
    ConfigurablePublisherRegistry,
    /// Publishing strategy via SDV Comms Data Tunnel
    SdvCommsDataTunnel,
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
