// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use log::info;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::result::Result as StdResult;
use std::sync::Arc;

use crate::data_tunnel_wrapper::{DataTunnelWrapper, DataTunnelWrapperError};

///Mock Struct for testing purposes
pub struct MockDataTunnelWrapperImpl;

/// Subscriber struct impl that encapsulates the data required for reading messages.
/// #[async_trait]
impl MockDataTunnelWrapperImpl {
    /// Creates a new instance of the struct.
    pub fn new() -> Self {
        // Create struct to interact with Data Tunnel Apis
        MockDataTunnelWrapperImpl
    }
}

#[async_trait]
impl DataTunnelWrapper for MockDataTunnelWrapperImpl {
    /// Subscribe to a Data Tunnel topic sample.
    /// The method returns a struct containing the topic information and the subscription topic handle.
    /// Returns a DT reader implementation that can be used to read messages.
    async fn subscribe(
        self: Arc<Self>,
        topic: &str,
        _callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> StdResult<(), DataTunnelWrapperError> {
        info!("Subscribing to data tunnel topic {}", topic);
        Ok(())
    }

    /// Checks if topic with given name exists in data tunnel
    fn topic_exists(&self, _topic_name: &str) -> bool {
        false
    }

    /// Checks if topic with given name exists in data tunnel
    fn get_message_descriptor(&self, _topic_name: &str) -> Option<MessageDescriptor> {
        None
    }
}
