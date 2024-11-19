// Copyright 2023 Google LLC

//! Telemetry subscription logic for SDV RPC.

use anyhow::Context;
use async_trait::async_trait;
use futures::future::{self, BoxFuture};
use futures::StreamExt;
use log::error;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::{any::Any, empty::Empty};
use protobuf::MessageDyn;
use sdv_comms::id::UnitIdentifier;
use sdv_comms::rpc;
use sdv_comms::ContextRef;
use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, GetMessageProviderOutcome, ServicePublisherSubscription,
};
use tokio::select;
use tokio_util::sync::{CancellationToken, DropGuard};

pub(super) struct SdvRpcClient {
    ctx: future::Shared<BoxFuture<'static, ContextRef>>,
}

struct SdvRpcSubscription {
    _subscription_drop_guard: DropGuard,
}

impl SdvRpcClient {
    pub(super) fn new(ctx: future::Shared<BoxFuture<'static, ContextRef>>) -> Self {
        Self { ctx }
    }

    pub(super) async fn get_message_provider(
        &self,
        _unit_identifier: UnitIdentifier,
        _message_descriptor: MessageDescriptor,
    ) -> anyhow::Result<GetMessageProviderOutcome> {
        Ok(GetMessageProviderOutcome::GetNotSupported)
    }

    pub(super) async fn create_subscription(
        &self,
        unit_identifier: UnitIdentifier,
        configuration: Option<&Any>,
        message_descriptor: MessageDescriptor,
        mut callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        let client = rpc::Client::create(
            self.ctx.clone().await,
            unit_identifier.clone(),
            &rpc::ClientOptions::default(),
        )
        .await
        .with_context(|| {
            format!(
                "Failed to connect to rpc server with unit identifier {}.",
                Self::to_debug_string(&unit_identifier)
            )
        })?;

        let configuration: Box<dyn MessageDyn> = if let Some(conf_msg) = configuration {
            Box::new(conf_msg.to_owned())
        } else {
            Box::new(Empty::default())
        };
        let configuration_bytes = configuration
            .write_to_bytes_dyn()
            .expect("Configuration serialization should be successful");
        let mut response_stream = client
            .server_streaming_call("CreateSubscription", &configuration_bytes)
            .await
            .with_context(|| "Failed to make server streaming call")?;

        let cancellation_token = CancellationToken::new();
        let subscription_drop_guard = cancellation_token.clone().drop_guard();

        tokio::spawn(async move {
            while let Some(result) = select! {
                biased;
                _ = cancellation_token.cancelled() => None,
                result = response_stream.next() => result,
            } {
                let Ok(buffer) = result.inspect_err(|err| error!("Server stream of service unit '{}' returned an intermittent error: {err:?}", Self::to_debug_string(&unit_identifier))) else {
                    continue;
                };

                let res = message_descriptor.parse_from_bytes(&buffer);
                match res {
                    Ok(msg) => {
                        (callback)(msg);
                    }
                    Err(err) => error!(
                        "Failed to convert bytes to protobuf for service unit '{}': {err:?}",
                        Self::to_debug_string(&unit_identifier)
                    ),
                };
            }
        });

        let subscription =
            Box::new(SdvRpcSubscription { _subscription_drop_guard: subscription_drop_guard });

        Ok(CreateSubscriptionOutcome::Subscribed(subscription))
    }

    fn to_debug_string(unit_identifier: &UnitIdentifier) -> String {
        format!("{}|{}", unit_identifier.get_sid(), unit_identifier.get_unit_name())
    }
}

#[async_trait]
impl ServicePublisherSubscription for SdvRpcSubscription {
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        // This drops `self` and therefore cancels the subscription via the `DropGuard`.
        Ok(())
    }
}
