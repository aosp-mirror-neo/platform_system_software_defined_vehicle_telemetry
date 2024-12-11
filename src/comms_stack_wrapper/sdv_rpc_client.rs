// Copyright 2023 Google LLC

//! Telemetry subscription logic for SDV RPC.

use anyhow::Context;
use async_trait::async_trait;
use futures::future::{self, BoxFuture};
use futures::StreamExt;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::{any::Any, empty::Empty};
use protobuf::{Message, MessageDyn};
use sdv_comms::id::UnitIdentifier;
use sdv_comms::rpc;
use sdv_comms::ContextRef;
use sdv_telemetry::metrics_config_runtime_error::ServiceRuntimeError;
use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, GetMessageProviderOutcome, ServicePublisherMessageProvider,
    ServicePublisherSubscription,
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
        unit_identifier: UnitIdentifier,
        message_descriptor: MessageDescriptor,
    ) -> anyhow::Result<GetMessageProviderOutcome> {
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

        struct ServicePublisherMessageProviderImpl {
            client: rpc::Client,
            message_descriptor: MessageDescriptor,
        }

        #[async_trait]
        impl ServicePublisherMessageProvider for ServicePublisherMessageProviderImpl {
            async fn get_latest_message(
                &self,
                configuration: Option<&Any>,
            ) -> anyhow::Result<Option<Box<dyn MessageDyn>>> {
                let configuration_bytes = SdvRpcClient::serialize_configuration(configuration);
                let response_bytes = self
                    .client
                    .unary_call("GetLatestMessage", &configuration_bytes)
                    .await
                    .with_context(|| "Failed to make RPC call")?;
                if response_bytes.is_empty() {
                    return Ok(None);
                }

                let response = self
                    .message_descriptor
                    .parse_from_bytes(&response_bytes)
                    .with_context(|| "Failed to convert bytes to protobuf message")?;
                Ok(Some(response))
            }
        }

        Ok(GetMessageProviderOutcome::Success(Box::new(ServicePublisherMessageProviderImpl {
            client,
            message_descriptor,
        })))
    }

    pub(super) async fn create_subscription(
        &self,
        unit_identifier: UnitIdentifier,
        configuration: Option<&Any>,
        message_descriptor: MessageDescriptor,
        mut callback: Box<
            dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync,
        >,
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

        let configuration_bytes = Self::serialize_configuration(configuration);
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
                let buffer = match result
                    .with_context(|| "RPC server stream produced an intermittent error")
                {
                    Ok(buffer) => buffer,
                    Err(err) => {
                        (callback)(Err(ServiceRuntimeError::MessageRetrievalError(err)));
                        continue;
                    }
                };

                let message = message_descriptor
                    .parse_from_bytes(&buffer)
                    .with_context(|| "Failed to parse the protobuf message")
                    .map_err(ServiceRuntimeError::MessageParsingError);
                (callback)(message);
            }
        });

        let subscription =
            Box::new(SdvRpcSubscription { _subscription_drop_guard: subscription_drop_guard });

        Ok(CreateSubscriptionOutcome::Subscribed(subscription))
    }

    fn serialize_configuration(configuration: Option<&Any>) -> Vec<u8> {
        if let Some(conf_msg) = configuration {
            conf_msg.write_to_bytes()
        } else {
            Empty::default_instance().write_to_bytes()
        }
        .expect("Configuration serialization should be successful")
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
