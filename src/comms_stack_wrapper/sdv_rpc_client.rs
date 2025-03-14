// Copyright 2023 Google LLC

//! Telemetry subscription logic for SDV RPC.

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use futures::future::{self, BoxFuture};
use futures::StreamExt;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::{any::Any, empty::Empty};
use protobuf::{Message, MessageDyn};
use sdv_comms::id::UnitIdentifier;
use sdv_comms::rpc;
use sdv_comms::sd::ServiceUnitDefinition;
use sdv_comms::ContextRef;
use sdv_config_provider::vsidl_provider_api::VsidlConfigProviderV1;
use sdv_telemetry_service_publisher_accessor::{
    CreateSubscriptionOutcome, GetMessageProviderOutcome, ServicePublisherAccessorError,
    ServicePublisherMessageProvider, ServicePublisherSubscription,
};
use tokio::select;
use tokio_util::sync::{CancellationToken, DropGuard};

const CREATE_SUBSCRIPTION_METHOD: &str = "CreateSubscription";
const GET_LATEST_MESSAGE_METHOD: &str = "GetLatestMessage";

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
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
        let client = rpc::Client::create(
            self.ctx.clone().await,
            unit_identifier.clone(),
            &rpc::ClientOptions::default(),
        )
        .await
        .map_err(ServicePublisherAccessorError::RpcFailedToConnect)?;

        struct ServicePublisherMessageProviderImpl {
            client: rpc::Client,
            message_descriptor: MessageDescriptor,
        }

        #[async_trait]
        impl ServicePublisherMessageProvider for ServicePublisherMessageProviderImpl {
            async fn get_latest_message(
                &self,
                configuration: Option<&Any>,
            ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
                let configuration_bytes = SdvRpcClient::serialize_configuration(configuration);
                let response_bytes = self
                    .client
                    .unary_call(GET_LATEST_MESSAGE_METHOD, &configuration_bytes)
                    .await
                    .map_err(|err| {
                        ServicePublisherAccessorError::MessageRetrievalFailed(err.into())
                    })?;
                if response_bytes.is_empty() {
                    return Ok(None);
                }

                let response =
                    self.message_descriptor.parse_from_bytes(&response_bytes).map_err(|err| {
                        ServicePublisherAccessorError::MessageParsingFailed(err.into())
                    })?;
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
        mut new_message_callback: Box<
            dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync,
        >,
        cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
    ) -> Result<CreateSubscriptionOutcome, ServicePublisherAccessorError> {
        let client = rpc::Client::create(
            self.ctx.clone().await,
            unit_identifier.clone(),
            &rpc::ClientOptions::default(),
        )
        .await
        .map_err(ServicePublisherAccessorError::RpcFailedToConnect)?;

        let configuration_bytes = Self::serialize_configuration(configuration);
        let mut response_stream = client
            .server_streaming_call(CREATE_SUBSCRIPTION_METHOD, &configuration_bytes)
            .await
            .map_err(|err| ServicePublisherAccessorError::FailedToSubscribe(err.into()))?;

        let cancellation_token = CancellationToken::new();
        let subscription_drop_guard = cancellation_token.clone().drop_guard();

        tokio::spawn(async move {
            loop {
                match select! {
                    biased;
                    _ = cancellation_token.cancelled() => None,
                    result = response_stream.next() => Some(result),
                } {
                    None => {
                        // Subscription is cancelled on the Telemetry Service side.
                        return;
                    }
                    Some(None) => {
                        // Subscription is cancelled on the publisher side.
                        (cancellation_callback)();
                        return;
                    }
                    Some(Some(result)) => {
                        let buffer = match result
                            .with_context(|| "RPC server stream produced an intermittent error")
                        {
                            Ok(buffer) => buffer,
                            Err(err) => {
                                (new_message_callback)(Err(
                                    ServicePublisherAccessorError::MessageRetrievalFailed(err),
                                ));
                                continue;
                            }
                        };

                        let message = message_descriptor
                            .parse_from_bytes(&buffer)
                            .with_context(|| "Failed to parse the protobuf message")
                            .map_err(ServicePublisherAccessorError::MessageParsingFailed);
                        (new_message_callback)(message);
                    }
                }
            }
        });

        let subscription =
            Box::new(SdvRpcSubscription { _subscription_drop_guard: subscription_drop_guard });

        Ok(CreateSubscriptionOutcome::Subscribed(subscription))
    }

    pub(super) async fn get_message_descriptor(
        vsidl_provider: &dyn VsidlConfigProviderV1,
        service_unit_definition: &ServiceUnitDefinition,
    ) -> anyhow::Result<MessageDescriptor> {
        let create_subscription_descriptor = Self::get_response_descriptor_for_method(
            vsidl_provider,
            service_unit_definition,
            CREATE_SUBSCRIPTION_METHOD,
        )
        .await?;
        let get_latest_message_descriptor = Self::get_response_descriptor_for_method(
            vsidl_provider,
            service_unit_definition,
            GET_LATEST_MESSAGE_METHOD,
        )
        .await?;

        match (create_subscription_descriptor, get_latest_message_descriptor) {
            (None, None) => Err(anyhow!(
                "VSIDL provider was unable to provide RPC method descriptor for {}|{}",
                service_unit_definition.get_service_bundle_identity().get_fqin(),
                service_unit_definition.get_unit_type()
            )),
            (None, Some(descriptor)) | (Some(descriptor), None) => Ok(descriptor),

            (
                Some(a @ /* type for clarity: */ MessageDescriptor { .. }),
                Some(b @ /* type for clarity: */ MessageDescriptor { .. }),
            ) => {
                // We want to ensure that both `MessageDescriptor`s describe the same message. We
                // MUST NOT compare the `MessageDescriptor`s directly, because they are only equal
                // if the message is defined in the exact same file and position within that file.
                //
                // Instead, we need to compare the `DescriptorProto`s (returned by
                // `MessageDescriptor::proto`), which describe the actual message formats,
                // irrespective of the file they are in, as well as the "full name" (= package name
                // + message name).
                if a.proto() == b.proto() && a.full_name() == b.full_name() {
                    Ok(a)
                } else {
                    Err(anyhow!(
                        "VSIDL provider returned conflicting RPC method descriptors for {}|{}",
                        service_unit_definition.get_service_bundle_identity().get_fqin(),
                        service_unit_definition.get_unit_type()
                    ))
                }
            }
        }
    }

    async fn get_response_descriptor_for_method(
        vsidl_provider: &dyn VsidlConfigProviderV1,
        service_unit_definition: &ServiceUnitDefinition,
        method_name: &str,
    ) -> anyhow::Result<Option<MessageDescriptor>> {
        let Some(rpc_descriptor) = vsidl_provider
            .get_rpc_method_descriptor(
                service_unit_definition.get_service_bundle_identity().get_fqin(),
                &service_unit_definition.get_unit_type(),
                method_name,
            )
            .await
        else {
            return Ok(None);
        };

        let Some(response_descriptor) = rpc_descriptor.response_message else {
            bail!(
                "VSIDL provider descriptor for {}|{} has no response message",
                service_unit_definition.get_service_bundle_identity().get_fqin(),
                service_unit_definition.get_unit_type()
            );
        };
        Ok(Some(response_descriptor))
    }

    fn serialize_configuration(configuration: Option<&Any>) -> Vec<u8> {
        if let Some(conf_msg) = configuration {
            conf_msg.write_to_bytes()
        } else {
            Empty::default_instance().write_to_bytes()
        }
        .expect("Configuration serialization should be successful")
    }
}

#[async_trait]
impl ServicePublisherSubscription for SdvRpcSubscription {
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        // This drops `self` and therefore cancels the subscription via the `DropGuard`.
        Ok(())
    }
}
