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
use sdv_status_rs::SdvStatusCode;
use sdv_telemetry_service_publisher_accessor::{
    CreateSubscriptionOutcome, GetMessageProviderOutcome, ServicePublisherAccessorError,
    ServicePublisherMessageProvider, ServicePublisherSubscription,
};
use tokio::select;
use tokio_util::sync::{CancellationToken, DropGuard};

const CREATE_SUBSCRIPTION_METHOD: &str = "CreateSubscription";
const GET_LATEST_MESSAGE_METHOD: &str = "GetLatestMessage";
const PROTO_ANY_FULL_NAME: &str = "google.protobuf.Any";

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
        config_descriptor: MessageDescriptor,
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
            config_descriptor: MessageDescriptor,
            message_descriptor: MessageDescriptor,
        }

        #[async_trait]
        impl ServicePublisherMessageProvider for ServicePublisherMessageProviderImpl {
            async fn get_latest_message(
                &self,
                configuration: Option<&Any>,
            ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
                let configuration_bytes =
                    SdvRpcClient::serialize_configuration(configuration, &self.config_descriptor)?;
                let maybe_response_bytes = self
                    .client
                    .unary_call(GET_LATEST_MESSAGE_METHOD, &configuration_bytes)
                    .await
                    .map_or_else(
                        |err| match err.status_code() {
                            SdvStatusCode::NotFound => Ok(None),
                            _ => Err(ServicePublisherAccessorError::MessageRetrievalFailed(
                                err.into(),
                            )),
                        },
                        |response_bytes| Ok(Some(response_bytes)),
                    )?;
                let Some(response_bytes) = maybe_response_bytes else { return Ok(None) };

                let response =
                    self.message_descriptor.parse_from_bytes(&response_bytes).map_err(|err| {
                        ServicePublisherAccessorError::MessageParsingFailed(err.into())
                    })?;
                Ok(Some(response))
            }
        }

        Ok(GetMessageProviderOutcome::Success(Box::new(ServicePublisherMessageProviderImpl {
            client,
            config_descriptor,
            message_descriptor,
        })))
    }

    pub(super) async fn create_subscription(
        &self,
        unit_identifier: UnitIdentifier,
        configuration: Option<&Any>,
        config_descriptor: MessageDescriptor,
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

        let configuration_bytes = Self::serialize_configuration(configuration, &config_descriptor)?;
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
                            .context("RPC server stream produced an intermittent error")
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
                            .context("Failed to parse the protobuf message")
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
    ) -> anyhow::Result<(MessageDescriptor, MessageDescriptor)> {
        let (create_subscription_req, create_subscription_res) = Self::get_descriptor_for_method(
            vsidl_provider,
            service_unit_definition,
            CREATE_SUBSCRIPTION_METHOD,
        )
        .await?
        .map_or_else(|| (None, None), |(req, res)| (Some(req), Some(res)));
        let (get_latest_message_req, get_latest_message_res) = Self::get_descriptor_for_method(
            vsidl_provider,
            service_unit_definition,
            GET_LATEST_MESSAGE_METHOD,
        )
        .await?
        .map_or_else(|| (None, None), |(req, res)| (Some(req), Some(res)));
        let req_descriptor =
            get_either_or_ensure_equal(create_subscription_req, get_latest_message_req)
                .with_context(|| {
                    format!(
                        "Retrieving request descriptor for {}|{}",
                        service_unit_definition.get_service_bundle_identity().get_fqin(),
                        service_unit_definition.get_unit_type()
                    )
                })?;
        let res_descriptor =
            get_either_or_ensure_equal(create_subscription_res, get_latest_message_res)
                .with_context(|| {
                    format!(
                        "Retrieving response descriptor for {}|{}",
                        service_unit_definition.get_service_bundle_identity().get_fqin(),
                        service_unit_definition.get_unit_type()
                    )
                })?;
        Ok((req_descriptor, res_descriptor))
    }

    async fn get_descriptor_for_method(
        vsidl_provider: &dyn VsidlConfigProviderV1,
        service_unit_definition: &ServiceUnitDefinition,
        method_name: &str,
    ) -> anyhow::Result<Option<(MessageDescriptor, MessageDescriptor)>> {
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

        let Some(request_message) = rpc_descriptor.request_message else {
            bail!(
                "VSIDL provider descriptor for {}|{} has no request message",
                service_unit_definition.get_service_bundle_identity().get_fqin(),
                service_unit_definition.get_unit_type()
            );
        };
        let Some(response_message) = rpc_descriptor.response_message else {
            bail!(
                "VSIDL provider descriptor for {}|{} has no response message",
                service_unit_definition.get_service_bundle_identity().get_fqin(),
                service_unit_definition.get_unit_type()
            );
        };

        Ok(Some((request_message, response_message)))
    }

    fn serialize_configuration(
        configuration: Option<&Any>,
        descriptor: &MessageDescriptor,
    ) -> Result<Vec<u8>, ServicePublisherAccessorError> {
        configuration
            .map_or_else(
                || Empty::default_instance().write_to_bytes().context("Serialization failed"),
                |config| {
                    if descriptor.full_name() == PROTO_ANY_FULL_NAME {
                        config.write_to_bytes()
                    } else {
                        config
                            .unpack_dyn(descriptor)
                            .context("Failed unpacking")?
                            .ok_or(anyhow!("Type mismatch between {config:?} and {descriptor:?}"))?
                            .write_to_bytes_dyn()
                    }
                    .context("Serialization failed")
                },
            )
            .map_err(ServicePublisherAccessorError::MetadataParsingFailed)
    }
}

#[async_trait]
impl ServicePublisherSubscription for SdvRpcSubscription {
    async fn unsubscribe(self: Box<Self>) {
        // This drops `self` and therefore cancels the subscription via the `DropGuard`.
    }
}

fn get_either_or_ensure_equal(
    a: Option<MessageDescriptor>,
    b: Option<MessageDescriptor>,
) -> anyhow::Result<MessageDescriptor> {
    match (a, b) {
        (None, None) => Err(anyhow!("VSIDL provider was unable to provide RPC method descriptor")),
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
                Err(anyhow!("VSIDL provider returned conflicting RPC method descriptors"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::well_known_types::wrappers::{Int32Value, StringValue};
    use protobuf::MessageFull;

    #[test]
    fn any_full_name_matches_descriptor() {
        assert_eq!(Any::descriptor().full_name(), PROTO_ANY_FULL_NAME);
    }

    #[test]
    fn test_serialize_configuration_none() {
        let descriptor = Empty::descriptor();
        let result = SdvRpcClient::serialize_configuration(None, &descriptor).unwrap();
        let expected = Empty::new().write_to_bytes().unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_configuration_with_any_descriptor() {
        // The expected descriptor is `google.protobuf.Any`. The original `Any` message bytes are
        // returned directly without unpacking.
        let inner_message = StringValue { value: "test".to_string(), ..Default::default() };
        let config_any = Any::pack(&inner_message).unwrap();
        let config_bytes = config_any.write_to_bytes().unwrap();

        // The descriptor is for `Any` itself.
        let descriptor = Any::descriptor();
        assert_eq!(descriptor.full_name(), PROTO_ANY_FULL_NAME);

        let result = SdvRpcClient::serialize_configuration(Some(&config_any), &descriptor).unwrap();
        assert_eq!(result, config_bytes);
    }

    #[test]
    fn test_serialize_configuration_unpack_success() {
        // A specific message type is expected, and the `Any` message contains that type.
        // The function successfully unpacks the `Any` and serializes the inner message.
        let inner_message = StringValue { value: "test".to_string(), ..Default::default() };
        let expected_bytes = inner_message.write_to_bytes().unwrap();
        let config_any = Any::pack(&inner_message).unwrap();

        // The descriptor is for the inner message type (`StringValue`).
        let descriptor = StringValue::descriptor();
        assert_ne!(descriptor.full_name(), PROTO_ANY_FULL_NAME);

        let result = SdvRpcClient::serialize_configuration(Some(&config_any), &descriptor).unwrap();
        assert_eq!(result, expected_bytes);
    }

    #[test]
    fn test_serialize_configuration_type_mismatch() {
        // A specific message type is expected, but the `Any` message contains a different type.
        // An error indicating a type mismatch should be returned.
        let inner_message = StringValue { value: "test".to_string(), ..Default::default() };
        let config_any = Any::pack(&inner_message).unwrap();

        // An `Int32Value` is expected, but the `Any` message contains a `StringValue`.
        let descriptor = Int32Value::descriptor();
        assert_ne!(descriptor.full_name(), PROTO_ANY_FULL_NAME);

        let result = SdvRpcClient::serialize_configuration(Some(&config_any), &descriptor);

        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        // Check that the error is wrapped correctly and contains the expected message.
        assert!(error_str.contains("Failed to parse metadata"));
        assert!(error_str.contains("Type mismatch"));
    }

    #[test]
    fn test_serialize_configuration_unpack_failure_malformed_any() {
        // `Any` message's value bytes are corrupted or malformed.
        // An error indicating an unpacking failure should be returned
        let mut config_any = Any::new();
        // Set the correct type URL for StringValue.
        config_any.type_url =
            format!("type.googleapis.com/{}", StringValue::descriptor().full_name());
        // Provide invalid bytes that cannot be parsed as a StringValue.
        config_any.value = vec![0xDE, 0xAD, 0xBE, 0xEF];

        let descriptor = StringValue::descriptor();
        assert_ne!(descriptor.full_name(), PROTO_ANY_FULL_NAME);

        let result = SdvRpcClient::serialize_configuration(Some(&config_any), &descriptor);

        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        // Check that the error is wrapped correctly and contains the expected message.
        assert!(error_str.contains("Failed to parse metadata"));
        assert!(error_str.contains("Failed unpacking"));
    }
}
