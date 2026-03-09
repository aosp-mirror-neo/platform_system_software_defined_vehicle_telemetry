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

//! Telemetry subscription logic for SDV RPC.

use crate::unit_identifier_to_string;
use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::{any::Any, empty::Empty};
use protobuf::{Message, MessageDyn};
use sdv_comms::id::UnitIdentifier;
use sdv_comms::rpc;
use sdv_comms::sd::ServiceUnitDefinition;
use sdv_comms::ContextRef;
use sdv_config_provider::vsidl_provider_api::RpcMethodDescriptor;
use sdv_config_provider::vsidl_provider_api::VsidlConfigProvider;
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use sdv_telemetry_service_publisher_accessor::{
    CancellationCallback, CreateSubscriptionOutcome, FetchLastMessageOutcome,
    GetMessageProviderOutcome, MessageId, NewMessageCallback, ServicePublisherAccessorError,
    ServicePublisherMessageProvider, ServicePublisherSubscription,
};
use std::time::Duration;
use std::time::SystemTime;
use tokio::select;
use tokio::time::timeout;
use tokio_util::sync::{CancellationToken, DropGuard};

const CREATE_SUBSCRIPTION_METHOD: &str = "CreateSubscription";
const GET_LATEST_MESSAGE_METHOD: &str = "GetLatestMessage";
const PROTO_ANY_FULL_NAME: &str = "google.protobuf.Any";
const RPC_UNARY_CALL_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(not(test))]
const RPC_CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
/// For tests, use a shorter timeout to fail faster.
#[cfg(test)]
const RPC_CLIENT_TIMEOUT: Duration = Duration::from_secs(1);

struct SdvRpcSubscription {
    _subscription_drop_guard: DropGuard,
}

pub(super) async fn get_message_provider(
    ctx: ContextRef,
    unit_identifier: UnitIdentifier,
    config_descriptor: MessageDescriptor,
    message_descriptor: MessageDescriptor,
) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
    let mut client_options = rpc::ClientOptions::default();
    client_options.set_timeout(RPC_UNARY_CALL_TIMEOUT);
    let client = rpc::Client::create(ctx, unit_identifier.clone(), &client_options)
        .await
        .map_err(ServicePublisherAccessorError::RpcFailedToConnect)?;

    struct ServicePublisherMessageProviderImpl {
        client: rpc::Client,
        config_descriptor: MessageDescriptor,
        message_descriptor: MessageDescriptor,
    }

    #[async_trait]
    impl ServicePublisherMessageProvider for ServicePublisherMessageProviderImpl {
        #[tracing::instrument(skip(self, configuration), fields(
            publisher_type = "rpc",
            await_start_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(
                |e| e.duration()).as_nanos(),
        ))]
        async fn get_latest_message(
            &self,
            configuration: Option<&Any>,
        ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
            let configuration_bytes =
                serialize_configuration(configuration, &self.config_descriptor)?;
            let maybe_response_bytes = self
                .client
                .unary_call(GET_LATEST_MESSAGE_METHOD, &configuration_bytes)
                .await
                .map_or_else(
                    |err| match err.status_code() {
                        SdvStatusCode::NotFound => Ok(None),
                        _ => Err(ServicePublisherAccessorError::MessageRetrievalFailed(err.into())),
                    },
                    |response_bytes| Ok(Some(response_bytes)),
                )?;
            let Some(response_bytes) = maybe_response_bytes else { return Ok(None) };

            let response = self
                .message_descriptor
                .parse_from_bytes(&response_bytes)
                .map_err(|err| ServicePublisherAccessorError::MessageParsingFailed(err.into()))?;
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
    ctx: ContextRef,
    unit_identifier: UnitIdentifier,
    configuration: Option<&Any>,
    config_descriptor: MessageDescriptor,
    message_descriptor: MessageDescriptor,
    mut new_message_callback: NewMessageCallback,
    cancellation_callback: CancellationCallback,
) -> Result<CreateSubscriptionOutcome, ServicePublisherAccessorError> {
    // Note: **No timeout** is set on this client.
    //
    // We set a general timeout, `RPC_UNARY_CALL_TIMEOUT` by default, on gRPC cals from the
    // `client` in `get_latest_message()`, which is **only** used for unary gRPC calls.
    //
    // This `client` is used **only** for server-streaming calls, which gRPC treats as the
    // timeout for receiving the whole stream (not the first message). Defining a general
    // timeout for all server-streaming calls is tricky. Some server-streaming RPC calls might
    // justifiably take a few seconds, even longer.
    let client = rpc::Client::create(ctx, unit_identifier.clone(), &rpc::ClientOptions::default())
        .await
        .map_err(ServicePublisherAccessorError::RpcFailedToConnect)?;
    let configuration_bytes = serialize_configuration(configuration, &config_descriptor)?;

    let cancellation_token = CancellationToken::new();
    let subscription_drop_guard = cancellation_token.clone().drop_guard();

    tokio::spawn(async move {
        // `rpc::Client::server_streaming_call` awaits until the first item in the stream arrives
        // to return the stream. This might take a while, since RPC server-streaming calls might be
        // used for long-lived infrequent streams. The enclosing function is called not only in the
        // context of MC activation in a detached task but also during initialization in the
        // context of the main task. Thus, `server_streaming_call` should be invoked inside the
        // detached task to avoid blocking initialization on startup.
        let Ok(mut response_stream) =
            client.server_streaming_call(CREATE_SUBSCRIPTION_METHOD, &configuration_bytes).await
        else {
            cancellation_callback();
            return;
        };

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
                    let message_id = MessageId::new();
                    tracing::event!(
                        tracing::Level::INFO,
                        "New message by RPC Publisher, message_id = {}",
                        message_id
                    );

                    let buffer =
                        match result.context("RPC server stream produced an intermittent error") {
                            Ok(buffer) => buffer,
                            Err(err) => {
                                (new_message_callback)(Err(
                                    ServicePublisherAccessorError::MessageRetrievalFailed(err),
                                ));
                                continue;
                            }
                        };

                    debug!(
                        "Received 1 message from service unit '{}'.",
                        unit_identifier_to_string(&unit_identifier)
                    );

                    let message = message_descriptor
                        .parse_from_bytes(&buffer)
                        .context("Failed to parse the protobuf message")
                        .map(|msg| (msg, message_id.into()))
                        .map_err(ServicePublisherAccessorError::MessageParsingFailed);
                    (new_message_callback)(message);
                }
            }
        }
    });

    let subscription =
        Box::new(SdvRpcSubscription { _subscription_drop_guard: subscription_drop_guard });

    Ok(CreateSubscriptionOutcome::Subscribed {
        subscription,
        fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
    })
}

pub(super) async fn get_message_descriptor(
    vsidl_provider: &dyn VsidlConfigProvider,
    service_unit_definition: &ServiceUnitDefinition,
) -> anyhow::Result<Option<(MessageDescriptor, MessageDescriptor)>> {
    let (create_subscription_req, create_subscription_res) = get_req_res_descriptors_for_method(
        vsidl_provider,
        service_unit_definition,
        CREATE_SUBSCRIPTION_METHOD,
    )
    .await?
    .unzip();

    let (get_latest_message_req, get_latest_message_res) = get_req_res_descriptors_for_method(
        vsidl_provider,
        service_unit_definition,
        GET_LATEST_MESSAGE_METHOD,
    )
    .await?
    .unzip();

    let config_descriptor =
        get_either_or_ensure_equal(create_subscription_req, get_latest_message_req)
            .context("Ambiguous configuration descriptor")?;
    let message_descriptor =
        get_either_or_ensure_equal(create_subscription_res, get_latest_message_res)
            .context("Ambiguous message descriptor")?;

    Ok(config_descriptor.zip(message_descriptor))
}

async fn get_req_res_descriptors_for_method(
    vsidl_provider: &dyn VsidlConfigProvider,
    service_unit_definition: &ServiceUnitDefinition,
    method_name: &str,
) -> anyhow::Result<Option<(MessageDescriptor, MessageDescriptor)>> {
    let Ok(rpc_descriptor) = get_rpc_method_descriptor_with_timeout(
        vsidl_provider,
        service_unit_definition,
        method_name,
    )
    .await
    else {
        return Ok(None);
    };

    let Some(request_message) = rpc_descriptor.request_message else {
        bail!("RPC method descriptor has no request message descriptor");
    };
    let Some(response_message) = rpc_descriptor.response_message else {
        bail!("RPC method descriptor has no response message descriptor");
    };

    Ok(Some((request_message, response_message)))
}

async fn get_rpc_method_descriptor_with_timeout(
    vsidl_provider: &dyn VsidlConfigProvider,
    service_unit_definition: &ServiceUnitDefinition,
    method_name: &str,
) -> SdvResult<RpcMethodDescriptor> {
    let service_bundle_identity = service_unit_definition.get_service_bundle_identity();
    let fqin = service_bundle_identity.get_fqin();
    let unit_type = service_unit_definition.get_unit_type();
    let future = vsidl_provider.get_rpc_method_descriptor(fqin, &unit_type, method_name);

    timeout(RPC_CLIENT_TIMEOUT, future).await.map_err(|_| {
        SdvStatus::with_message(
            SdvStatusCode::DeadlineExceeded,
            format!(
                "Timed out getting RPC method descriptor for {fqin}|{unit_type} after {} seconds",
                RPC_CLIENT_TIMEOUT.as_secs()
            ),
        )
    })?
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

#[async_trait]
impl ServicePublisherSubscription for SdvRpcSubscription {
    async fn unsubscribe(self: Box<Self>) {
        // This drops `self` and therefore cancels the subscription via the `DropGuard`.
    }
}

fn get_either_or_ensure_equal(
    a: Option<MessageDescriptor>,
    b: Option<MessageDescriptor>,
) -> anyhow::Result<Option<MessageDescriptor>> {
    match (a, b) {
        (None, None) => Ok(None),
        (None, Some(descriptor)) | (Some(descriptor), None) => Ok(Some(descriptor)),
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
                Ok(Some(a))
            } else {
                Err(anyhow!("Conflicting RPC method descriptors"))
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
        let result = serialize_configuration(None, &descriptor).unwrap();
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

        let result = serialize_configuration(Some(&config_any), &descriptor).unwrap();
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

        let result = serialize_configuration(Some(&config_any), &descriptor).unwrap();
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

        let result = serialize_configuration(Some(&config_any), &descriptor);

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

        let result = serialize_configuration(Some(&config_any), &descriptor);

        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        // Check that the error is wrapped correctly and contains the expected message.
        assert!(error_str.contains("Failed to parse metadata"));
        assert!(error_str.contains("Failed unpacking"));
    }
}
