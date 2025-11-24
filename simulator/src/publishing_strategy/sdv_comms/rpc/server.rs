// Copyright 2025 Google LLC

use futures::{
    future,
    stream::{StreamExt, TryStreamExt},
    FutureExt,
};
use parking_lot::Mutex;
use protobuf::Message;
use sdv_comms::{
    rpc, sd::ServerUnitMetadata, sd::UnitMetadata, sd::UnitRegistrationArgs, sd::UnitType,
    ContextRef,
};
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_status_rs::{SdvStatus, SdvStatusCode};
use sdv_telemetry_utils::observable_value::ObservableValue;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Weak};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

type ObservableMessage = ObservableValue<Vec<u8>, 100>;

pub(super) struct Server {
    // We need to keep a reference to the RPC server, because it is unregistered when dropped.
    _rpc_server: rpc::Server,
    latest_message: Arc<Mutex<ObservableMessage>>,
}

impl Server {
    pub(super) async fn new(
        ctx: ContextRef,
        unit_type: &UnitType,
        unit_name: &str,
        metadata: &MessageMetadata,
    ) -> anyhow::Result<Self> {
        let latest_message = Arc::new(Mutex::new(ObservableValue::new()));

        let unit_registration_args =
            Self::create_registration_args(unit_type, unit_name, &metadata.write_to_bytes()?);

        let rpc_server = rpc::Server::create_with_generic_handlers(
            ctx,
            unit_registration_args,
            Self::make_methods_map(Arc::downgrade(&latest_message)),
            &rpc::ServerOptions::default(),
        )
        .await?;

        Ok(Self { _rpc_server: rpc_server, latest_message })
    }

    pub(super) fn publish(&self, message: Vec<u8>) {
        self.latest_message.lock().set(message);
    }

    fn create_registration_args(
        unit_type: &UnitType,
        unit_name: &str,
        application_metadata: &[u8],
    ) -> UnitRegistrationArgs {
        let mut unit_registration_args = UnitRegistrationArgs::new();
        unit_registration_args.set_unit_type(unit_type);
        unit_registration_args.set_unit_name(unit_name);
        unit_registration_args.set_application_metadata(application_metadata);
        unit_registration_args.set_unit_metadata(&UnitMetadata::Server({
            let mut server_unit_metadata = ServerUnitMetadata::new();
            server_unit_metadata.set_version(1);
            server_unit_metadata
        }));

        unit_registration_args
    }

    fn make_methods_map(
        latest_message: Weak<Mutex<ObservableMessage>>,
    ) -> HashMap<String, rpc::RpcMethod> {
        HashMap::from([
            (
                "CreateSubscription".to_owned(),
                rpc::RpcMethod::ServerStreaming(Self::make_create_subscription_method(
                    Weak::clone(&latest_message),
                )),
            ),
            (
                "GetLatestMessage".to_owned(),
                rpc::RpcMethod::Unary(Self::make_get_latest_message_method(latest_message)),
            ),
        ])
    }

    fn make_create_subscription_method(
        latest_message: Weak<Mutex<ObservableMessage>>,
    ) -> rpc::ServerStreamingRpcMethod {
        Arc::new(move |_, _configuration| {
            future::ready(
                latest_message
                    .upgrade()
                    .ok_or_else(|| {
                        SdvStatus::with_message(
                            SdvStatusCode::Unavailable,
                            "The RPC server has been unregistered.",
                        )
                    })
                    .map(|latest_message| {
                        latest_message
                            .lock()
                            .observe()
                            .map_err(|err| match err {
                                BroadcastStreamRecvError::Lagged(skipped_count) => SdvStatus::with_cause_and_message(
                                    SdvStatusCode::DataLoss,
                                    err,
                                    format!("Failed to read generated data fast enough (skipped {skipped_count} messages)."),
                                ),
                            })
                            .boxed()
                    })
                )
            .boxed()
        })
    }

    fn make_get_latest_message_method(
        latest_message: Weak<Mutex<ObservableMessage>>,
    ) -> rpc::UnaryRpcMethod {
        Arc::new(move |_, _configuration| {
            future::ready(
                latest_message
                    .upgrade()
                    .ok_or_else(|| {
                        SdvStatus::with_message(
                            SdvStatusCode::Unavailable,
                            "The RPC server has been unregistered.",
                        )
                    })
                    .and_then(|latest_message| {
                        latest_message
                            .lock()
                            .get()
                            .cloned()
                            .ok_or_else(|| SdvStatus::new(SdvStatusCode::NotFound))
                    }),
            )
            .boxed()
        })
    }
}

impl Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("_rpc_server", &"_")
            .field("latest_message", &self.latest_message)
            .finish()
    }
}
