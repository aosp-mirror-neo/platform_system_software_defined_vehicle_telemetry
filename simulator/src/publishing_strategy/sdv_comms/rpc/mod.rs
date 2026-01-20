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

mod server;

use anyhow::bail;
use protobuf::descriptor::FileDescriptorProto;
use sdv_comms::ContextRef;
use sdv_telemetry_simulator_publishing_strategy::PublishingStrategy;
use std::fmt::Debug;

use crate::prepare_registration_args;
use crate::rpc::server::Server;

pub(super) struct PublishingStrategyImpl {
    ctx: ContextRef,
    server: Option<Server>,
}

impl PublishingStrategyImpl {
    pub(super) fn new(ctx: ContextRef) -> Self {
        Self { ctx, server: None }
    }
}

#[async_trait::async_trait]
impl PublishingStrategy for PublishingStrategyImpl {
    async fn register(
        &mut self,
        service_name: &str,
        message_name: &str,
        file_descriptor_protos: &[FileDescriptorProto],
    ) -> anyhow::Result<()> {
        let (unit_type, unit_name, metadata) = prepare_registration_args(
            service_name,
            message_name,
            file_descriptor_protos.to_owned(),
        )?;

        self.server = Some(Server::new(self.ctx, &unit_type, &unit_name, &metadata).await?);

        Ok(())
    }

    async fn unregister(&mut self) -> anyhow::Result<()> {
        if let Some(server) = self.server.take() {
            // Dropping the server unregisters it.
            drop(server);
        }

        Ok(())
    }

    fn publish(&mut self, message: &[u8]) -> anyhow::Result<()> {
        let Some(ref mut server) = self.server else {
            bail!("Publisher is not yet registered.");
        };
        server.publish(message.to_owned());

        Ok(())
    }
}

impl Debug for PublishingStrategyImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishingStrategyImpl")
            .field("ctx", &"_")
            .field("server", &self.server)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::TryStreamExt;
    use googletest::expect_that;
    use googletest::matchers::{
        container_eq, contains_substring, displays_as, err, none, ok, some,
    };
    use protobuf::Message;
    use protobuf::{well_known_types::wrappers::Int32Value, MessageFull};
    use sdv_comms::{rpc, sd::UnitType};
    use sdv_status_rs::{SdvStatus, SdvStatusCode};
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::time::sleep;

    use crate::tests::{discover_service, make_context};

    async fn get_latest_message(client: &rpc::Client) -> Result<Option<Vec<u8>>, SdvStatus> {
        client.unary_call("GetLatestMessage", &[]).await.map_or_else(
            |err| match err.status_code() {
                SdvStatusCode::NotFound => Ok(None),
                _ => Err(err),
            },
            |response_bytes| Ok(Some(response_bytes)),
        )
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn publish_messages_success() {
        let expected_messages =
            (1..=3).map(|value| Int32Value { value, ..Default::default() }).collect::<Vec<_>>();
        let last_expected_message = expected_messages.last().unwrap();

        let ctx = make_context().await;

        let mut strategy = PublishingStrategyImpl::new(ctx);

        // Register publisher.
        strategy
            .register(
                "com.google.sdv.telemetry.tests.SdvCommsPublishingStrategyFactory#RpcUnitType|unit-name",
                Int32Value::descriptor().full_name(),
                &[Int32Value::descriptor().file_descriptor_proto().to_owned()]
            )
            .await
            .unwrap();

        // Subscribe to publisher.
        let service_unit_definition = discover_service(
            ctx,
            &UnitType::from_str(
                "com.google.sdv.telemetry.tests.SdvCommsPublishingStrategyFactory#RpcUnitType",
            )
            .unwrap(),
            "unit-name",
        )
        .await;
        let client = rpc::Client::create(
            ctx,
            service_unit_definition.get_unit_identifier(),
            &rpc::ClientOptions::default(),
        )
        .await
        .unwrap();

        let subscriber_stream =
            client.server_streaming_call("CreateSubscription", &[]).await.unwrap();

        expect_that!(get_latest_message(&client).await, ok(none()));

        for message in &expected_messages {
            strategy.publish(&message.write_to_bytes().unwrap()).unwrap();
        }

        sleep(Duration::from_millis(500)).await;

        expect_that!(
            get_latest_message(&client).await,
            ok(some(container_eq(last_expected_message.write_to_bytes().unwrap())))
        );

        strategy.unregister().await.unwrap();

        let messages = subscriber_stream.try_collect::<Vec<_>>().await;

        expect_that!(
            messages,
            ok(container_eq(
                expected_messages
                    .iter()
                    .map(|expected_message| expected_message.write_to_bytes().unwrap())
                    .collect::<Vec<_>>()
            ))
        );
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn publish_message_failure() {
        let ctx = make_context().await;

        let mut strategy = PublishingStrategyImpl::new(ctx);

        let result = strategy.publish(&[1, 2, 3]);

        expect_that!(
            result,
            err(displays_as(contains_substring("Publisher is not yet registered.")))
        );
    }
}
