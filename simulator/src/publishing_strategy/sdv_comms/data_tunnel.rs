// Copyright 2025 Google LLC

use anyhow::bail;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::Message;
use sdv_comms::ContextRef;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_simulator_publishing_strategy::PublishingStrategy;
use std::fmt::Debug;

use crate::prepare_registration_args;

pub(super) struct PublishingStrategyImpl {
    ctx: ContextRef,
    pub(super) message_count: u32,
    pub(super) message_size: u32,
    publisher: Option<DataTunnelPublisher>,
}

impl PublishingStrategyImpl {
    pub(super) fn new(ctx: ContextRef, message_count: u32, message_size: u32) -> Self {
        Self { ctx, message_count, message_size, publisher: None }
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

        let publisher = DataTunnelPublisher::register_topic(
            self.ctx,
            &unit_type,
            &unit_name,
            self.message_count,
            self.message_size,
            metadata.write_to_bytes()?,
        )
        .await?;
        self.publisher = Some(publisher);

        Ok(())
    }

    async fn unregister(&mut self) -> anyhow::Result<()> {
        if let Some(publisher) = self.publisher.take() {
            // Dropping the publisher unregisters it.
            drop(publisher);
        }

        Ok(())
    }

    fn publish(&mut self, message: &[u8]) -> anyhow::Result<()> {
        let Some(ref publisher) = self.publisher else {
            bail!("Publisher is not yet registered.");
        };
        publisher.publish_length_delimited_message(message)
    }
}

impl Debug for PublishingStrategyImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishingStrategyImpl")
            .field("ctx", &"_")
            .field("message_count", &self.message_count)
            .field("message_size", &self.message_size)
            .field("publisher", &self.publisher.as_ref().map(|_| "_"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;
    use googletest::expect_that;
    use googletest::matchers::{container_eq, contains_substring, displays_as, err, ok};
    use itertools::Itertools;
    use protobuf::{well_known_types::wrappers::Int32Value, MessageFull};
    use sdv_comms::{dt, sd::UnitType};
    use sdv_telemetry_utils::conversions::u32_into_usize;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::time::sleep;

    use crate::tests::{discover_service, make_context};

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn publish_messages_success() {
        const MESSAGE_COUNT: u32 = 10;
        const MESSAGE_SIZE: u32 = 10;
        let expected_messages =
            (1..=3).map(|value| Int32Value { value, ..Default::default() }).collect::<Vec<_>>();

        let ctx = make_context().await;

        let mut strategy = PublishingStrategyImpl::new(ctx, MESSAGE_COUNT, MESSAGE_SIZE);

        // Register publisher.
        strategy
            .register(
                "com.google.sdv.telemetry.tests.SdvCommsPublishingStrategyFactory#DtUnitType|unit-name",
                Int32Value::descriptor().full_name(),
                &[Int32Value::descriptor().file_descriptor_proto().to_owned()]
            )
            .await
            .unwrap();

        // Subscribe to publisher.
        let service_unit_definition = discover_service(
            ctx,
            &UnitType::from_str(
                "com.google.sdv.telemetry.tests.SdvCommsPublishingStrategyFactory#DtUnitType",
            )
            .unwrap(),
            "unit-name",
        )
        .await;
        let subscriber_stream = dt::SubscriberStream::create(
            ctx,
            &service_unit_definition.get_unit_identifier(),
            Duration::ZERO,
            dt::SubscriberOptions { fetch_last_message: false },
        )
        .await
        .unwrap();

        // Give the `SubscriberStream` some time to actually subscribe before publishing messages.
        // Otherwise, we run into test flakiness: b/473988762.
        sleep(Duration::from_millis(200)).await;

        for message in &expected_messages {
            strategy.publish(&message.write_to_bytes().unwrap()).unwrap();
        }

        strategy.unregister().await.unwrap();

        let messages = subscriber_stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map_ok(|(_, buffer)| {
                buffer
                    .chunks(u32_into_usize(MESSAGE_SIZE))
                    .map(|chunk| chunk.to_vec())
                    .collect::<Vec<_>>()
            })
            .flatten_ok()
            .collect::<Result<Vec<_>, _>>();

        expect_that!(
            messages,
            ok(container_eq(
                expected_messages
                    .iter()
                    .map(|expected_message| {
                        let mut buffer =
                            expected_message.write_length_delimited_to_bytes().unwrap();
                        if buffer.len() < u32_into_usize(MESSAGE_SIZE) {
                            buffer.resize(u32_into_usize(MESSAGE_SIZE), 0);
                        }
                        buffer
                    })
                    .collect::<Vec<_>>()
            ))
        );
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn publish_message_failure() {
        const MESSAGE_COUNT: u32 = 10;
        const MESSAGE_SIZE: u32 = 10;
        let ctx = make_context().await;

        let mut strategy = PublishingStrategyImpl::new(ctx, MESSAGE_COUNT, MESSAGE_SIZE);

        let result = strategy.publish(&[42].repeat(u32_into_usize(MESSAGE_SIZE)));

        expect_that!(
            result,
            err(displays_as(contains_substring("Publisher is not yet registered.")))
        );
    }
}
