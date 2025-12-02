// Copyright 2025 Google LLC

#![cfg(test)]

use mockall::predicate as m;
use protobuf::well_known_types::wrappers::Int64Value;
use protobuf::{text_format, Message, MessageFull};
use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig as MetricsConfigProto;
use sdv_telemetry_service_publisher_accessor::{
    CreateSubscriptionOutcome, FetchLastMessageOutcome, MockAvailabilityCallbackRegistration,
    MockServicePublisherAccessor, MockServicePublisherSubscription, ServicePublisherMetadata,
    SubSamplingInterval,
};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

use crate::metrics_config_manager::MetricsConfigManager;
use crate::metrics_config_uuid::MetricsConfigUuid;

const SERVICE_NAME: &str = "a_service_name";
static CONFIG_UUID: LazyLock<MetricsConfigUuid> =
    LazyLock::new(|| MetricsConfigUuid::from_str("4a7f4387-2a8f-4e96-8b1e-d35af9276271").unwrap());

// Creates a simple Metrics Config with a Service Publisher, a Data Trigger subscribed to it, and
// Metrics Reports being generated on Data Trigger firing.
fn create_metrics_config() -> MetricsConfigProto {
    text_format::parse_from_str::<MetricsConfigProto>(&format!(
        r#"
uuid: "{config_uuid}"

publishers {{
    name: "a_publisher"
    service_publisher {{
        service_name: "{SERVICE_NAME}"
        connection_type: SUBSCRIPTION
    }}
}}

triggers {{
    name: "a_trigger"
    data_trigger {{ publisher_name: "a_publisher" }}
}}

# index 0
expression_nodes {{ field_leaf_node {{ publisher_name: "a_publisher" field_names: "value" }} }}

metrics_report_configs {{
    name: "Report"
    trigger_names: "a_trigger"
    report_incomplete: false

    message_builder {{
        message_type: ".google.protobuf.Int64Value"
        field_assignments {{
            field_name: "value"
            no_aggregation {{ expression_node_index: 0 }}
        }}
    }}
}}
"#,
        config_uuid = *CONFIG_UUID
    ))
    .unwrap()
}

#[googletest::test]
#[sdv_telemetry_test_utils::timeout]
#[tokio::test]
async fn test_deactivation_while_pending() {
    let config = create_metrics_config();

    let (availability_callback_tx, availability_callback_rx) = oneshot::channel();
    let (cancellation_callback_tx, cancellation_callback_rx) = oneshot::channel();

    let mut service_publisher_accessor = MockServicePublisherAccessor::new();
    service_publisher_accessor.expect_get_message_provider().never();
    service_publisher_accessor.expect_get_metadata().with(m::eq(SERVICE_NAME)).returning(
        |_service_name| {
            Ok(Some(ServicePublisherMetadata { message_descriptor: Int64Value::descriptor() }))
        },
    );
    service_publisher_accessor
        .expect_create_subscription()
        .once()
        .withf(
            move |service_name,
                  configuration,
                  fetch_last_message,
                  sub_sampling_interval,
                  _new_message_callback,
                  _cancellation_callback| {
                service_name == SERVICE_NAME
                    && configuration.is_none()
                    && !fetch_last_message
                    && *sub_sampling_interval == SubSamplingInterval::ZERO
            },
        )
        .return_once(
            move |_service_name,
                  _configuration,
                  _fetch_last_message,
                  _sub_sampling_interval,
                  new_message_callback,
                  cancellation_callback| {
                Ok(CreateSubscriptionOutcome::NotRegistered {
                    new_message_callback,
                    cancellation_callback,
                })
            },
        );
    service_publisher_accessor
        .expect_register_availability_callback()
        .once()
        .withf(move |service_name, _callback| service_name == SERVICE_NAME)
        .return_once(move |_service_name, callback| {
            let callback = Arc::clone(callback);
            tokio::spawn(async move {
                availability_callback_tx
                    .send(callback)
                    .unwrap_or_else(|_| panic!("Failed to send callback."));
            });
            Ok(Some(Box::new({
                let mut subscription = MockAvailabilityCallbackRegistration::new();
                subscription.expect_unregister().once().return_const(());
                subscription
            })))
        });
    service_publisher_accessor
        .expect_create_subscription()
        .once()
        .withf(
            move |service_name,
                  configuration,
                  fetch_last_message,
                  sub_sampling_interval,
                  _new_message_callback,
                  _cancellation_callback| {
                service_name == SERVICE_NAME
                    && configuration.is_none()
                    && !fetch_last_message
                    && *sub_sampling_interval == SubSamplingInterval::ZERO
            },
        )
        .return_once(
            move |_service_name,
                  _configuration,
                  _fetch_last_message,
                  _sub_sampling_interval,
                  _new_message_callback,
                  cancellation_callback| {
                tokio::spawn(async move {
                    cancellation_callback_tx
                        .send(cancellation_callback)
                        .unwrap_or_else(|_| panic!("Failed to send callback."));
                });
                Ok(CreateSubscriptionOutcome::Subscribed {
                    subscription: Box::new(MockServicePublisherSubscription::new()),
                    fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
                })
            },
        );
    service_publisher_accessor
        .expect_create_subscription()
        .once()
        .withf(
            move |service_name,
                  configuration,
                  fetch_last_message,
                  sub_sampling_interval,
                  _new_message_callback,
                  _cancellation_callback| {
                service_name == SERVICE_NAME
                    && configuration.is_none()
                    && !fetch_last_message
                    && *sub_sampling_interval == SubSamplingInterval::ZERO
            },
        )
        .return_once(
            move |_service_name,
                  _configuration,
                  _fetch_last_message,
                  _sub_sampling_interval,
                  _new_message_callback,
                  _cancellation_callback| {
                Ok(CreateSubscriptionOutcome::Subscribed {
                    subscription: Box::new({
                        let mut subscription = MockServicePublisherSubscription::new();
                        subscription.expect_unsubscribe().once().return_const(());
                        subscription
                    }),
                    fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
                })
            },
        );

    let (manager, _metrics_report_rx, _status_rx, _temp_dirs) =
        super::create_manager(vec![Arc::new(service_publisher_accessor)]).await;

    manager.add_metrics_config(config.write_to_bytes().unwrap().as_ref()).await.unwrap();

    manager.activate_metrics_config(&config.uuid.to_string()).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    let availability_callback = availability_callback_rx.await.unwrap();
    availability_callback();
    sleep(Duration::from_millis(200)).await;

    let cancellation_callback = cancellation_callback_rx.await.unwrap();
    cancellation_callback();
    sleep(Duration::from_millis(200)).await;

    manager.deactivate_metrics_config(&config.uuid.to_string()).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    drop(manager);
}
