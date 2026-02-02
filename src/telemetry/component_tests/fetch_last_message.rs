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

#![cfg(test)]

use futures::future::join_all;
use futures::StreamExt;
use googletest::expect_that;
use googletest::matchers::{eq, ok, pat, pointwise, predicate, result_of_ref, some};
use mockall::predicate as m;
use protobuf::well_known_types::any::Any;
use protobuf::well_known_types::empty::Empty;
use protobuf::well_known_types::wrappers::Int64Value;
use protobuf::{text_format, Message, MessageField, MessageFull};
use sdv_telemetry_proto_rust::metrics_configuration::{
    MetricsConfig as MetricsConfigProto, MetricsReport as MetricsReportProto,
};
use sdv_telemetry_service_publisher_accessor::{
    CreateSubscriptionOutcome, FetchLastMessageOutcome, MockServicePublisherAccessor,
    MockServicePublisherSubscription, ServicePublisherMetadata, ServicePublisherSubscription,
    SubSamplingInterval,
};
use sdv_telemetry_test_utils::parameterized_test;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::metrics_config_manager::MetricsConfigManager;
use crate::metrics_config_uuid::MetricsConfigUuid;

const SERVICE_NAME: &str = "a_service_name";
const MANUAL_TRIGGER_SERVICE_NAME: &str = "manual_trigger_service";

fn create_metrics_config(
    uuid: &MetricsConfigUuid,
    fetch_last_message: &bool,
) -> MetricsConfigProto {
    text_format::parse_from_str::<MetricsConfigProto>(&format!(
        r#"
uuid: "{uuid}"

sources {{
    name: "manual_publisher"
    data_source {{
        source_identifier: "{MANUAL_TRIGGER_SERVICE_NAME}"
        connection_type: SUBSCRIPTION
        fetch_last_message: false
    }}
}}

triggers {{
    name: "manual_trigger"
    data_trigger {{ source_name: "manual_publisher" }}
}}

sources {{
    name: "a_publisher"
    data_source {{
        source_identifier: "{SERVICE_NAME}"
        connection_type: SUBSCRIPTION
        fetch_last_message: {fetch_last_message}
    }}
}}

# index 0
expression_nodes {{ field_leaf_node {{ source_name: "a_publisher" field_names: "value" }} }}

deactivate_trigger_name: "manual_trigger"

metrics_report_configs {{
    name: "Report"
    trigger_names: "manual_trigger"
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
        fetch_last_message = if *fetch_last_message { "true" } else { "false" }
    ))
    .unwrap()
}

fn create_mock_subscription() -> Box<dyn ServicePublisherSubscription> {
    let mut subscription = MockServicePublisherSubscription::new();
    subscription.expect_unsubscribe().once().return_const(());

    Box::new(subscription)
}

fn expect_manual_trigger_service(
    service_publisher_accessor: &mut MockServicePublisherAccessor,
) -> impl Fn() + Send + 'static {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut rx = UnboundedReceiverStream::new(rx);

    service_publisher_accessor
        .expect_get_metadata()
        .with(m::eq(MANUAL_TRIGGER_SERVICE_NAME))
        .returning(|_service_name| {
            Ok(Some(ServicePublisherMetadata { message_descriptor: Empty::descriptor() }))
        });
    service_publisher_accessor
        .expect_create_subscription()
        .once()
        .with(
            /* service_name */ m::eq(MANUAL_TRIGGER_SERVICE_NAME),
            /* configuration */ m::always(),
            /* fetch_last_message */ m::eq(false),
            /* sub_sampling_interval */ m::eq(SubSamplingInterval::ZERO),
            /* new_message_callback */ m::always(),
            /* cancellation_callback */ m::always(),
        )
        .return_once(
            move |_service_name,
                  _configuration,
                  _fetch_last_message,
                  _sub_sampling_interval,
                  mut new_message_callback,
                  _cancellation_callback| {
                task::spawn(async move {
                    while let Some(()) = rx.next().await {
                        new_message_callback(Ok((Box::new(Empty::default()), None)));
                    }
                });

                Ok(CreateSubscriptionOutcome::Subscribed {
                    subscription: create_mock_subscription(),
                    fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
                })
            },
        );

    move || tx.send(()).unwrap()
}

static CONFIG_UUIDS: LazyLock<[MetricsConfigUuid; 3]> = LazyLock::new(|| {
    [
        MetricsConfigUuid::from_str("1a7f4387-2a8f-4e96-8b1e-d35af9276271").unwrap(),
        MetricsConfigUuid::from_str("2a7f4387-2a8f-4e96-8b1e-d35af9276271").unwrap(),
        MetricsConfigUuid::from_str("3a7f4387-2a8f-4e96-8b1e-d35af9276271").unwrap(),
    ]
});

parameterized_test!(
    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    fetch_last_message_works_with_concurrent_metrics_configs_for_same_publisher,

    |configs: Vec<bool>| async move {
        run_test(configs).await;
    },

    r#false: (vec![false]),
    r#true:  (vec![true]),

    false_false: (vec![false, false]),
    false_true:  (vec![false, true]),
    true_false:  (vec![true,  false]),
    true_true:   (vec![true,  true]),

    false_false_false: (vec![false, false, false]),
    false_false_true:  (vec![false, false, true]),
    false_true_false:  (vec![false, true,  false]),
    false_true_true:   (vec![false, true,  true]),
    true_false_false:  (vec![true,  false, false]),
    true_false_true:   (vec![true,  false, true]),
    true_true_false:   (vec![true,  true,  false]),
    true_true_true:    (vec![true,  true,  true]),
);

async fn run_test(configs: Vec<bool>) {
    let configs = configs
        .iter()
        .enumerate()
        .map(|(i, fetch_last_message)| {
            let config_uuid = &CONFIG_UUIDS[i];
            (
                config_uuid,
                fetch_last_message,
                create_metrics_config(config_uuid, fetch_last_message),
            )
        })
        .collect::<Vec<_>>();

    let mut service_publisher_accessor = MockServicePublisherAccessor::new();
    service_publisher_accessor.expect_get_message_provider().never();
    service_publisher_accessor.expect_register_availability_callback().never();

    let manual_trigger = expect_manual_trigger_service(&mut service_publisher_accessor);

    service_publisher_accessor.expect_get_metadata().with(m::eq(SERVICE_NAME)).returning(
        |_service_name| {
            Ok(Some(ServicePublisherMetadata { message_descriptor: Int64Value::descriptor() }))
        },
    );

    for flm_enabled in [false, true] {
        // If any metrics config has `fetch_last_message: $enabled`, we expect a
        // `create_subscription` call with `fetch_last_message: $enabled` that returns
        // `FetchLastMessageOutcome::new_supported($enabled)`.
        if configs.iter().any(|(_, fetch_last_message, _)| flm_enabled == **fetch_last_message) {
            service_publisher_accessor
                .expect_create_subscription()
                .once()
                .with(
                    /* service_name */ m::eq(SERVICE_NAME),
                    /* configuration */ m::always(),
                    /* fetch_last_message */ m::eq(flm_enabled),
                    /* sub_sampling_interval */ m::eq(SubSamplingInterval::ZERO),
                    /* new_message_callback */ m::always(),
                    /* cancellation_callback */ m::always(),
                )
                .returning(
                    move |_service_name,
                          _configuration,
                          _fetch_last_message,
                          _sub_sampling_interval,
                          mut new_message_callback,
                          _cancellation_callback| {
                        if flm_enabled {
                            new_message_callback(Ok((
                                Box::new(Int64Value { value: 123, ..Default::default() }),
                                None,
                            )));
                        }

                        Ok(CreateSubscriptionOutcome::Subscribed {
                            subscription: create_mock_subscription(),
                            fetch_last_message_outcome: FetchLastMessageOutcome::new_supported(
                                flm_enabled,
                            ),
                        })
                    },
                );
        }
    }

    let (manager, metrics_report_rx, _status_rx, _temp_dirs) =
        super::create_manager(vec![Arc::new(service_publisher_accessor)]).await;

    for (_, _, config) in &configs {
        manager.add_metrics_config(config.write_to_bytes().unwrap().as_ref()).await.unwrap();
    }
    for (config_uuid, _, _) in &configs {
        manager.activate_metrics_config(&config_uuid.to_string()).await.unwrap();
        sleep(Duration::from_millis(100)).await;
    }

    manual_trigger();
    join_all(
        configs.iter().map(|(config_uuid, _, _)| {
            super::wait_for_metrics_config_inactive(&manager, config_uuid)
        }),
    )
    .await;

    manager.remove_all_metrics_configs().await.unwrap();
    drop(manager);

    let mut metrics_reports = metrics_report_rx.take(configs.len()).collect::<Vec<_>>().await;
    metrics_reports.sort_by_key(|metrics_report| metrics_report.metrics_config_uuid.to_owned());

    expect_that!(
        metrics_reports,
        pointwise!(
            |(expected_config_uuid, expected_value)| pat!(MetricsReportProto {
                metrics_config_uuid: predicate(
                    move |config_uuid: &String| *config_uuid == expected_config_uuid
                ),
                report_data: result_of_ref!(
                    |report_data: &MessageField<Any>| report_data
                        .to_owned()
                        .into_option()
                        .map(|report_data| report_data.unpack::<Int64Value>()),
                    some(ok(some(pat!(Int64Value { value: eq(expected_value) }))))
                ),
            }),
            configs.into_iter().map(|(config_uuid, fetch_last_message, _)| {
                (config_uuid.to_string(), if *fetch_last_message { &123 } else { &0 })
            })
        ),
    );
}
