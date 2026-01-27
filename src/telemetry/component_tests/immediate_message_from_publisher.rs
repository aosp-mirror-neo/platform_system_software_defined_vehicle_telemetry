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

use futures::StreamExt;
use googletest::expect_that;
use googletest::matchers::{eq, ok, pat, pointwise, result_of_ref, some};
use mockall::predicate as m;
use protobuf::well_known_types::{
    any::Any,
    wrappers::{BoolValue, Int64Value},
};
use protobuf::{Message, MessageField, MessageFull};
use sdv_telemetry_proto_rust::expression::{node::Node_type, FieldLeafNode, Node};
use sdv_telemetry_proto_rust::metrics_configuration::{
    data_source::ConnectionType, proto_message_builder::field_assignment::Aggregated_field_value,
    proto_message_builder::field_assignment::NoAggregation, proto_message_builder::FieldAssignment,
    source::Source_type, trigger::Trigger_type, Aggregator, DataSource, DataTrigger,
    MetricsConfig as MetricsConfigProto, MetricsReport as MetricsReportProto, MetricsReportConfig,
    ProtoMessageBuilder, Source, Trigger,
};
use sdv_telemetry_service_publisher_accessor::{
    CreateSubscriptionOutcome, FetchLastMessageOutcome, MockServicePublisherAccessor,
    MockServicePublisherSubscription, ServicePublisherAccessor, ServicePublisherMetadata,
    SubSamplingInterval,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::metrics_config_manager::MetricsConfigManager;

const CONFIG_UUID: &str = "b9e9e36c-e8b9-4524-86f4-168551b57103";
const SERVICE_NAME: &str = "service_name";
const OTHER_SERVICE_NAME: &str = "other_service_name";
const AGGREGATION_PUBLISHER_NAME: &str = "aggregation_publisher";

fn make_metrics_config() -> MetricsConfigProto {
    MetricsConfigProto {
        uuid: CONFIG_UUID.to_owned(),
        sources: vec![
            make_service_publisher(SERVICE_NAME),
            make_service_publisher(OTHER_SERVICE_NAME),
            // Aggregation publisher runs under a service publisher data trigger and performs
            // no_aggregation over this publisher's message
            make_aggregation_publisher(AGGREGATION_PUBLISHER_NAME, SERVICE_NAME, 0),
        ],
        triggers: vec![
            make_data_trigger(SERVICE_NAME),
            make_data_trigger(OTHER_SERVICE_NAME),
            make_data_trigger(AGGREGATION_PUBLISHER_NAME),
        ],
        // Expression nodes are FieldLeaf nodes accessing the message of the given service publisher
        expression_nodes: vec![
            make_expression_node(SERVICE_NAME),
            make_expression_node(AGGREGATION_PUBLISHER_NAME),
        ],
        // Reports are generated under a data trigger corresponding to the given publisher.
        // Aggregation is of no_aggregation type over the message contained by the publisher.
        metrics_report_configs: vec![
            make_report_config(SERVICE_NAME, 0),
            make_report_config(AGGREGATION_PUBLISHER_NAME, 1),
        ],
        // Fires never
        deactivate_trigger_name: make_data_trigger_name(OTHER_SERVICE_NAME),
        ..Default::default()
    }
}

fn make_service_publisher(name: &str) -> Source {
    Source {
        name: name.to_owned(),
        source_type: Some(Source_type::DataSource(DataSource {
            source_identifier: name.to_owned(),
            connection_type: ConnectionType::SUBSCRIPTION.into(),
            fetch_last_message: false,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn make_aggregation_publisher(name: &str, service_publisher: &str, node_index: u32) -> Source {
    Source {
        name: name.to_owned(),
        source_type: Some(Source_type::Aggregator(Aggregator {
            trigger_names: vec![make_data_trigger_name(service_publisher).to_owned()],
            message_builder: Some(make_message_builder(node_index)).into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn make_data_trigger_name(publisher_name: &str) -> String {
    format!("{publisher_name}_trigger")
}

fn make_data_trigger(publisher_name: &str) -> Trigger {
    Trigger {
        name: make_data_trigger_name(publisher_name),
        trigger_type: Some(Trigger_type::DataTrigger(DataTrigger {
            source_name: publisher_name.to_owned(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn make_expression_node(publisher_name: &str) -> Node {
    Node {
        node_type: Some(Node_type::FieldLeafNode(FieldLeafNode {
            source_name: publisher_name.to_owned(),
            field_names: vec!["value".to_owned()],
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn make_report_config(publisher_name: &str, node_index: u32) -> MetricsReportConfig {
    MetricsReportConfig {
        name: format!("{publisher_name}_report"),
        report_incomplete: false,
        trigger_names: vec![make_data_trigger_name(publisher_name)],
        message_builder: Some(make_message_builder(node_index)).into(),
        ..Default::default()
    }
}

fn make_message_builder(node_index: u32) -> ProtoMessageBuilder {
    ProtoMessageBuilder {
        message_type: ".google.protobuf.Int64Value".to_owned(),
        field_assignments: vec![FieldAssignment {
            field_name: "value".to_owned(),
            aggregated_field_value: Some(Aggregated_field_value::NoAggregation(NoAggregation {
                expression_node_index: Some(node_index),
                ..Default::default()
            })),
            ..Default::default()
        }],
        ..Default::default()
    }
}

struct SlowAccessor {}

#[async_trait::async_trait]
impl ServicePublisherAccessor for SlowAccessor {
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> Result<
        Option<ServicePublisherMetadata>,
        sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError,
    > {
        if service_name != OTHER_SERVICE_NAME {
            return Ok(None);
        }
        Ok(Some(ServicePublisherMetadata { message_descriptor: BoolValue::descriptor() }))
    }

    async fn get_message_provider(
        &self,
        _service_name: &str,
    ) -> Result<
        sdv_telemetry_service_publisher_accessor::GetMessageProviderOutcome,
        sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError,
    > {
        unimplemented!();
    }

    async fn create_subscription<'a>(
        &'a self,
        service_name: &'a str,
        _configuration: Option<&'a Any>,
        _fetch_last_message: bool,
        _sub_sampling_interval: &'a SubSamplingInterval,
        new_message_callback: sdv_telemetry_service_publisher_accessor::NewMessageCallback,
        cancellation_callback: sdv_telemetry_service_publisher_accessor::CancellationCallback,
    ) -> Result<
        CreateSubscriptionOutcome,
        sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError,
    > {
        if service_name != OTHER_SERVICE_NAME {
            return Ok(CreateSubscriptionOutcome::NotRegistered {
                new_message_callback,
                cancellation_callback,
            });
        }
        sleep(Duration::from_secs(1)).await;

        let mut subscription = MockServicePublisherSubscription::new();
        subscription.expect_unsubscribe().once().return_const(());
        Ok(CreateSubscriptionOutcome::Subscribed {
            subscription: Box::new(subscription),
            fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
        })
    }

    async fn register_availability_callback(
        &self,
        _service_name: &str,
        _callback: &sdv_telemetry_service_publisher_accessor::AvailabilityCallback,
    ) -> anyhow::Result<
        Option<Box<dyn sdv_telemetry_service_publisher_accessor::AvailabilityCallbackRegistration>>,
    > {
        unimplemented!();
    }
}

#[googletest::test]
#[sdv_telemetry_test_utils::timeout(5_000)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn report_is_generated_when_publisher_publishes_immediately() {
    let mut service_publisher_accessor = MockServicePublisherAccessor::new();

    service_publisher_accessor
        .expect_get_metadata()
        .with(m::eq(OTHER_SERVICE_NAME))
        .returning(|_service_name| Ok(None));
    service_publisher_accessor
        .expect_create_subscription()
        .with(
            /* service_name */ m::eq(OTHER_SERVICE_NAME),
            /* configuration */ m::always(),
            /* fetch_last_message */ m::always(),
            /* sub_sampling_interval */ m::eq(SubSamplingInterval::ZERO),
            /* new_message_callback */ m::always(),
            /* cancellation_callback */ m::always(),
        )
        .returning(
            |_service_name,
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

    service_publisher_accessor.expect_get_metadata().times(4).with(m::eq(SERVICE_NAME)).returning(
        |_service_name| {
            Ok(Some(ServicePublisherMetadata { message_descriptor: Int64Value::descriptor() }))
        },
    );
    service_publisher_accessor
        .expect_create_subscription()
        .once()
        .with(
            /* service_name */ m::eq(SERVICE_NAME),
            /* configuration */ m::always(),
            /* fetch_last_message */ m::always(),
            /* sub_sampling_interval */ m::eq(SubSamplingInterval::ZERO),
            /* new_message_callback */ m::always(),
            /* cancellation_callback */ m::always(),
        )
        .returning(
            |_service_name,
             _configuration,
             _fetch_last_message,
             _sub_sampling_interval,
             mut new_message_callback,
             _cancellation_callback| {
                new_message_callback(Ok(Box::new(Int64Value { value: 123, ..Default::default() })));

                let mut subscription = MockServicePublisherSubscription::new();
                subscription.expect_unsubscribe().once().return_const(());

                Ok(CreateSubscriptionOutcome::Subscribed {
                    subscription: Box::new(subscription),
                    fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
                })
            },
        );
    service_publisher_accessor.expect_get_message_provider().never();
    service_publisher_accessor.expect_register_availability_callback().never();

    let (manager, metrics_report_rx, _status_rx, _temp_dirs) = super::create_manager(vec![
        Arc::new(service_publisher_accessor),
        Arc::new(SlowAccessor {}),
    ])
    .await;

    let config = make_metrics_config();
    manager.add_metrics_config(config.write_to_bytes().unwrap().as_ref()).await.unwrap();
    manager.activate_metrics_config(CONFIG_UUID).await.unwrap();

    let mut metrics_reports = metrics_report_rx.take(2).collect::<Vec<_>>().await;
    metrics_reports.sort_by_key(|report| report.report_config_name.clone());

    manager.remove_all_metrics_configs().await.unwrap();
    drop(manager);

    expect_that!(
        metrics_reports,
        pointwise!(
            |report_config_name| pat!(MetricsReportProto {
                metrics_config_uuid: eq(CONFIG_UUID),
                report_config_name: eq(report_config_name),
                report_data: result_of_ref!(
                    |report_data: &MessageField<Any>| report_data
                        .to_owned()
                        .into_option()
                        .map(|report_data| report_data.unpack::<Int64Value>()),
                    some(ok(some(pat!(Int64Value { value: eq(&123) }))))
                ),
            }),
            ["aggregation_publisher_report", "service_name_report"]
        ),
    );
}
