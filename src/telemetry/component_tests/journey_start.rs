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
use mockall::predicate as m;
use protobuf::well_known_types::wrappers::BoolValue;
use protobuf::{text_format, Message, MessageFull};
use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig as MetricsConfigProto;
use sdv_telemetry_service_publisher_accessor::{
    CreateSubscriptionOutcome, FetchLastMessageOutcome, MockServicePublisherAccessor,
    MockServicePublisherSubscription, ServicePublisherMetadata,
};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use crate::metrics_config_manager::MetricsConfigManager;
use crate::metrics_config_uuid::MetricsConfigUuid;

static CONFIG_UUID: LazyLock<MetricsConfigUuid> =
    LazyLock::new(|| MetricsConfigUuid::from_str("36a06787-58cb-4d06-8fc8-b7d07ca83059").unwrap());

const SOURCE_IDENTIFIER: &str = "journey_status_service";

// The metrics configuration in this test has start and stop triggers reacting to some data source
// publishing `true` and `false` values corresponding to updates on journey status. Data collection
// should start when `true` is received for the first time, and stop when `false` is received for
// the first time after a `true`, i.e. data collection starts on `false -> true` transition, and
// stops on `true -> false` transition. A metrics report is generated at the end of each data
// collection interval.
fn create_metrics_config() -> MetricsConfigProto {
    text_format::parse_from_str::<MetricsConfigProto>(&format!(
        r#"
uuid: "{config_uuid}"
version: 3758096386
descriptor_protos {{
  name: "adhoc.proto"
  package: "aaos.sdv.telemetry.adhoc"
  message_type {{
    name: "counter"
    field {{
      name: "count"
      number: 1
      type: TYPE_INT32
    }}
  }}
  message_type {{
    name: "previous"
    field {{
      name: "value"
      number: 1
      type: TYPE_BOOL
    }}
  }}
  message_type {{
    name: "report"
    field {{
      name: "value"
      number: 1
      type: TYPE_BOOL
    }}
  }}
  syntax: "proto2"
}}
triggers {{
  name: "counter_trigger"
  data_trigger {{
    source_name: "counter"
  }}
}}
triggers {{
  name: "data_trigger"
  data_trigger {{
    source_name: "source"
  }}
}}
triggers {{
  name: "falling_trigger"
  conditional_trigger {{
    trigger_names: "counter_trigger"
    selector_node_index: 14
    is_true {{}}
  }}
}}
triggers {{
  name: "rising_trigger"
  conditional_trigger {{
    trigger_names: "counter_trigger"
    selector_node_index: 10
    is_true {{}}
  }}
}}
sources {{
  name: "counter"
  aggregator {{
    trigger_names: "data_trigger"
    message_builder {{
      message_type: ".aaos.sdv.telemetry.adhoc.counter"
      field_assignments {{
        field_name: "count"
        count_aggregation {{}}
      }}
    }}
  }}
}}
sources {{
  name: "source"
  data_source {{
    source_identifier: "{SOURCE_IDENTIFIER}"
    connection_type: SUBSCRIPTION
  }}
}}
sources {{
  name: "previous"
  aggregator {{
    trigger_names: "rising_trigger"
    trigger_names: "falling_trigger"
    message_builder {{
      message_type: ".aaos.sdv.telemetry.adhoc.previous"
      field_assignments {{
        field_name: "value"
        no_aggregation {{
          expression_node_index: 0
        }}
      }}
    }}
  }}
}}
# Expression Node 0
expression_nodes {{
  field_leaf_node {{
    source_name: "source"
    field_names: "value"
  }}
}}
# Expression Node 1
expression_nodes {{
  constant_leaf_node {{
    bool_value: true
  }}
}}
# Expression Node 2, "source.value == true"
expression_nodes {{
  combination_node {{
    left_index: 0
    right_index: 1
    relational_operator: EQ
  }}
}}
# Expression Node 3
expression_nodes {{
  field_leaf_node {{
    source_name: "counter"
    field_names: "count"
  }}
}}
# Expression Node 4
expression_nodes {{
  constant_leaf_node {{
    int32_value: 1
  }}
}}
# Expression Node 5, "counter.count == 1"
expression_nodes {{
  combination_node {{
    left_index: 3
    right_index: 4
    relational_operator: EQ
  }}
}}
# Expression Node 6
expression_nodes {{
  field_leaf_node {{
    source_name: "previous"
    field_names: "value"
  }}
}}
# Expression Node 7
expression_nodes {{
  constant_leaf_node {{
    bool_value: false
  }}
}}
# Expression Node 8, "previous.value == false"
expression_nodes {{
  combination_node {{
    left_index: 6
    right_index: 7
    relational_operator: EQ
  }}
}}
# Expression Node 9, "counter.count == 1 || previous.value == false"
expression_nodes {{
  combination_node {{
    left_index: 5
    right_index: 8
    logical_operator: OR
  }}
}}
# Expression Node 10, "source.value == true && (counter.count == 1 || previous.value == false)"
expression_nodes {{
  combination_node {{
    left_index: 2
    right_index: 9
    logical_operator: AND
  }}
}}
# Expression Node 11, "source.value == false"
expression_nodes {{
  combination_node {{
    left_index: 0
    right_index: 7
    relational_operator: EQ
  }}
}}
# Expression Node 12, "previous.value == true"
expression_nodes {{
  combination_node {{
    left_index: 6
    right_index: 1
    relational_operator: EQ
  }}
}}
# Expression Node 13, "counter.count == 1 || previous.value == true"
expression_nodes {{
  combination_node {{
    left_index: 5
    right_index: 12
    logical_operator: OR
  }}
}}
# Expression Node 14, "source.value == false && (counter.count == 1 || previous.value == true)"
expression_nodes {{
  combination_node {{
    left_index: 11
    right_index: 13
    logical_operator: AND
  }}
}}
metrics_report_configs {{
  name: "report"
  trigger_names: "falling_trigger"
  message_builder {{
    message_type: ".aaos.sdv.telemetry.adhoc.report"
    field_assignments {{
      field_name: "value"
      no_aggregation {{
        expression_node_index: 0
      }}
    }}
  }}
}}
start_trigger_name: "rising_trigger"
stop_trigger_name: "falling_trigger"
    "#,
        config_uuid = *CONFIG_UUID
    ))
    .unwrap()
}

#[googletest::test]
#[sdv_telemetry_test_utils::timeout]
#[tokio::test]
async fn test_journey_start() {
    let config = create_metrics_config();
    let mut service_publisher_accessor = MockServicePublisherAccessor::new();

    service_publisher_accessor.expect_get_metadata().with(m::eq(SOURCE_IDENTIFIER)).returning(
        |_service_name| {
            Ok(Some(ServicePublisherMetadata { message_descriptor: BoolValue::descriptor() }))
        },
    );

    let (tx, mut rx) = mpsc::unbounded_channel();
    service_publisher_accessor
        .expect_create_subscription()
        .once()
        .withf(|name, _, _, _, _, _| name == SOURCE_IDENTIFIER)
        .return_once(move |_, _, _, _, mut new_message_callback, _| {
            tokio::spawn(async move {
                while let Some(value) = rx.recv().await {
                    new_message_callback(Ok((
                        Box::new(BoolValue { value, ..Default::default() }),
                        None,
                    )));
                }
            });
            Ok(CreateSubscriptionOutcome::Subscribed {
                subscription: Box::new(MockServicePublisherSubscription::new()),
                fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
            })
        });

    let (manager, metrics_report_rx, _status_rx, _temp_dirs) =
        super::create_manager(vec![Arc::new(service_publisher_accessor)]).await;

    manager.add_metrics_config(config.write_to_bytes().unwrap().as_ref()).await.unwrap();
    manager.activate_metrics_config(&config.uuid).await.unwrap();

    for val in [false, true, false, true, false, false, true, true, false] {
        tx.send(val).unwrap();
        sleep(Duration::from_millis(50)).await;
    }

    // Data collection starts on `false -> true` transition, and stops on `true -> false`
    // transition. Metrics reports are generated at the end of each data collection interval, so
    // there expected to be exactly 3 metrics reports.
    let report_count = metrics_report_rx.take(3).collect::<Vec<_>>().await.len();
    assert_eq!(report_count, 3, "Expected 3 metrics reports");
}
