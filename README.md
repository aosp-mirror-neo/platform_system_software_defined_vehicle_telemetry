# Telemetry Services

This repository contains the telemetry specific services for the software defined vehicle.

See [go/sdv-telemetry](https://go/sdv-telemetry) for general information about SDV.

## Build Instructions

Run the following commands

`$ UNSAFE_DISABLE_HIDDENAPI_FLAGS=true m -j`

Run the client application with path to the serialzed config passed as a command line argument.

## How to Test:

Start Guest:

`$ launch_cvd -guest-enforce-security false`

Steps to reproduce:
1. LAUNCH NEW TERMINAL (**Terminal #1**), switch to root, start logcat:

    ```
    adb root

    adb push config.bin data/local/tmp/config.bin

    adb logcat | grep -E 'telemetry|telemetry_rust_c_client'
    ```

EXPECTED:

**EXPECT THAT NOTHING IS PRINTED YET**

2. LAUNCH NEW TERMINAL (**Terminal #2**)

```
adb shell sdv_telemetry_service
```

EXPECTED (on this one, **Terminal #2**):

```
Adding android.sdv.telemetry.ITelemetryService service to binder
```

EXPECTED (on **Terminal #1**, the one with `logcat`):

```
10-26 23:32:49.092  1217  1217 I SdvTelemetryService: main: Creating telemetry service with 10MBs metric store size
10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: active config telemetry directory created
10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: inactive config telemetry directory created
10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: Telemetry service initialized with 0 active configs and 0 inactive configs occupying 0 memory
```

(**HINT:** you can press the ENTER key a couple of times on this terminal so you can see if new logs are printed after this one)

3. LAUNCH NEW TERMINAL (**Terminal #3**)

For rust client application:
```
adb shell telemetry_rust_c_client data/local/tmp/config.bin
```

For cpp client application:

```
adb shell telemetry_cpp_client data/local/tmp/config.bin
```

EXPECTED:

In **Terminal #1**, the one with `logcat`, expect new logs (on top of what was already printed in step 2):

Note: The logs shown here are for the rust client application. Similar logs can be expected for the cpp client.

```
10-26 23:33:32.166  1424  1424 I telemetry_rust_c_client: telemetry_rust_c_client: Connecting to service
10-26 23:33:32.167  1217  1221 I SdvTelemetryService: sdv_telemetry: setReportReadyListener
10-26 23:33:32.167  1424  1424 I telemetry_rust_c_client: telemetry_rust_c_client: Status code: 208 status message: Status listener successfully added
10-26 23:33:32.170  1217  1221 I SdvTelemetryService: sdv_telemetry: addMetricsConfig: MetricsConfig { uuid: "c6005cea-8fde-436a-9f29-c12f06a10c102504", version: 1, descriptor_protos: [FileDescriptorProto { name: Some("metrics.proto"), package: Some("android.automotive.telemetry.messages"), dependency: [], public_dependency: [], weak_dependency: [], message_type: [DescriptorProto { name: Some("VehicleConnectivity"), field: [FieldDescriptorProto { name: Some("vector_vehicle_connection_status"), number: Some(1), label: Some(LABEL_OPTIONAL), type_: Some(TYPE_INT32), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, FieldDescriptorProto { name: Some("vector_vehicle_connection_signal_strength"), number: Some(2), label: Some(LABEL_REPEATED), type_: Some(TYPE_FLOAT), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, FieldDescriptorProto { name: Some("vector_connection_status_timestamps"), number: Some(3), label: Some(LABEL_REPEATED), type_: Some(TYPE_UINT64), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], extension: [], nested_type: [], enum_type: [], extension_range: [], oneof_decl: [], options: MessageField(None), reserved_range: [], reserved_name: [], special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, DescriptorProto { name: Some("VehicleConnectivityReport"), field: [FieldDescriptorProto { name: Some("vector_vehicle_connection_status"), number: Some(1), label: Some(LABEL_OPTIONAL), type_: Some(TYPE_INT32), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, FieldDescriptorProto { name: Some("vector_vehicle_connection_signal_strength"), number: Some(2), label: Some(LABEL_REPEATED), type_: Some(TYPE_FLOAT), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, FieldDescriptorProto { name: Some("max_speed"), number: Some(3), label: Some(LABEL_OPTIONAL), type_: Some(TYPE_FLOAT), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, FieldDescriptorProto { name: Some("count_airbag_isDeployed"), number: Some(4), label: Some(LABEL_OPTIONAL), type_: Some(TYPE_UINT32), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, FieldDescriptorProto { name: Some("latest_hmi_car_settings_clicks"), number: Some(5), label: Some(LABEL_OPTIONAL), type_: Some(TYPE_UINT32), type_name: None, extendee: None, default_value: None, oneof_index: None, json_name: None, options: MessageField(None), proto3_optional: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], extension: [], nested_type: [], enum_type: [], extensi
10-26 23:33:32.170  1217  1221 I SdvTelemetryService: on_range: [], oneof_decl: [], options: MessageField(None), reserved_range: [], reserved_name: [], special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], enum_type: [], service: [], extension: [], options: MessageField(None), source_code_info: MessageField(None), syntax: None, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], script: "uid494_metricsconfig_2.lua", triggers: [Trigger { name: "start_trigger", trigger_type: Some(PeriodicTrigger(PeriodicTrigger { interval_ms: 2000, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }, Trigger { name: "period_2s", trigger_type: Some(PeriodicTrigger(PeriodicTrigger { interval_ms: 2000, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], publishers: [], expression_nodes: [Node { node_type: Some(ConstantLeafNode(ConstantLeafNode { node_value: Some(Int32Value(0)), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], metrics_report_configs: [MetricsReportConfig { name: "SampleReport", trigger_names: ["period_2s"], report_incomplete: false, message_builder: MessageField(Some(ProtoMessageBuilder { message_type: ".android.automotive.telemetry.messages.VehicleConnectivityReport", field_assignments: [FieldAssignment { field_name: "vector_vehicle_connection_status", expression_node_index: 0, aggregation_type: NO_AGGREGATION, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }], start_trigger_name: "start_trigger", end_trigger_name: "", finish_trigger_name: "", special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } } of size: 701
10-26 23:33:32.171  1217  1221 I SdvTelemetryService: sdv_telemetry: activateMetricsConfig: "c6005cea-8fde-436a-9f29-c12f06a10c102504"
10-26 23:33:36.173  1424  1428 I telemetry_rust_c_client: telemetry_rust_c_client::metrics_report_manager: Report ready: c6005cea-8fde-436a-9f29-c12f06a10c102504 report number: 1
10-26 23:33:36.174  1217  1221 I SdvTelemetryService: sdv_telemetry: getMetricsReport: c6005cea-8fde-436a-9f29-c12f06a10c102504
10-26 23:33:36.174  1424  1427 I telemetry_rust_c_client: telemetry_rust_c_client::metrics_report_manager: Report received: c6005cea-8fde-436a-9f29-c12f06a10c102504
10-26 23:33:36.174  1424  1424 I telemetry_rust_c_client: telemetry_rust_c_client: Report received: MetricsReport { report_number: 1, report_timestamp: MessageField(Some(Timestamp { seconds: 1698363216, nanos: 173714484, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), report_reason: REGULAR, metrics_config_uuid: "c6005cea-8fde-436a-9f29-c12f06a10c102504", metrics_config_version: 1, report_data: MessageField(Some(Any { type_url: "type.googleapis.com/android.automotive.telemetry.messages.VehicleConnectivityReport", value: [8, 0], special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }
10-26 23:33:36.174  1217  1221 I SdvTelemetryService: sdv_telemetry: removeMetricsConfig: c6005cea-8fde-436a-9f29-c12f06a10c102504
10-26 23:33:36.175  1424  1424 I telemetry_rust_c_client: telemetry_rust_c_client: Successfully removed config
```

Check logs with `adb logcat`

## Unit Test Instructions

`UNSAFE_DISABLE_HIDDENAPI_FLAGS=true atest libsdv_telemetry_unit_tests`
