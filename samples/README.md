<!-- Copyright 2024 Google LLC -->

# Sample Vendor Client Application

This directory contains vendor client application which communicates with the
telemetry service using ABI stable C APIs.

## Telemetry Service

The telemetry service is written using rust and uses tokio runtime to handle
asynchronous API calls. It implements the aidl-generated trait defined in
`ITelemetryService.aidl`. The service is registered as an async binder service
using the `sdv_telemetry_service_agent` executable.

Run the client application with path to the serialized config passed as a
command line argument.

## How to Test

Lunch Target:

For Core:

`lunch sdv_core_cf-trunk_staging-userdebug`

For IVI:

`lunch sdv_ivi_cf-trunk_staging-userdebug`

Start Guest:

For Core:

`cvd start`

For IVI:

```bash
cvd start -vsock_guest_cid=4 -base_instance_num=2 --extra_bootconfig_args="androidboot.sdv.instance_name=instance2 androidboot.virt.address=4"
```

Note: if testing on ivi, telemetry service needs to be started explicity. To do
that launch a new terminal, and start telemetry service using the following
steps:

```bash
adb root
adb shell sdv_ivi_telemetry_service_agent
```

1.  LAUNCH NEW TERMINAL (**Terminal #1**), switch to root, start logcat:

    ```bash
    # restart adbd with root permissions, if you haven't already
    adb root

    adb push config1.bin data/local/tmp/config1.bin
    .
    .
    adb push configN.bin data/local/tmp/configN.bin

    adb logcat | grep -i 'Telemetry'
    ```

    EXPECTED:

    ```
    10-26 23:32:49.092  1217  1217 I SdvTelemetryService: main: Creating telemetry service with 10MBs metric store size
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: active config telemetry directory created
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: inactive config telemetry directory created
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: Telemetry service initialized with 0 active configs and 0 inactive configs occupying 0 memory
    ```

    (**HINT:** you can press the ENTER key a couple of times on this terminal so
    you can see if new logs are printed after this one)

2.  LAUNCH NEW TERMINAL (**Terminal #2**)

    For rust client application:

    ```bash
    # restart adbd with root permissions, if you haven't already
    adb root
    adb shell TelemetryRustCClient <path_to_config1> ... <path_to_configN>
    ```

    The client applications will keep running until at least one report is
    consumed for each valid metric config.

    EXPECTED:

    In **Terminal #1**, the one with `logcat`, expect new logs (on top of what
    was already printed in step 2):

    ```
    10-26 23:33:32.166  1424  1424 I TelemetryRustCClient: TelemetryRustCClient: Connecting to service
    10-26 23:33:32.167  1217  1221 I SdvTelemetryService: sdv_telemetry: registerReportReadyListener
    10-26 23:33:32.167  1424  1424 I TelemetryRustCClient: TelemetryRustCClient: Status code: 208 status message: Status listener successfully added
    10-26 23:33:32.170  1217  1221 I SdvTelemetryService: sdv_telemetry: addMetricsConfig: MetricsConfig { uuid: "c6005cea-8fde-436a-9f29-c12f06a10c102504", version: 1, ... } of size: 701
    10-26 23:33:32.171  1217  1221 I SdvTelemetryService: sdv_telemetry: activateMetricsConfig: "c6005cea-8fde-436a-9f29-c12f06a10c102504"
    10-26 23:33:36.173  1424  1428 I TelemetryRustCClient: TelemetryRustCClient::metrics_report_manager: Report ready: c6005cea-8fde-436a-9f29-c12f06a10c102504 report number: 1
    10-26 23:33:36.174  1217  1221 I SdvTelemetryService: sdv_telemetry: getMetricsReport: c6005cea-8fde-436a-9f29-c12f06a10c102504
    10-26 23:33:36.174  1424  1427 I TelemetryRustCClient: TelemetryRustCClient::metrics_report_manager: Report received: c6005cea-8fde-436a-9f29-c12f06a10c102504
    10-26 23:33:36.174  1424  1424 I TelemetryRustCClient: TelemetryRustCClient: Report received: MetricsReport { report_number: 1, report_timestamp: MessageField(Some(Timestamp { seconds: 1698363216, nanos: 173714484, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), report_reason: REGULAR, metrics_config_uuid: "c6005cea-8fde-436a-9f29-c12f06a10c102504", metrics_config_version: 1, report_data: MessageField(Some(Any { type_url: "type.googleapis.com/android.automotive.telemetry.messages.VehicleConnectivityReport", value: [8, 0], special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }
    10-26 23:33:36.174  1217  1221 I SdvTelemetryService: sdv_telemetry: removeMetricsConfig: c6005cea-8fde-436a-9f29-c12f06a10c102504
    10-26 23:33:36.175  1424  1424 I TelemetryRustCClient: TelemetryRustCClient: successfully removed config
    ```

Check logs with `adb logcat`

## Configurable Publishers

Start Guest:

`cvd start`

Steps to reproduce:

1.  LAUNCH NEW TERMINAL (**Terminal #1**), switch to root, start logcat:

    ```bash
    adb root

    adb push config1.bin data/local/tmp/config1.bin
    .
    .
    adb push configN.bin data/local/tmp/configN.bin

    adb logcat | grep -i 'Telemetry'
    ```

    EXPECTED:

    ```
    10-26 23:32:49.092  1217  1217 I SdvTelemetryService: main: Creating telemetry service with 10MBs metric store size
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: active config telemetry directory created
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: inactive config telemetry directory created
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: Telemetry service initialized with 0 active configs and 0 inactive configs occupying 0 memory
    ```

    (**HINT:** you can press the ENTER key a couple of times on this terminal so
    you can see if new logs are printed after this one)

2.  LAUNCH NEW TERMINAL (**Terminal #2**)

    ```bash
    # restart adbd with root permissions, if you haven't already
    adb root
    adb shell telemetry_sample_publisher
    ```

    In **Terminal #1**, the one with `logcat`, expect new logs (on top of what
    was already printed in step 2):

    ```
    11-20 22:29:05.206   237   243 I SdvTelemetryService: sdv_telemetry_configurable_publisher: Registering publisher with telemetry service name SampleService
    ```

3.  LAUNCH NEW TERMINAL (**Terminal #3**)

    For rust client application:

    Make sure at least one of the configs uses the sample publisher registered
    in step 3.

    ```bash
    # restart adbd with root permissions, if you haven't already
    adb root
    adb shell TelemetryRustCClient <path_to_config1> ... <path_to_configN>
    ```

    The client applications will keep running until at least one report is
    consumed for each valid metric config.

    EXPECTED:

    In **Terminal #1**, the one with `logcat`, expect new logs (on top of what
    was already printed in step 2):

    ```
    11-20 22:35:47.993   627   627 I TelemetryRustCClient: TelemetryRustCClient: Connecting to service
    11-20 22:35:47.994   237   243 I SdvTelemetryService: sdv_telemetry: registerReportReadyListener
    11-20 22:35:47.994   627   627 I TelemetryRustCClient: TelemetryRustCClient: Status code: 208 status message: Status listener successfully added
    11-20 22:35:47.994   237   243 I SdvTelemetryService: sdv_telemetry: addMetricsConfig: MetricsConfig { uuid: "c6005cea-8fde-436a-9f29-c12f06a10c102504", version: 1, ... } of size: 775
    11-20 22:35:47.994   237   243 I SdvTelemetryService: sdv_telemetry: activateMetricsConfig: "c6005cea-8fde-436a-9f29-c12f06a10c102504"
    11-20 22:35:47.995   627   627 I TelemetryRustCClient: TelemetryRustCClient: Config added successfully: c6005cea-8fde-436a-9f29-c12f06a10c102504
    11-20 22:35:51.997   627   631 I TelemetryRustCClient: TelemetryRustCClient::metrics_report_manager: Report ready: c6005cea-8fde-436a-9f29-c12f06a10c102504 report number: 1
    11-20 22:35:51.997   237   243 I SdvTelemetryService: sdv_telemetry: getMetricsReport: c6005cea-8fde-436a-9f29-c12f06a10c102504
    11-20 22:35:51.997   627   630 I TelemetryRustCClient: TelemetryRustCClient::metrics_report_manager: Report received: c6005cea-8fde-436a-9f29-c12f06a10c102504
    11-20 22:35:51.997   627   627 I TelemetryRustCClient: TelemetryRustCClient: Report received: MetricsReport { report_number: 1, report_timestamp: MessageField(Some(Timestamp { seconds: 1700519751, nanos: 997051620, special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), report_reason: REGULAR, metrics_config_uuid: "c6005cea-8fde-436a-9f29-c12f06a10c102504", metrics_config_version: 1, report_data: MessageField(Some(Any { type_url: "type.googleapis.com/android.automotive.telemetry.messages.VehicleConnectivityReport", value: [8, 1], special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } })), special_fields: SpecialFields { unknown_fields: UnknownFields { fields: None }, cached_size: CachedSize { size: 0 } } }
    ```

The configurable publisher executable needs to be terminated manually using
`CTRL + C`.

## Data Tunnel

Start Guest:

`cvd start`

Steps to reproduce:

1.  LAUNCH NEW TERMINAL (**Terminal #1**), switch to root, start logcat:

    ```bash
    adb root

    adb push data_tunnel_config.bin data/local/tmp/

    adb logcat | grep -i 'Telemetry'
    ```

    EXPECTED:

    ```

    10-26 23:32:49.092  1217  1217 I SdvTelemetryService: main: Creating telemetry service with 10MBs metric store size
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: active config telemetry directory created
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: inactive config telemetry directory created
    10-26 23:32:49.093  1217  1217 I SdvTelemetryService: sdv_telemetry: Telemetry service initialized with 0 active configs and 0 inactive configs occupying 0 memory
    ```

    (**HINT:** you can press the ENTER key a couple of times on this terminal so
    you can see if new logs are printed after this one)

2.  LAUNCH NEW TERMINAL (**Terminal #2**)

    ```bash
    # restart adbd with root permissions, if you haven't already
    adb root
    adb shell dt_telemetry_sample_publisher
    ```

    EXPECTED:

    ```
    Message written to DT sample_data: 423908830 of size: 7
    Message written to DT sample_data: -18516534 of size: 12
    Message written to DT sample_data: -237141808 of size: 12
    Message written to DT sample_data: -7741888 of size: 12
    Message written to DT sample_data: 1237407559 of size: 7
    Message written to DT sample_data: -193225183 of size: 12
    ```

    In **Terminal #1**, the one with `logcat`, expect new logs (on top of what
    was already printed in step 2):

    ```
    01-25 18:47:16.110   223   223 D libsdv_dt: RegisterTopic(name=DataTunnelSampleService, properties = { message_size=2, message_count=1024 })
    ```

3.  LAUNCH NEW TERMINAL (**Terminal #2**)

    For rust client application:

    Make sure the config uses the data tunnel topic registered in step 2.

    ```bash
    # restart adbd with root permissions, if you haven't already
    adb root
    adb shell TelemetryRustCClient <path_to_data_tunnel_config>
    ```

    The client applications will keep running until at least one report is
    consumed for each valid metric config.

    EXPECTED:

    In **Terminal #1**, the one with `logcat`, expect new logs (on top of what
    was already printed in step 2):

    ```
    01-25 18:47:29.422   232   232 I TelemetryRustCClient: TelemetryRustCClient: Connecting to service
    01-25 18:47:29.423   232   232 I StatusListenerImpl: Created
    01-25 18:47:29.423   232   236 D StatusListenerImpl: sending status back to client. API call id: 0, Status: 208, Message: Status listener successfully added
    01-25 18:47:29.423   135   162 I SdvTelemetryService: sdv_telemetry: registerReportReadyListener
    01-25 18:47:29.423   232   236 D StatusListenerImpl: sending status back to client. API call id: 0, Status: 206, Message: Report Ready Listener successfully added
    01-25 18:47:29.423   232   232 I ServiceClient: TelemetryService Created
    01-25 18:47:29.423   232   232 I StatusListenerImpl: set_status_listener
    01-25 18:47:29.423   232   232 D StatusListenerImpl: sending status back to client. API call id: 0, Status: 208, Message: Status listener successfully added
    01-25 18:47:29.423   232   232 I TelemetryRustCClient: TelemetryRustCClient: Status code: 208 status message: Status listener successfully added
    01-25 18:47:29.423   232   232 I ServiceClient: set status callback from CPP
    01-25 18:47:29.423   232   232 I ServiceClient: registerReportReadyListener from CPP
    01-25 18:47:29.424   232   232 I ServiceClient: addMetricsConfig in CPP
    01-25 18:47:29.425   135   162 I SdvTelemetryService: sdv_telemetry: addMetricsConfig: MetricsConfig { uuid: "c6115cea-8fde-436a-9f29-c12f06a10c102504", version: 1, ... } of size: 780
    01-25 18:47:29.429   232   236 D StatusListenerImpl: sending status back to client. API call id: 2, Status: 200, Message: c6115cea-8fde-436a-9f29-c12f06a10c102504
    01-25 18:47:29.429   232   232 I ServiceClient: activate metrics config inside CPP
    01-25 18:47:29.429   135   162 I SdvTelemetryService: sdv_telemetry: activateMetricsConfig: "c6115cea-8fde-436a-9f29-c12f06a10c102504"
    01-25 18:47:29.429   135   162 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Subscribing to data tunnel topic DataTunnelSampleService
    01-25 18:47:29.429   135   162 D libsdv_dt: SubscribeToTopic(topic_name=DataTunnelSampleService)
    01-25 18:47:29.426   135   135 I binder:135_1: type=1400 audit(0.0:23): avc:  denied  { call } for  scontext=u:r:sdv_telemetry:s0 tcontext=u:r:sdv_dt_agent:s0 tclass=binder permissive=1
    01-25 18:47:29.426   135   135 W binder:135_1: type=1300 audit(0.0:23): arch=c000003e syscall=16 success=yes exit=0 a0=8 a1=c0306201 a2=7dd3ff2eca60 a3=8668f79994459dac items=0 ppid=1 auid=4294967295 uid=1000 gid=1000 euid=1000 suid=1000 fsuid=1000 egid=1000 sgid=1000 fsgid=1000 tty=(none) ses=4294967295 exe="/system_ext/bin/sdv_telemetry_service_agent" subj=u:r:sdv_telemetry:s0 key=(null)
    01-25 18:47:29.431   135   162 I SdvTelemetryService: sdv_telemetry: initializing c6115cea-8fde-436a-9f29-c12f06a10c102504 Config
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Received 5 messages
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Dimensions of message array: 100 2
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Length of message chunk: 2
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Message= sample_data: 95
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Length of message chunk: 2
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Message= sample_data: 83
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Length of message chunk: 2
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Message= sample_data: 40
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Length of message chunk: 2
    01-25 18:47:29.431   135   159 I SdvTelemetryService: sdv_telemetry_dt_wrapper: Message= sample_data: 11
    ```

The data tunnel publisher needs to be terminated manually using `CTRL + C`.
