<!-- Copyright 2025 Google LLC -->

# Manual Generation and Usage of a Protobuf Schema Library

## Objective

This document outlines the manual steps to generate a Java library containing an embedded `FileDescriptorProto` schema from a source `.proto` file. This process is a temporary, manual workflow that demonstrates how to create and use the schema library required by the IVI Telemetry Publisher SDK.

The goal is to produce a `java_library` that can be used as a dependency for any publisher app, allowing it to register with the telemetry service without needing the full protobuf runtime on the device. This manual process will be automated by a custom Soong module in a future change.

## Prerequisites

Before starting, ensure the `ProtoDescriptorGenerator` tool has been built successfully:
```shell
m sdv_telemetry_descriptor_generator
```

# Using the Descriptor Generator Directly

We can use the java descriptor directly and create a java library with the file descriptor embedded using the following steps:


## Generate the Java Schema File
This step runs the pre-built ProtoDescriptorGenerator tool. The command reads a source .proto file and outputs a new .java file containing the embedded schema information.

```shell
  sdv_telemetry_descriptor_generator
  --proto_file=<PATH_TO_HOME_DIR>/main-swcar-dev/system/software_defined_vehicle/telemetry/samples/rust/data_tunnel_publisher_error_report/error_report_publisher.proto \
  --protoc_tool=/usr/bin/protoc \
  --output_file=<PATH_TO_HOME_DIR>/main-swcar-dev/system/software_defined_vehicle/telemetry/samples/java/sample_ivi_sdk_publisher/com/google/android/sdv/telemetry/schemas/ErrorReportPublisherSchema.java \
  --output_package=com.google.android.sdv.telemetry.schemas \
  --output_class=ErrorReportPublisherSchema \
  --root_message_name=com.google.sdv.telemetry.samples.ErrorReportData
```

## Package the Generated File into a java_library
To make the generated source file usable by the Android build system, define a java_library module in an Android.bp file.

```android
// Compiles the generated ErrorReportPublisherSchema.java into a library.
java_library {
    name: "error_report_publisher_schema_lib",
    srcs: [Path to generated java file],
}
```

# Using the Descriptor Generator via gensrcs

Alternatively instead of calling the tool directly you can create gensrcs module and use that to create a java library as shown below:

```android
gensrcs {
    name: "error_report_publisher_schema_gen",
    tools: [
        "sdv_telemetry_descriptor_generator",
        "aprotoc",
    ],
    srcs: ["sample_protos/ErrorReportPublisher.proto"],
    output_extension: "java",
    cmd: "$(location sdv_telemetry_descriptor_generator) " +
        "--proto_file $(location sample_protos/ErrorReportPublisher.proto) " +
        "--protoc_tool $(location aprotoc) " +
        "--output_file $(out) " +
        "--output_package com.google.android.sdv.telemetry.schemas " +
        "--output_class ErrorReportPublisher " +
        "--root_message_name com.google.sdv.telemetry.samples.ErrorReportData",
}

java_library {
    name: "error_report_publisher_schema",
    srcs: [":error_report_publisher_schema_gen"],
    vendor: true,
    sdk_version: "system_current",
}
```


# Instructions to run this example

```bash
# Startup IVI SDV target
lunch sdv_ivi_cf-trunk_staging-userdebug
cvd start

# Change to root on device
adb root

# Start Telemtry Service on IVI
adb shell sdv_ivi_telemetry_service_agent
# Start Service that serves as Sample Java Publisher on IVI
adb shell am startservice -n google.sdv.telemetry.java.JavaSdkSampleIviPublisher/.JavaSdkSampleIviPublisher

# Push Configuration and run sample client
adb push samples/sample_protos/error_publisher_java_sdk.textproto /tmp/
adb shell telemetry_rust_c_client --write-reports-to=/tmp/ /tmp/error_publisher_java_sdk.textproto
```

## Helpful Logging

To analyze logs to evaluate adding, activation and report generation from metrics configuration, you can run in a separate terminal:

```bash
adb logcat | grep Telemetry
```

To see logs from the sample publisher, use:

```bash
adb logcat | grep JavaSdkSampleIviPublisher
```
