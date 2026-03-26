<!--
  Copyright (C) 2026 The Android Open Source Project

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# **EXPERIMENTAL** Protobuf Descriptor Generator for Java

**WARNING**: This part of the SDK is currently considered experimental. We might
make **breaking changes** in the future. We are releasing it early to gather
feedback. Please report any bugs and suggestions to us.

This directory contains the
`sdv_telemetry_sdk_descriptor_generator_java` tool, which generates
Java code containing a serialized `FileDescriptorSet` protobuf message from a
source `.proto` file.

The generated `.java` file can be compiled into a `java_library`. This library
can then be used as a dependency for a publisher using the Configurable
Publisher Registry, without needing the full protobuf runtime on the device.

## Command Line Arguments

The `sdv_telemetry_sdk_descriptor_generator_java` binary accepts the
following arguments:

*  `--proto_file`: (Required) Path to the input `.proto` file.
*  `--proto_path`: (Required) Specify the directory in which to search for
   imports. May be specified multiple times; directories will be searched in
   order. The file provided via `--proto_file` must also be in a directory
   provided via `--proto_path`.
*  `--message_full_name`: (Required) Fully qualified name of the protobuf
   message defined in the `.proto` file.
*  `--output_file`: (Required) Path where the generated `.java` file will be
   written.
*  `--output_package`: (Required) Java package name for the generated class.
*  `--output_class`: (Required) Name of the generated Java class.
*  `--fatal_warnings`: (Optional) Treat warnings as errors. Defaults to `true`.
   Set to `false` to disable.

## Integration in Android Builds

The recommended way to use the descriptor generator is by defining a `genrule`
module in an `Android.bp` file. This automates the code generation and
seamlessly integrates it with the Android build system. The `genrule` gives you
explicit control over the exact input and output paths for the code generation
step.

```android
genrule {
    name: "example_descriptors_gen",
    tools: ["sdv_telemetry_sdk_descriptor_generator_java"],
    srcs: ["Example.proto"],
    out: ["Example.java"],
    cmd: "$(location sdv_telemetry_sdk_descriptor_generator_java) " +
        "--proto_file $(location Example.proto) " +
        "--proto_path $$(dirname $(location Example.proto)) " +
        "--message_full_name com.example.ExampleMessage " +
        "--output_file $(out) " +
        "--output_package com.example " +
        "--output_class Example "
}

java_library {
    name: "example_descriptors_lib",
    srcs: [":example_descriptors_gen"],
}
```
