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

# **EXPERIMENTAL** Binder Accessor SDK

**WARNING**: This part of the SDK is currently considered experimental. We might
make **breaking changes** in the future. We are releasing it early to gather
feedback. Please report any bugs and suggestions to us.

This SDK provides a JNI library that allows access to Binder services from Java.
To use it, specify `libsdv_telemetry_sdk_binder_accessor_java` in your
`static_libs` list, and `libsdv_telemetry_sdk_binder_accessor_java_jni` in your
`jni_libs` list.
