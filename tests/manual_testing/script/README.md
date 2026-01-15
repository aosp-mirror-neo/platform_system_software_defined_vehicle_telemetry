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

# Manual testing helper script

[run_configs.sh](run_configs.sh) is a helper script to make running many configs
at the same time easier. **Run at your own risk.**

## Usage

`$ bash tests/manual_testing/script/run_configs.sh [textproto-config-path1
textproto-config-path2]`

The script can be run with one or more paths to Metric Configs in textproto
files or without a command line argument to run all configs in
[tests/manual_testing/configs](../configs). Symlinks in this folder work too.

### Prerequisites

You need to have
[gqui](https://g3doc.corp.google.com/util/tools/gqui/g3doc/overview.md)
installed (go/gqui#installation):

```bash
sudo glinux-add-repo gqui
sudo apt update
sudo apt install gqui
```

## What it does

1. Converts the `.textproto` files to binary `.pb` files using
    [gqui](https://g3doc.corp.google.com/util/tools/gqui/g3doc/overview.md) in a
    temporary folder
2. Runs `cvd clear` and then `cvd create`
3. Pushing all binary config files to the vm
4. Starts all publishers listed in [tests/manual_testing/script/.env](.env)
5. Starts telemetry client with the configs
6. Starts `adb logcat` and directs output to file if specified in
    [tests/manual_testing/script/.env](.env)
7. Waits for rust client to finish execution
8. Pulls all created reports from the vm into a temporary folder
9. Converts binary reports into human-readable `.textproto` files and writes
    them to a folder reports/ in the set output directory
10. Stops publishers, stops vm, deletes temporary folders

## Settings

There are some 'settings' that can be changed by making changes to the
[.env](.env) file.

### Output directory

The output directory for the returned reports and log file is set by `OUT_DIR`
(`out` by default).

### Logs

You can choose to write logs to the command line or to a file by setting
`LOG_TO_FILE` (`true` by default). The log file gets written as
`{timestamp}.log` in the set output directory.

### Publishers

`IVI_PUBLISHERS` and `CORE_PUBLISHERS` are strings with a space-separated list
of rust binary names. The script will run all publishers `IVI_PUBLISHERS` when
you `lunch`ed an IVI target, and all `CORE_PUBLISHERS` when you `lunch`ed a CORE
target. By default, these contain all applicable sample publishers in
[samples/rust](../../../samples/rust).

For publishers to work, their rust binary name must be listed in
[device/google/sdv/sdv_core_common/sdv_packages_telemetry.mk](../../../../../../device/google/sdv/sdv_core_common/sdv_packages_telemetry.mk).

TODO(b/318739373): This might change after b/318739373 is fixed.
