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
installed:

`sudo apt install gqui`

The script needs some symlinks to work to find the right proto files:

`ln -s $T/external/protobuf/src/google ./google && ln -s $T/system ./system`

Without these symlinks in place, you will likely get these errors:

```text
Couldn't find file 'google/protobuf/descriptor.proto' (protodb fallback).
Couldn't load proto "proto/metrics_configuration.proto"
Could not detect type. Please provide a 'proto' statement.
```

## What it does

1.  Converts the `.textproto` files to binary `.pb` files using
    [gqui](https://g3doc.corp.google.com/util/tools/gqui/g3doc/overview.md) in a
    temporary folder
2.  Runs `cvd clear` and then `cvd start`
3.  Pushing all binary config files to the vm
4.  Starts all publishers listed in [tests/manual_testing/script/.env](.env)
5.  Starts telemetry client with the configs
6.  Starts `adb logcat` and directs output to file if specified in
    [tests/manual_testing/script/.env](.env)
7.  Waits for rust client to finish execution
8.  Pulls all created reports from the vm into a temporary folder
9.  Converts binary reports into human-readable `.textproto` files and writes
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

`PUBLISHERS` is a string with a space-separated list of rust binary names. The
script will run all publishers listed here. By default this contains all sample
publishers in [samples/rust](../../../samples/rust).

For publishers to work, their rust binary name must be listed in
[device/google/sdv/sdv_core_common/sdv_packages_telemetry.mk](../../../../../../device/google/sdv/sdv_core_common/sdv_packages_telemetry.mk).

### guest-enforce-security flag

Set `RUN_CVD_WITH_SECURITY_FLAG` to `true` to run `cvd start` with
`-guest-enforce-security false`
