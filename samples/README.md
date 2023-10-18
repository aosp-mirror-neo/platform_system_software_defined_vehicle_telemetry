# Sample Vendor Client Application

This directory contains vendor client application which communicates with the
telemetry service using ABI stable C APIs.

## Telemetry Service

The telemetry service is written using rust and uses tokio runtime to handle
asynchrnous API calls. It implements the aidl-generated trait defined in
ITelemetryAService.aidl. The service is registered as an async binder service
using the sdv_telemetry_service executable.

## Testing

0. Build

    ```bash
    cd path/to/android-repo
    source build/envsetup.sh
    lunch sdv_core_cf-userdebug
    m
    ```

1. Launch SDV device

    ```bash
    launch_cvd
    # Expect tons of logs finishing by "Virtual device booted successfully"
    ```
2. Start a new terminal, switch to root, push the metric config binary file and start logcat

    ```bash
    adb root
    # Expected "restarting adbd as root"
    # Expect the root terminal "#"
    adb push <path_to_file> <path_on_device>
    adb logcat
    ```

3. Start a new terminal, and start telemetry service

    ```bash
    adb root
    # Expected "restarting adbd as root"
    adb shell
    # Expect the root terminal "#"
    sdv_telemetry_service
    # Expected "Adding android.sdv.telemetry.ITelemetryService service to
    # binder"
    ```

    In case service crashes with the error `Aborted` run the following command and try again.
    ```bash
    setprop ro.telemetry.metric_config_store_in_mb 10
    getprop ro.telemetry.metric_config_store_in_mb
    # Expected "10"
    ```

4. Run client application

    1. Start a new terminal and run cpp client

        ```bash
        adb shell
        # Expect the root terminal "#"
        telemetry_cpp_client <path_on_device>
        # Expect no output
        # In the previous terminal with logcat expect logs with logtag
        # "CPPClient" and "SdvTelemetryService"
        ```

    2. Start a new terminal and run rust client application

        ```bash
        adb shell
        # Expect the root terminal "#"
        telemetry_rust_c_client <path_on_device>
        # Expect "Getting service android.sdv.telemetry.ITelemetryService"
        # In the previous terminal with logcat expect logs with logtag
        # "telemetry_rust_c_client" and "SdvTelemetryService"
        ```
