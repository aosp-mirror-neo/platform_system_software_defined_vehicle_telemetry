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

use log::{error, info};
use protobuf::Message;
use sdv_telemetry_sdk_client_internal as rust_to_c;
use std::ffi::{c_char, c_void, CStr};
use std::slice;
use tokio::sync::oneshot;

use crate::{
    MetricsReport, ReportInfo, TelemetryError, TelemetryResult, TelemetryServiceStatusCodes,
};

type OnStatusReceivedResult = TelemetryResult<String>;
type OnConfigListReceivedResult = TelemetryResult<Vec<String>>;
type OnReportReceivedResult = TelemetryResult<MetricsReport>;
type OnReadyReportsInfoReceivedResult = TelemetryResult<Vec<ReportInfo>>;

/// Wrapper around `oneshot::Sender` that never errors, even if the receiver was dropped.
struct Sender<T>(oneshot::Sender<T>);

impl<T> Sender<T> {
    fn send(self, value: T) {
        // Sending may fail if the receiver has been dropped (e.g., when the user of this library
        // drops the future). Thus, we ignore that error here.
        let _ = self.0.send(value);
    }
}

/// This function abstracts the complexity of calling an async C API.
///
/// * `call_api` is a callback that must do the actual call to the C API. It receives user data and
///   callback to pass on to the API.
/// * `callback` is the callback on the Rust side that should be run when the API call succeeds.
async fn call_async_c_api<R, C, F>(call_api: F, callback: C) -> TelemetryResult<R>
where
    F: FnOnce(*mut c_void, C) -> i32,
{
    let (result_tx, result_rx) = oneshot::channel();
    let result_tx = Box::into_raw(Box::new(Sender(result_tx)));

    let status_code = call_api(result_tx as *mut c_void, callback);
    let status_code = TelemetryServiceStatusCodes(status_code);
    if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        // Release memory in case of API call failure.
        //
        // SAFETY: Safe to call since `result_tx` was created using `Box::into_raw` and is
        // guaranteed to not have been consumed due to the API call failure.
        let _ = unsafe { Box::from_raw(result_tx) };

        // At this point, waiting for `result_rx` is pointless (and would panic!), because
        // `result_tx` has been dropped.
        return Err(TelemetryError { status: status_code, message: "API call failed".to_owned() });
    }

    result_rx.await.expect(
        "A result should always be received, because the sender is only dropped after it has sent a result.",
    )
}

/// Callback that receives status for API calls.
///
/// # Safety
///
/// * `sender` must point to a valid `Sender` originally sent to the caller during the
///   `call_api_with_status_callback` API call. It will not be valid after the function completes.
/// * `status_msg` must point to a C raw string that satisfies `CStr::from_ptr` safety requirements.
unsafe extern "C" fn on_status_received(
    sender: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    let status_code = TelemetryServiceStatusCodes(status_code);
    // SAFETY: safe to use as the sender passed by the caller was originally allocated using a
    // global allocator using Box::new().
    let sender = unsafe { Box::from_raw(sender as *mut Sender<OnStatusReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let status_message = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    let result = if status_code == TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(status_message)
    } else {
        Err(TelemetryError { status: status_code, message: status_message })
    };
    sender.send(result);
}

/// Safely call a C API that returns data through a `StatusCallbackFunction` callback. This wrapper
/// ensures that the correct callback function (`on_status_received`) is used for these calls.
///
/// The return type of this function MUST match the type used in the `Sender` typecast in
/// `on_status_received`.
pub(super) async fn call_api_with_status_callback<F>(call_api: F) -> OnStatusReceivedResult
where
    F: FnOnce(*mut c_void, rust_to_c::StatusCallbackFunction) -> i32,
{
    call_async_c_api(call_api, Some(on_status_received)).await
}

/// Callback used to send config list back to client.
///
/// # Safety
///
/// * `sender` must point to a valid `Sender` originally sent to the caller during the
///   `call_api_with_config_list_callback` API call. It will not be valid after the function
///   completes.
/// * `status_msg` must point to a C raw string that satisfies `CStr::from_ptr` safety requirements.
/// * `config_list` must point to valid pointer array containing C raw strings that satisfy
///   `CStr::from_ptr` safety requirements.
/// * `config_list_size` must contain the length of the byte array that can be parsed from
///   `config_list`.
unsafe extern "C" fn on_config_list_received(
    sender: *mut c_void,
    status_code: i32,
    status_msg: *const c_char,
    config_list: *const *const c_char,
    config_list_size: usize,
) {
    let status_code = TelemetryServiceStatusCodes(status_code);
    // SAFETY: safe to use as the `listener` passed by the caller was originally allocated using a
    // global allocator using Box::new().
    let sender = unsafe { Box::from_raw(sender as *mut Sender<OnConfigListReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let status_msg = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving config list: {status_msg}");
        sender.send(Err(TelemetryError { status: status_code, message: status_msg }));
        return;
    }

    // SAFETY: The caller has provided pointers that satisfy `slice::from_raw_parts` safety
    // requirements.
    let config_list = unsafe { slice::from_raw_parts(config_list, config_list_size) };
    let config_list = config_list
        .iter()
        .map(|c_str| {
            // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
            // safety requirements
            unsafe { CStr::from_ptr(*c_str) }.to_str().unwrap().to_owned()
        })
        .collect::<Vec<_>>();

    sender.send(Ok(config_list));
}

/// Safely call a C API that returns data through a `ConfigListCallbackFunction` callback. This
/// wrapper ensures that the correct callback function (`on_config_list_received`) is used for these
/// calls.
///
/// The return type of this function MUST match the type used in the `Sender` typecast in
/// `on_config_list_received`.
pub(super) async fn call_api_with_config_list_callback<F>(call_api: F) -> OnConfigListReceivedResult
where
    F: FnOnce(*mut c_void, rust_to_c::ConfigListCallbackFunction) -> i32,
{
    call_async_c_api(call_api, Some(on_config_list_received)).await
}

/// Callback used to send reports back to client.
///
/// # Safety
///
/// * `sender` must point to a valid `Sender` originally sent to the caller during the
///   `call_api_with_metrics_report_callback` API call. It will not be valid after the function
///   completes.
/// * `status_msg` must point to C raw string that satisfies `CStr::from_ptr` safety requirements.
/// * `report` must point to valid array of length `report_size`.
/// * `report_size` must contain the length of the byte array that can be parsed from `report`.
unsafe extern "C" fn on_report_received(
    sender: *mut c_void,
    status_code: i32,
    status_msg: *const c_char,
    report: *const u8,
    report_size: usize,
) {
    let status_code = TelemetryServiceStatusCodes(status_code);
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe { Box::from_raw(sender as *mut Sender<OnReportReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let msg = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving metrics report: {msg}");
        sender.send(Err(TelemetryError { status: status_code, message: msg }));
        return;
    }

    info!("Report received: {msg}");

    // SAFETY: The caller has provided pointers that satisfy `slice::from_raw_parts` safety
    // requirements.
    let byte_slice = unsafe { slice::from_raw_parts(report, report_size) };
    let metrics_report = match MetricsReport::parse_from_bytes(byte_slice) {
        Ok(report) => report,
        Err(e) => {
            sender.send(Err(TelemetryError {
                status: status_code,
                message: format!("Error in parsing report {e}"),
            }));
            return;
        }
    };

    sender.send(Ok(metrics_report));
}

/// Safely call a C API that returns data through a `GetSerializedDataCallbackFunction` callback.
/// This wrapper ensures that the correct callback function (`on_report_received`) is used for these
/// calls.
///
/// The return type of this function MUST match the type used in the `Sender` typecast in
/// `on_report_received`.
pub(super) async fn call_api_with_metrics_report_callback<F>(call_api: F) -> OnReportReceivedResult
where
    F: FnOnce(*mut c_void, rust_to_c::GetSerializedDataCallbackFunction) -> i32,
{
    call_async_c_api(call_api, Some(on_report_received)).await
}

/// Callback used to send ready reports info back to client.
///
/// # Safety
///
/// * `sender` must point to a valid `Sender` originally sent to the caller during the
///   `call_api_with_ready_reports_info_callback` API call. It will not be valid after the function
///   completes.
/// * `status_msg` must point to C raw string that satisfies `CStr::from_ptr` safety requirements.
/// * `ready_reports_info` must point to a valid pointer array of MetricsReportInfo with
///   `config_uuid` and `report_uuid` fields as C raw strings that satisfy `CStr::from_ptr` safety
///   requirements.
/// * `ready_reports_info_size` must contain the length of the byte array that can be parsed from
///   `ready_reports_info`.
unsafe extern "C" fn on_ready_reports_info_received(
    sender: *mut c_void,
    status_code: i32,
    status_msg: *const c_char,
    ready_reports_info: *const rust_to_c::MetricsReportInfo,
    ready_reports_info_size: usize,
) {
    let status_code = TelemetryServiceStatusCodes(status_code);
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe { Box::from_raw(sender as *mut Sender<OnReadyReportsInfoReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let msg = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving metrics report: {msg}");
        sender.send(Err(TelemetryError { status: status_code, message: msg }));
        return;
    }

    info!("Ready reports info received: {msg}");

    // SAFETY: The caller has provided pointers that satisfy `slice::from_raw_parts` safety
    // requirements.
    let ready_reports_info =
        unsafe { slice::from_raw_parts(ready_reports_info, ready_reports_info_size) };
    let ready_reports_info = ready_reports_info
        .iter()
        .map(|info| {
            ReportInfo {
                // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
                // safety requirements
                config_uuid: unsafe { CStr::from_ptr(info.config_uuid) }
                    .to_str()
                    .unwrap()
                    .to_owned(),
                // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
                // safety requirements
                report_uuid: unsafe { CStr::from_ptr(info.report_uuid) }
                    .to_str()
                    .unwrap()
                    .to_owned(),
            }
        })
        .collect::<Vec<_>>();

    sender.send(Ok(ready_reports_info));
}

/// Safely call a C API that returns data through a `ReadyReportsInfoCallbackFunction` callback.
/// This wrapper ensures that the correct callback function (`on_ready_reports_info_received`) is
/// used for these calls.
///
/// The return type of this function MUST match the type used in the `Sender` typecast in
/// `on_ready_reports_info_received`.
pub(super) async fn call_api_with_ready_reports_info_callback<F>(
    call_api: F,
) -> OnReadyReportsInfoReceivedResult
where
    F: FnOnce(*mut c_void, rust_to_c::ReadyReportsInfoCallbackFunction) -> i32,
{
    call_async_c_api(call_api, Some(on_ready_reports_info_received)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::matchers::{container_eq, elements_are, eq, err, ok, pat};
    use googletest::{assert_that, expect_that};
    use sdv_telemetry_test_utils::oneshot_callback;
    use tokio::join;
    use tokio::time::{error::Elapsed, sleep, timeout, Duration};

    type AsyncReturnValue = String;
    // In reality the callback would be a function pointer.
    type Callback = u8;

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_async_c_api_success() {
        let (tx, rx) = oneshot_callback();
        let result_future = call_async_c_api::<AsyncReturnValue, Callback, _>(
            |callback_data, callback| -> i32 {
                tx((callback, callback_data));
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
            },
            123,
        );

        let (result, ()) = join!(result_future, async move {
            // Simulate the C-API calling the callback asynchronously after 100ms.
            sleep(Duration::from_millis(100)).await;
            let (callback, callback_data) = rx.await;

            assert_that!(callback, eq(123));
            let result_tx =
                // SAFETY: This test simulates the C FFI boundary. `call_async_c_api`
                // creates a `Box<Sender>` and passes it as a `*mut c_void`. This code
                // reclaims ownership of the `Box` as the C callback would.
                unsafe { Box::from_raw(callback_data as *mut Sender<TelemetryResult<AsyncReturnValue>>) };

            result_tx.send(Ok("return value".to_owned()));
        });

        expect_that!(result, ok(eq("return value")));
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_async_c_api_drop_future() {
        let (tx, rx) = oneshot_callback();
        let result_future = call_async_c_api::<AsyncReturnValue, Callback, _>(
            |callback_data, callback| -> i32 {
                tx((callback, callback_data));
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
            },
            123,
        );

        // Simulate that the caller waits for the future for 200ms, but then gives up.
        let result = timeout(Duration::from_millis(200), result_future).await;
        assert_that!(result, err(pat!(Elapsed { .. })));
        drop(result);

        // Now, simulate the API call returning. This should not panic, even though the future is
        // already dropped.
        let (callback, callback_data) = rx.await;

        assert_that!(callback, eq(123));
        let result_tx =
            // SAFETY: This test simulates the C FFI boundary. `call_async_c_api`
            // creates a `Box<Sender>` and passes it as a `*mut c_void`. This code
            // reclaims ownership of the `Box` as the C callback would.
            unsafe { Box::from_raw(callback_data as *mut Sender<TelemetryResult<AsyncReturnValue>>) };
        result_tx.send(Ok("return value".to_owned()));
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_status_callback_success() {
        let (tx, rx) = oneshot_callback();
        let result_future = call_api_with_status_callback(|callback_data, callback| -> i32 {
            tx((callback.unwrap(), callback_data));
            TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
        });

        let (result, ()) = join!(result_future, async move {
            // Simulate the C-API calling the callback asynchronously after 100ms.
            sleep(Duration::from_millis(100)).await;
            let (callback, callback_data) = rx.await;

            // SAFETY: This test simulates the C side calling the callback. The arguments
            // are constructed to be valid, matching the contract of the
            // `on_status_received` function. `callback_data` and `callback` are known
            // to be valid from the call to `call_api_with_status_callback`.
            unsafe {
                (callback)(
                    callback_data,
                    TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                    c"some status message".as_ptr(),
                );
            }
        });

        expect_that!(result, ok(eq("some status message")));
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_status_callback_drop_future() {
        let (tx, rx) = oneshot_callback();
        let result_future = call_api_with_status_callback(|callback_data, callback| -> i32 {
            tx((callback.unwrap(), callback_data));
            TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
        });

        // Simulate that the caller waits for the future for 200ms, but then gives up.
        let result = timeout(Duration::from_millis(200), result_future).await;
        assert_that!(result, err(pat!(Elapsed { .. })));
        drop(result);

        // Now, simulate the API call returning. This should not panic, even though the future is
        // already dropped.
        let (callback, callback_data) = rx.await;

        // SAFETY: This test simulates the C side calling the callback. The arguments
        // are constructed to be valid, matching the contract of the
        // `on_status_received` function. `callback_data` and `callback` are known
        // to be valid from the call to `call_api_with_status_callback`.
        unsafe {
            (callback)(
                callback_data,
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                c"some status message".as_ptr(),
            );
        }
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_config_list_callback_success() {
        let (tx, rx) = oneshot_callback();
        let result_future = call_api_with_config_list_callback(|callback_data, callback| -> i32 {
            tx((callback.unwrap(), callback_data));
            TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
        });

        let (result, ()) = join!(result_future, async move {
            // Simulate the C-API calling the callback asynchronously after 100ms.
            sleep(Duration::from_millis(100)).await;
            let (callback, callback_data) = rx.await;
            let configs = [c"config1".as_ptr(), c"config2".as_ptr()];

            // SAFETY: This test simulates the C side calling the callback. The arguments
            // are constructed to be valid, matching the contract of the
            // `on_config_list_received` function. `callback_data` and `callback` are
            // known to be valid from the call to `call_api_with_config_list_callback`.
            unsafe {
                (callback)(
                    callback_data,
                    TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                    c"some status message".as_ptr(),
                    configs.as_ptr(),
                    configs.len(),
                );
            }
        });

        expect_that!(result, ok(container_eq(vec!["config1", "config2"])));
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_config_list_callback_drop_future() {
        let (tx, rx) = oneshot_callback();
        let result_future = call_api_with_config_list_callback(|callback_data, callback| -> i32 {
            tx((callback.unwrap(), callback_data));
            TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
        });

        // Simulate that the caller waits for the future for 200ms, but then gives up.
        let result = timeout(Duration::from_millis(200), result_future).await;
        assert_that!(result, err(pat!(Elapsed { .. })));
        drop(result);

        // Now, simulate the API call returning. This should not panic, even though the future is
        // already dropped.
        let (callback, callback_data) = rx.await;
        let configs = [c"config1".as_ptr(), c"config2".as_ptr()];

        // SAFETY: This test simulates the C side calling the callback. The arguments
        // are constructed to be valid, matching the contract of the
        // `on_config_list_received` function. `callback_data` and `callback` are
        // known to be valid from the call to `call_api_with_config_list_callback`.
        unsafe {
            (callback)(
                callback_data,
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                c"some status message".as_ptr(),
                configs.as_ptr(),
                configs.len(),
            );
        }
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_metrics_report_callback_success() {
        let (tx, rx) = oneshot_callback();
        let result_future =
            call_api_with_metrics_report_callback(|callback_data, callback| -> i32 {
                tx((callback.unwrap(), callback_data));
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
            });

        let (result, ()) = join!(result_future, async move {
            // Simulate the C-API calling the callback asynchronously after 100ms.
            sleep(Duration::from_millis(100)).await;
            let (callback, callback_data) = rx.await;
            let report = MetricsReport { report_number: 123, ..Default::default() };
            let report_bytes = report.write_to_bytes().unwrap();

            // SAFETY: This test simulates the C side calling the callback. The arguments
            // are constructed to be valid, matching the contract of the
            // `on_report_received` function. `callback_data` and `callback` are known
            // to be valid from the call to `call_api_with_metrics_report_callback`.
            unsafe {
                (callback)(
                    callback_data,
                    TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                    c"some status message".as_ptr(),
                    report_bytes.as_ptr(),
                    report_bytes.len(),
                );
            }
        });

        expect_that!(result, ok(pat!(MetricsReport { report_number: &123, .. })));
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_metrics_report_callback_drop_future() {
        let (tx, rx) = oneshot_callback();
        let result_future =
            call_api_with_metrics_report_callback(|callback_data, callback| -> i32 {
                tx((callback.unwrap(), callback_data));
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
            });

        // Simulate that the caller waits for the future for 200ms, but then gives up.
        let result = timeout(Duration::from_millis(200), result_future).await;
        assert_that!(result, err(pat!(Elapsed { .. })));
        drop(result);

        // Now, simulate the API call returning. This should not panic, even though the future is
        // already dropped.
        let (callback, callback_data) = rx.await;
        let report = MetricsReport { report_number: 123, ..Default::default() };
        let report_bytes = report.write_to_bytes().unwrap();

        // SAFETY: This test simulates the C side calling the callback. The arguments
        // are constructed to be valid, matching the contract of the
        // `on_report_received` function. `callback_data` and `callback` are known
        // to be valid from the call to `call_api_with_metrics_report_callback`.
        unsafe {
            (callback)(
                callback_data,
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                c"some status message".as_ptr(),
                report_bytes.as_ptr(),
                report_bytes.len(),
            );
        }
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_ready_reports_info_callback_success() {
        let (tx, rx) = oneshot_callback();
        let result_future =
            call_api_with_ready_reports_info_callback(|callback_data, callback| -> i32 {
                tx((callback.unwrap(), callback_data));
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
            });

        let (result, ()) = join!(result_future, async move {
            // Simulate the C-API calling the callback asynchronously after 100ms.
            sleep(Duration::from_millis(100)).await;
            let (callback, callback_data) = rx.await;
            let reports_info = [
                rust_to_c::MetricsReportInfo {
                    config_uuid: c"config1".as_ptr(),
                    report_uuid: c"report1".as_ptr(),
                },
                rust_to_c::MetricsReportInfo {
                    config_uuid: c"config2".as_ptr(),
                    report_uuid: c"report2".as_ptr(),
                },
            ];

            // SAFETY: This test simulates the C side calling the callback. The arguments
            // are constructed to be valid, matching the contract of the
            // `on_ready_reports_info_received` function. `callback_data` and
            // `callback` are known to be valid from the call to
            // `call_api_with_ready_reports_info_callback`.
            unsafe {
                (callback)(
                    callback_data,
                    TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                    c"some status message".as_ptr(),
                    reports_info.as_ptr(),
                    reports_info.len(),
                );
            }
        });

        expect_that!(
            result,
            ok(elements_are![
                pat!(ReportInfo { config_uuid: "config1", report_uuid: "report1", .. }),
                pat!(ReportInfo { config_uuid: "config2", report_uuid: "report2", .. }),
            ])
        );
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn call_api_with_ready_reports_info_callback_drop_future() {
        let (tx, rx) = oneshot_callback();
        let result_future =
            call_api_with_ready_reports_info_callback(|callback_data, callback| -> i32 {
                tx((callback.unwrap(), callback_data));
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0
            });

        // Simulate that the caller waits for the future for 200ms, but then gives up.
        let result = timeout(Duration::from_millis(200), result_future).await;
        assert_that!(result, err(pat!(Elapsed { .. })));
        drop(result);

        // Now, simulate the API call returning. This should not panic, even though the future is
        // already dropped.
        let (callback, callback_data) = rx.await;
        let reports_info = [
            rust_to_c::MetricsReportInfo {
                config_uuid: c"config1".as_ptr(),
                report_uuid: c"report1".as_ptr(),
            },
            rust_to_c::MetricsReportInfo {
                config_uuid: c"config2".as_ptr(),
                report_uuid: c"report2".as_ptr(),
            },
        ];

        // SAFETY: This test simulates the C side calling the callback. The arguments
        // are constructed to be valid, matching the contract of the
        // `on_ready_reports_info_received` function. `callback_data` and
        // `callback` are known to be valid from the call to
        // `call_api_with_ready_reports_info_callback`.
        unsafe {
            (callback)(
                callback_data,
                TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                c"some status message".as_ptr(),
                reports_info.as_ptr(),
                reports_info.len(),
            );
        }
    }
}
