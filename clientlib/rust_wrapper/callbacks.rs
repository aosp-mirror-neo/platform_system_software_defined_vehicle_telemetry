// Copyright 2024 Google LLC

use log::{error, info};
use protobuf::Message;
use sdv_telemetry_bindgen as rust_to_c;
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
    let result_tx = Box::into_raw(Box::new(result_tx));

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
/// * `sender` points to a valid `oneshot::Sender` originally sent to the caller during the
///    `call_api_with_status_callback` API call. It will not be valid after the function completes
/// * `status_msg` must point to a C raw string that satisfies `CStr::from_ptr` safety requirements.
unsafe extern "C" fn on_status_received(
    sender: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    let status_code = TelemetryServiceStatusCodes(status_code);
    // SAFETY: safe to use as the sender passed by the caller was originally allocated using a
    // global allocator using Box::new().
    let sender = unsafe { Box::from_raw(sender as *mut oneshot::Sender<OnStatusReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let status_message = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    let result = if status_code == TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(status_message)
    } else {
        Err(TelemetryError { status: status_code, message: status_message })
    };
    sender.send(result).unwrap();
}

/// Safely call a C API that returns data through a `StatusCallbackFunction` callback. This wrapper
/// ensures that the correct callback function (`on_status_received`) is used for these calls.
///
/// The return type of this function MUST match the type used in the `oneshot::Sender` typecast in
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
/// * `sender` points to a valid `oneshot::Sender` originally sent to the caller during the
///    `call_api_with_config_list_callback` API call. It will not be valid after the function
///    completes.
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
    let sender =
        unsafe { Box::from_raw(sender as *mut oneshot::Sender<OnConfigListReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let status_msg = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving config list: {}", status_msg);
        sender.send(Err(TelemetryError { status: status_code, message: status_msg })).unwrap();
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

    sender.send(Ok(config_list)).unwrap();
}

/// Safely call a C API that returns data through a `ConfigListCallbackFunction` callback. This
/// wrapper ensures that the correct callback function (`on_config_list_received`) is used for these
/// calls.
///
/// The return type of this function MUST match the type used in the `oneshot::Sender` typecast in
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
/// * `sender` points to a valid `oneshot::Sender` originally sent to the caller during the
///    `call_api_with_metrics_report_callback` API call. It will not be valid after the function
///    completes.
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
    let sender = unsafe { Box::from_raw(sender as *mut oneshot::Sender<OnReportReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let msg = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving metrics report: {}", msg);
        sender.send(Err(TelemetryError { status: status_code, message: msg })).unwrap();
        return;
    }

    info!("Report received: {msg}");

    // SAFETY: The caller has provided pointers that satisfy `slice::from_raw_parts` safety
    // requirements.
    let byte_slice = unsafe { slice::from_raw_parts(report, report_size) };
    let metrics_report = match MetricsReport::parse_from_bytes(byte_slice) {
        Ok(report) => report,
        Err(e) => {
            sender
                .send(Err(TelemetryError {
                    status: status_code,
                    message: format!("Error in parsing report {}", e),
                }))
                .unwrap();
            return;
        }
    };

    sender.send(Ok(metrics_report)).unwrap();
}

/// Safely call a C API that returns data through a `GetSerializedDataCallbackFunction` callback.
/// This wrapper ensures that the correct callback function (`on_report_received`) is used for these
/// calls.
///
/// The return type of this function MUST match the type used in the `oneshot::Sender` typecast in
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
/// * `sender` points to a valid `oneshot::Sender` originally sent to the caller during the
///    `call_api_with_ready_reports_info_callback` API call. It will not be valid after the function
///    completes.
/// * `status_msg` must point to C raw string that satisfies `CStr::from_ptr` safety requirements.
/// * `ready_reports_info` must point to a valid pointer array of MetricsReportInfo with
///   `config_uuid` and `report_uuid` fields as C raw strings that satisfy `CStr::from_ptr` safety
///   requirements.
/// * `ready_reports_info_size` must contain the length of the byte array that can be parsed from `ready_reports_info`.
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
    let sender =
        unsafe { Box::from_raw(sender as *mut oneshot::Sender<OnReadyReportsInfoReceivedResult>) };

    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let msg = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving metrics report: {}", msg);
        sender.send(Err(TelemetryError { status: status_code, message: msg })).unwrap();
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

    sender.send(Ok(ready_reports_info)).unwrap();
}

/// Safely call a C API that returns data through a `ReadyReportsInfoCallbackFunction` callback.
/// This wrapper ensures that the correct callback function (`on_ready_reports_info_received`) is
/// used for these calls.
///
/// The return type of this function MUST match the type used in the `oneshot::Sender` typecast in
/// `on_ready_reports_info_received`.
pub(super) async fn call_api_with_ready_reports_info_callback<F>(
    call_api: F,
) -> OnReadyReportsInfoReceivedResult
where
    F: FnOnce(*mut c_void, rust_to_c::ReadyReportsInfoCallbackFunction) -> i32,
{
    call_async_c_api(call_api, Some(on_ready_reports_info_received)).await
}
