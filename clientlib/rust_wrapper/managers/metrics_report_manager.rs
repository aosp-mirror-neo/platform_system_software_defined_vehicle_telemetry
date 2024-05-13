// Copyright 2023 Google LLC. All Rights Reserved.

use crate::api_callback;
use crate::TelemetryError;
use crate::TelemetryResult;
use crate::TelemetryServiceHolder;

use log::{error, info};
use protobuf::Message;
use sdv_telemetry_bindgen as rust_to_c;
use sdv_telemetry_status_codes_bindgen as status_codes;
use status_codes::TelemetryServiceStatusCodes;
use std::ffi::{c_char, c_void, CStr, CString};
use std::slice;
use tokio::sync::oneshot;

use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;

/// Callback used to send reports back to client.
///
/// # Safety
///
/// * `status_msg` must point to C raw strings that satisfy `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the get_metrics_report api call. Listener will not be valid after the function completes
unsafe extern "C" fn on_report_received(
    listener: *mut c_void,
    status_code: TelemetryServiceStatusCodes,
    status_msg: *const c_char,
    report: *const u8,
    size: usize,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe {
        Box::from_raw(listener as *mut oneshot::Sender<(Option<MetricsReport>, i32, String)>)
    };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    if status_code != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving metrics report: {}", msg);
        sender.send((None, status_code, msg)).unwrap();
        return;
    }

    info!("Report received: {}", msg);

    // SAFETY: The caller has provided pointers that satisfy `slice::from_raw_parts`
    // safety requirements.
    let byte_slice = unsafe { slice::from_raw_parts(report, size) };
    let metrics_report = match MetricsReport::parse_from_bytes(byte_slice) {
        Ok(report) => report,
        Err(e) => {
            sender.send((None, status_code, format!("Error in parsing report {}", e))).unwrap();
            return;
        }
    };

    sender.send((Some(metrics_report), status_code, msg)).unwrap();
}

/// Callback that handles report ready event.
///
/// # Safety
///
/// * `metrics_config_uuid` and `report_config_name` must point to C raw strings that satisfy
///   `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `MetricsReportReadyListener` originally sent to the caller as
///    part of the `set_report_ready_listener` API call.
unsafe extern "C" fn on_report_ready(
    listener: *mut std::ffi::c_void,
    metrics_config_uuid: *const std::ffi::c_char,
    report_config_name: *const std::ffi::c_char,
    report_number: i32,
) {
    // SAFETY: The caller sends back the reference to `MetricsReportReadyListener` that was
    // originally sent to the caller during as part of the `set_report_ready_listener` API call.
    //
    // We do not use `Box::from_raw` here, because we do not want to take ownership, so that future
    // calls to `on_report_ready` can still access the same `listener`.
    let listener = unsafe { &mut *(listener as *mut MetricsReportReadyListener) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let (metrics_config_uuid, report_config_name) = unsafe {
        (
            CStr::from_ptr(metrics_config_uuid).to_str().unwrap().to_string(),
            CStr::from_ptr(report_config_name).to_str().unwrap().to_string(),
        )
    };

    info!(
        "Report ready: {:?} {:?} report number: {}",
        metrics_config_uuid, report_config_name, report_number
    );

    (listener.callback)(metrics_config_uuid, report_config_name, report_number);
}

/// Struct used to wrap the report ready callback received from the client.
struct MetricsReportReadyListener {
    callback: Box<dyn Fn(String, String, i32)>,
}

pub async fn set_report_ready_listener(
    service: &TelemetryServiceHolder,
    report_ready_callback: impl Fn(String, String, i32) + Send + Sync + 'static,
) -> TelemetryResult<String> {
    let listener =
        Box::new(MetricsReportReadyListener { callback: Box::new(report_ready_callback) });

    let (set_listener_sender, set_listener_receiver) = oneshot::channel();

    let raw_listener: *mut MetricsReportReadyListener = Box::into_raw(listener);
    let raw_sender = Box::into_raw(Box::new(set_listener_sender));
    let mut previous_raw_listener = std::ptr::null_mut::<MetricsReportReadyListener>();
    // SAFETY: `service.0` points to a valid `ServiceHolder`. `raw_listener` points to a valid
    // `MetricsReportReadyListener` and `raw_sender` points to a valid `oneshot::Sender`, both of
    // which are created using `Box::into_raw` and thus will not move in memory, and remain a valid
    // reference in case of successful `rust_to_c` API calls. In case of a failed API call, both are
    // released to avoid memory leaks. The output argument, `previous_raw_listener`, is a
    // null-initialized pointer to a `MetricsReportReadyListener`. A valid non-null pointer to this
    // pointer is passed to `set_report_ready_listener`. This makes the function safe to call.
    unsafe {
        let status_code = rust_to_c::set_report_ready_listener(
            service.0,
            raw_listener as *mut c_void,
            Some(on_report_ready),
            raw_sender as *mut c_void,
            Some(api_callback),
            (&mut previous_raw_listener) as *mut *mut _ as *mut *mut c_void,
        );
        if !previous_raw_listener.is_null() {
            // Regardless of whether the call succeeded or failed, if `previous_raw_listener` is
            // updated, take back ownership of it to reclaim memory.
            let _ = Box::from_raw(previous_raw_listener);
        }
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_listener);
            let _ = Box::from_raw(raw_sender);
            error!("Error while setting report ready listener: {status_code}");
            return Err(TelemetryError {
                status: status_code,
                message: "Invalid params".to_owned(),
            });
        }
    };

    let (status, message) = set_listener_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(message)
    } else {
        // Releasing memory in case of API call failure
        // SAFETY: safe to call since the caller is responsible managing raw_listener's memory
        // raw_listener was created using Box::into_raw above.
        let _ = unsafe { Box::from_raw(raw_listener) };
        Err(TelemetryError { status, message })
    }
}

pub fn clear_report_ready_listener(service: &TelemetryServiceHolder) -> TelemetryResult<()> {
    let mut previous_raw_listener = std::ptr::null_mut::<MetricsReportReadyListener>();
    // SAFETY: `service.0` points to a valid `ServiceHolder`. The output argument,
    // `previous_raw_listener`, is a null-initialized pointer to a `MetricsReportReadyListener`. A
    // valid non-null pointer to this pointer is passed to `set_report_ready_listener`. This makes
    // the function safe to call.
    let status_code = unsafe {
        rust_to_c::clear_report_ready_listener(
            service.0,
            (&mut previous_raw_listener) as *mut *mut _ as *mut *mut c_void,
        )
    };
    if !previous_raw_listener.is_null() {
        // SAFETY: If `clear_report_ready_listener` sets this pointer to a non-null value, it is
        // guaranteed to point to a `MetricsReportReadyListener` set in a previous call to
        // `set_report_ready_listener`. Therefore, it is safe to reclaim the memory.
        let _ = unsafe { Box::from_raw(previous_raw_listener) };
    }
    if status_code != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error while clearing report ready listener: {status_code}");
        return Err(TelemetryError { status: status_code, message: String::default() });
    }
    Ok(())
}

pub async fn get_metrics_report(
    service: &TelemetryServiceHolder,
    metrics_config_uuid: String,
    report_config_name: String,
    report_number: i32,
) -> TelemetryResult<MetricsReport> {
    let (get_report_sender, get_report_receiver) =
        oneshot::channel::<(Option<MetricsReport>, i32, String)>();

    info!(
        "Report ready: {:?} {:?} report number: {}",
        metrics_config_uuid, report_config_name, report_number
    );

    let metrics_config_uuid = CString::new(metrics_config_uuid).expect("CString::new failed");
    let report_config_name = CString::new(report_config_name).expect("CString::new failed");

    let config_uuid_ref: *const c_char = metrics_config_uuid.as_ptr();
    let config_name_ref: *const c_char = report_config_name.as_ptr();
    let raw_sender_wrapper = Box::into_raw(Box::new(get_report_sender));
    // SAFETY: the caller has provided pointers that are used to safely get the
    // metrics report from the telemetry service.
    unsafe {
        let status_code = rust_to_c::get_metrics_report(
            service.0,
            config_uuid_ref,
            config_name_ref,
            report_number,
            raw_sender_wrapper as *mut c_void,
            Some(on_report_received),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_sender_wrapper);
        }
    }

    let (report, status, message) = get_report_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(report.unwrap())
    } else {
        Err(TelemetryError { status, message })
    }
}
