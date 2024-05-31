// Copyright 2023 Google LLC. All Rights Reserved.

use crate::api_callback;
use crate::TelemetryError;
use crate::TelemetryResult;
use crate::TelemetryServiceHolder;

use log::{error, info};
use protobuf::Message;
use sdv_telemetry_bindgen as rust_to_c;
use sdv_telemetry_status_codes_bindgen as status_codes;
use std::ffi::{c_char, c_void, CStr, CString};
use std::slice;
use std::sync::Arc;
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
    status_code: i32,
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
/// * `listener` points to a valid MetricsReportReadyListener originally sent to the caller during
///    the set_report_ready_listener api call.
unsafe extern "C" fn on_report_ready(
    listener: *mut std::ffi::c_void,
    metrics_config_uuid: *const std::ffi::c_char,
    report_config_name: *const std::ffi::c_char,
    report_number: i32,
) {
    // SAFETY: The caller sends back the reference to MetricsReportReadyListener that
    // was originally sent to the caller during set_report_ready_listener call.
    let report_ready_listener =
        unsafe { Box::from_raw(listener as *mut MetricsReportReadyListener) };

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

    (report_ready_listener.report_ready_callback)(
        metrics_config_uuid,
        report_config_name,
        report_number,
    );
}

/// Struct used to wrap report_ready_callback received from Client
struct MetricsReportReadyListener {
    report_ready_callback: Arc<dyn Fn(String, String, i32) + Send + Sync>,
}

pub async fn set_report_ready_listener(
    service: &TelemetryServiceHolder,
    report_ready_callback: impl Fn(String, String, i32) + Send + Sync + 'static,
) -> TelemetryResult<String> {
    let listener = Box::new(MetricsReportReadyListener {
        report_ready_callback: Arc::new(report_ready_callback),
    });

    let (set_listener_sender, set_listener_receiver) = oneshot::channel();
    let sender_wrapper = Box::new(set_listener_sender);

    // SAFETY: `raw_listener` points to MetricsReportReadyListener and `raw_sender` points to a
    // valid oneshot::Sender that are initialized using Box::new and thus will not move in memory,
    // and remain a valid reference in case of successful rust_to_c api calls making the function
    // safe to call.
    // In case of failed api calls both the listeners are released to avoid memory leaks
    let raw_listener = unsafe {
        let raw_listener = Box::into_raw(listener);
        let raw_sender = Box::into_raw(sender_wrapper);

        let status_code = rust_to_c::set_report_ready_listener(
            service.0,
            raw_listener as *mut c_void,
            Some(on_report_ready),
            raw_sender as *mut c_void,
            Some(api_callback),
            std::ptr::null_mut(),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_listener);
            let _ = Box::from_raw(raw_sender);
            error!("Error in setting report ready listener");
            return Err(TelemetryError {
                status: status_code,
                message: "Invalid params".to_owned(),
            });
        }
        raw_listener
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

pub async fn clear_report_ready_listener(service: &TelemetryServiceHolder) -> TelemetryResult<()> {
    // SAFETY: `report_manager_raw` points to MetricsReportManager that is pinned
    // and thus will not move in memory, and remain a valid reference making the
    // function safe to call.
    unsafe {
        rust_to_c::clear_report_ready_listener(service.0, std::ptr::null_mut());
    }
    Ok(())
}

pub async fn get_metrics_report(
    service: &TelemetryServiceHolder,
    metrics_config_uuid: String,
    report_config_name: String,
    report_number: i32,
) -> TelemetryResult<MetricsReport> {
    let (get_report_sender, get_report_receiver) = oneshot::channel();
    let sender_wrapper = Box::new(get_report_sender);

    info!(
        "Report ready: {:?} {:?} report number: {}",
        metrics_config_uuid, report_config_name, report_number
    );

    let metrics_config_uuid = CString::new(metrics_config_uuid).expect("CString::new failed");
    let report_config_name = CString::new(report_config_name).expect("CString::new failed");

    let config_uuid_ref: *const c_char = metrics_config_uuid.as_ptr();
    let config_name_ref: *const c_char = report_config_name.as_ptr();

    // SAFETY: the caller has provided pointers that are used to safely get the
    // metrics report from the telemetry service.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper) as *mut c_void;
        let status_code = rust_to_c::get_metrics_report(
            service.0,
            config_uuid_ref,
            config_name_ref,
            report_number,
            raw_ptr,
            Some(on_report_received),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr as *mut oneshot::Sender<(i32, String)>);
        }
    }

    let (report, status, message): (Option<MetricsReport>, i32, String) =
        get_report_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(report.unwrap())
    } else {
        Err(TelemetryError { status, message })
    }
}
