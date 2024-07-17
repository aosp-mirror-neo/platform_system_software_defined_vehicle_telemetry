// Copyright 2023 Google LLC

use crate::callbacks::{call_api_with_metrics_report_callback, call_api_with_status_callback};
use crate::{ReportInfo, SendSyncPtr, TelemetryError, TelemetryResult, TelemetryServiceHolder};

use log::info;
use sdv_telemetry_bindgen as rust_to_c;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use sdv_telemetry_status_codes_bindgen as status_codes;
use std::ffi::{c_char, c_void, CStr, CString};

/// Callback that handles report ready event.
///
/// # Safety
///
/// * `listener` points to a valid `MetricsReportReadyListener` originally sent to the caller as
///    part of the `set_report_ready_listener` API call.
///
/// * `metrics_config_uuid` and `report_config_name` must point to C raw strings that satisfy
///   `CStr::from_ptr` safety requirements.
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

    let metrics_config_uuid =
        // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
        // requirements.
        unsafe { CStr::from_ptr(metrics_config_uuid) }.to_str().unwrap().to_owned();
    let report_config_name =
        // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
        // requirements.
        unsafe { CStr::from_ptr(report_config_name) }.to_str().unwrap().to_owned();

    info!(
        "Report ready: {metrics_config_uuid:?} {report_config_name:?} report number: {report_number}"
    );

    (listener.callback)(ReportInfo { metrics_config_uuid, report_config_name, report_number });
}

/// Struct used to wrap the report ready callback received from the client.
struct MetricsReportReadyListener {
    callback: Box<dyn Fn(ReportInfo) + Send + Sync>,
}

impl MetricsReportReadyListener {
    fn new(callback: Box<dyn Fn(ReportInfo) + Send + Sync>) -> Self {
        Self { callback }
    }
}

pub async fn set_report_ready_listener(
    service: &TelemetryServiceHolder,
    report_ready_callback: impl Fn(ReportInfo) + Send + Sync + 'static,
) -> TelemetryResult<String> {
    let raw_listener = SendSyncPtr(Box::into_raw(Box::new(MetricsReportReadyListener::new(
        Box::new(report_ready_callback),
    ))));
    call_api_with_status_callback(|status_callback_data, status_callback| {
        let mut previous_raw_listener = std::ptr::null_mut::<MetricsReportReadyListener>();

        // SAFETY: `service.0` points to a valid `ServiceHolder`. `raw_listener` points to a valid
        // `MetricsReportReadyListener` created using `Box::into_raw` and thus will not move in
        // memory, and remain a valid reference in case of successful `rust_to_c` API calls. In case
        // of a failed API call, both are released to avoid memory leaks. The output argument,
        // `previous_raw_listener`, is a null-initialized pointer to a `MetricsReportReadyListener`.
        // A valid non-null pointer to this pointer is passed to `set_report_ready_listener`. The
        // validity of `status_callback_data` and `status_callback` is ensured by
        // `call_api_with_status_callback`. This makes the function safe to call.
        unsafe {
            let status_code = rust_to_c::set_report_ready_listener(
                service.0,
                raw_listener.as_void_ptr(),
                Some(on_report_ready),
                status_callback_data,
                status_callback,
                (&mut previous_raw_listener) as *mut *mut _ as *mut *mut c_void,
            );
            if !previous_raw_listener.is_null() {
                // Regardless of whether the call succeeded or failed, if `previous_raw_listener` is
                // updated, take back ownership of it to reclaim memory.
                let _ = Box::from_raw(previous_raw_listener);
            }
            status_code
        }
    })
    .await
    .inspect_err(move |_| {
        // Releasing memory in case of API call failure.
        //
        //  SAFETY: safe to call since the `raw_listener.0` was created using `Box::into_raw` above
        // and it is guaranteed to not be used if by the C code if the API call fails.
        let _ = unsafe { Box::from_raw(raw_listener.0) };
    })
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
        return Err(TelemetryError { status: status_code, message: String::default() });
    }
    Ok(())
}

pub async fn get_metrics_report(
    service: &TelemetryServiceHolder,
    report_info: ReportInfo,
) -> TelemetryResult<MetricsReport> {
    info!(
        "Report ready: {:?} {:?} report number: {}",
        report_info.metrics_config_uuid, report_info.report_config_name, report_info.report_number
    );

    let metrics_config_uuid =
        CString::new(report_info.metrics_config_uuid).expect("CString::new failed");
    let report_config_name =
        CString::new(report_info.report_config_name).expect("CString::new failed");

    call_api_with_metrics_report_callback(move |callback_data, callback| {
        let config_uuid_ref: *const c_char = metrics_config_uuid.as_ptr();
        let config_name_ref: *const c_char = report_config_name.as_ptr();

        // SAFETY: the caller has provided pointers that are used to safely get the metrics report
        // from the telemetry service. The validity of `callback_data` and `callback` is ensured by
        // `call_api_with_metrics_report_callback`.
        unsafe {
            rust_to_c::get_metrics_report(
                service.0,
                config_uuid_ref,
                config_name_ref,
                report_info.report_number,
                callback_data,
                callback,
            )
        }
    })
    .await
}
