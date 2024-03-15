// Copyright 2023 Google LLC. All Rights Reserved.

use crate::protobuf::Message;
use log::{error, info};
use sdv_telemetry_bindgen as rust_to_c;
use status_codes_bindgen as status_codes;
use std::ffi::CStr;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::slice;
use tokio::sync::mpsc;

use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;

/// Callback that handles report ready event.
///
/// # Safety
///
/// * `metrics_config_uuid` and `report_config_name` must point to C raw strings that satisfy
///   `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid MetricsReportManager originally sent to the caller during
///    the set_report_ready_listener api call.
unsafe extern "C" fn on_report_ready(
    listener: *mut std::ffi::c_void,
    metrics_config_uuid: *const std::ffi::c_char,
    report_config_name: *const std::ffi::c_char,
    report_number: i32,
) {
    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    unsafe {
        info!(
            "Report ready: {:?} {:?} report number: {}",
            CStr::from_ptr(metrics_config_uuid),
            CStr::from_ptr(report_config_name),
            report_number
        );
    }

    // SAFETY: The caller sends back a valid reference to MetricsReportManager
    // Refer the function docstring for more details
    let report_manager = unsafe { &mut *(listener as *mut MetricsReportManager) };

    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: the caller has provided pointers that are used to safely get the
    // metrics report from the telemetry service.
    unsafe {
        rust_to_c::get_metrics_report(
            report_manager.service,
            metrics_config_uuid,
            report_config_name,
            report_number,
            listener,
            Some(on_report_received),
        );
    }
}

unsafe extern "C" fn on_report_received(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
    report: *const u8,
    size: usize,
) {
    // SAFETY: The caller sends back the reference to MetricsReportManager that
    // was originally sent to the caller during set_report_ready_listener call.
    let report_manager = unsafe { &mut *(listener as *mut MetricsReportManager) };

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
        let _ = report_manager.get_report_sender.try_send(Err(msg.into()));
        return;
    }

    info!("Report received: {}", msg);

    // SAFETY: The caller has provided pointers that satisfy `slice::from_raw_parts`
    // safety requirements.
    let byte_slice = unsafe { slice::from_raw_parts(report, size) };
    let metrics_report = match MetricsReport::parse_from_bytes(byte_slice) {
        Ok(report) => report,
        Err(e) => {
            error!("Error in parsing report {}", e);
            return;
        }
    };

    let _ = report_manager.get_report_sender.try_send(Ok(metrics_report));
}

#[repr(C)]
pub struct MetricsReportManager {
    service: *mut rust_to_c::ServiceHolder,
    get_report_sender:
        mpsc::Sender<Result<MetricsReport, Box<dyn std::error::Error + Send + Sync>>>,
    get_report_receiver:
        mpsc::Receiver<Result<MetricsReport, Box<dyn std::error::Error + Send + Sync>>>,
    _pin: PhantomPinned,
}

impl MetricsReportManager {
    pub fn new(service: *mut rust_to_c::ServiceHolder) -> Pin<Box<Self>> {
        let (get_report_sender, get_report_receiver) = mpsc::channel(10);

        let report_manager = Box::pin(MetricsReportManager {
            service,
            get_report_sender,
            get_report_receiver,
            _pin: PhantomPinned,
        });

        // Convert to a raw pointer
        let report_manager_raw = &*report_manager as *const MetricsReportManager;

        // Unsafe block needed to be able to call C API functions
        // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
        // SAFETY: `report_manager_raw` points to MetricsReportManager that is pinned
        // and thus will not move in memory, and remain a valid reference making the
        // function safe to call.
        unsafe {
            rust_to_c::set_report_ready_listener(
                service,
                report_manager_raw as *mut std::ffi::c_void,
                Some(on_report_ready),
            );
        }

        report_manager
    }

    pub async fn get_metrics_report(
        self: Pin<&mut Self>,
    ) -> Result<MetricsReport, Box<dyn std::error::Error>> {
        // SAFETY: The data inside the mutable reference will never move out of the
        // mutable reference and the function is safe to call.
        let mut_ref = unsafe { self.get_unchecked_mut() };
        let report = match mut_ref.get_report_receiver.recv().await {
            Some(Ok(report)) => report,
            Some(Err(e)) => {
                error!("Error in getting report");
                return Err(e);
            }
            None => todo!(),
        };
        Ok(report)
    }
}
