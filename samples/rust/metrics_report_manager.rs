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

extern "C" fn on_report_ready(
    listener: *mut std::ffi::c_void,
    report_number: i32,
    report_uuid: *const std::ffi::c_char,
) {
    // SAFETY: This function is always safe to call
    let uuid = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(report_uuid);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    info!("Report ready: {} report number: {}", uuid, report_number);

    // SAFETY: This function is always safe to call
    let report_manager = unsafe { &mut *(listener as *mut MetricsReportManager) };

    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: This function is always safe to call.
    unsafe {
        rust_to_c::get_metrics_report(
            report_manager.service as *mut rust_to_c::ServiceHolder,
            report_number,
            report_uuid,
            listener,
            Some(on_report_received),
        );
    }
}

extern "C" fn on_report_received(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
    report: *const u8,
    size: usize,
) {
    // SAFETY: This function is always safe to call
    let report_manager = unsafe { &mut *(listener as *mut MetricsReportManager) };

    // SAFETY: This function is always safe to call
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    if status_code != status_codes::TelemetryServiceStatusCodes_GET_FINISHED_REPORT_SUCCEEDED {
        error!("Error in receiving metrics report: {}", msg);
        let _ = report_manager.get_report_sender.try_send(Err(msg.into()));
        return;
    }

    info!("Report received: {}", msg);

    // SAFETY: This function is always safe to call
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
    service: usize,
    get_report_sender:
        mpsc::Sender<Result<MetricsReport, Box<dyn std::error::Error + Send + Sync>>>,
    get_report_receiver:
        mpsc::Receiver<Result<MetricsReport, Box<dyn std::error::Error + Send + Sync>>>,
    _pin: PhantomPinned,
}

impl MetricsReportManager {
    pub fn new(service: usize) -> Pin<Box<Self>> {
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
        // SAFETY: This function is always safe to call.
        unsafe {
            rust_to_c::set_report_ready_listener(
                service as *mut rust_to_c::ServiceHolder,
                report_manager_raw as *mut std::ffi::c_void,
                Some(on_report_ready),
            );
        }

        report_manager
    }

    pub async fn get_metrics_report(
        self: Pin<&mut Self>,
    ) -> Result<MetricsReport, Box<dyn std::error::Error>> {
        // SAFETY: This function is always safe to call.
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
