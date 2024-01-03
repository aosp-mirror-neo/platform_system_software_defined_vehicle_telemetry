// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(dead_code)]
use log::error;
use sdv_telemetry_bindgen as rust_to_c;
use status_codes_bindgen as status_codes;
use std::ffi::{c_void, CStr, CString};
use std::marker::PhantomPinned;
use std::pin::Pin;
use tokio::sync::oneshot;

/// Callback that handles config added event.
///
/// # Safety
///
/// * `status_msg` must point to C raw strings that satisfy `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the add_metrics_config api call.
unsafe extern "C" fn on_config_added(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe { Box::from_raw(listener as *mut oneshot::Sender<(bool, String)>) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    let _ = sender.send((
        status_code == status_codes::TelemetryServiceStatusCodes_ADD_METRICS_CONFIG_SUCCEEDED,
        msg,
    ));
}

/// Callback that handles config activated event.
///
/// # Safety
///
/// * `status_msg` must point to C raw strings that satisfy `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the activate_metrics_config api call.
extern "C" fn on_config_activated(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe { Box::from_raw(listener as *mut oneshot::Sender<(bool, String)>) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    let _ = sender.send((
        status_code == status_codes::TelemetryServiceStatusCodes_ACTIVATE_METRICS_CONFIG_SUCCEEDED,
        msg,
    ));
}

/// Callback that handles config activated event.
///
/// # Safety
///
/// * `status_msg` must point to C raw strings that satisfy `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the remove_metrics_config api call.
extern "C" fn on_config_removed(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe { Box::from_raw(listener as *mut oneshot::Sender<(bool, String)>) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    let _ = sender.send((
        status_code == status_codes::TelemetryServiceStatusCodes_REMOVE_METRICS_CONFIG_SUCCEEDED,
        msg,
    ));
}

#[derive(Clone)]
#[repr(C)]
pub struct MetricsConfigManager {
    service: usize,
    _pin: PhantomPinned,
}

impl MetricsConfigManager {
    pub fn new(service: usize) -> Pin<Box<Self>> {
        Box::pin(MetricsConfigManager { service, _pin: PhantomPinned })
    }

    pub async fn add_metrics_config(&self, serialized_config: &[u8]) -> Result<String, String> {
        let (add_config_sender, add_config_receiver) = oneshot::channel();
        let sender_wrapper = Box::new(add_config_sender);

        // Unsafe block needed to be able to call C API functions
        // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
        // SAFETY: safe to use as all pointers needed to satify safety requirements
        // for on_config_added are passed.
        unsafe {
            rust_to_c::add_metrics_config(
                self.service as *mut rust_to_c::ServiceHolder,
                serialized_config.as_ptr() as *mut u8,
                serialized_config.len(),
                Box::into_raw(sender_wrapper) as *mut c_void,
                Some(on_config_added),
            );
        }

        match add_config_receiver.await {
            Ok((status, message)) => {
                if status {
                    Ok(message)
                } else {
                    Err(message)
                }
            }
            Err(e) => {
                error!("Error: {}", e);
                Err(e.to_string())
            }
        }
    }

    pub async fn activate_metrics_config(&self, config_uuid: &str) -> Result<String, String> {
        let (activate_config_sender, activate_config_receiver) = oneshot::channel();
        let sender_wrapper = Box::new(activate_config_sender);

        let uuid_cstring = CString::new(config_uuid).unwrap();
        // Unsafe block needed to be able to call C API functions
        // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
        // SAFETY: safe to use as all pointers needed to satify safety requirements
        // for on_config_activated are passed.
        unsafe {
            rust_to_c::activate_metrics_config(
                self.service as *mut rust_to_c::ServiceHolder,
                uuid_cstring.as_ptr(),
                Box::into_raw(sender_wrapper) as *mut c_void,
                Some(on_config_activated),
            );
        }

        match activate_config_receiver.await {
            Ok((status, message)) => {
                if status {
                    Ok(message)
                } else {
                    Err(message)
                }
            }
            Err(e) => {
                error!("Error: {}", e);
                Err(e.to_string())
            }
        }
    }

    pub async fn remove_metrics_config(&self, config_uuid: &str) -> Result<String, String> {
        let (remove_config_sender, remove_config_receiver) = oneshot::channel();
        let sender_wrapper = Box::new(remove_config_sender);

        let uuid_cstring = CString::new(config_uuid).unwrap();
        // Unsafe block needed to be able to call C API functions
        // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
        // SAFETY: safe to use as all pointers needed to satify safety requirements
        // for on_config_removed are passed.
        unsafe {
            rust_to_c::remove_metrics_config(
                self.service as *mut rust_to_c::ServiceHolder,
                uuid_cstring.as_ptr(),
                Box::into_raw(sender_wrapper) as *mut c_void,
                Some(on_config_removed),
            );
        }

        match remove_config_receiver.await {
            Ok((status, message)) => {
                if status {
                    Ok(message)
                } else {
                    Err(message)
                }
            }
            Err(e) => {
                error!("Error: {}", e);
                Err(e.to_string())
            }
        }
    }
}
