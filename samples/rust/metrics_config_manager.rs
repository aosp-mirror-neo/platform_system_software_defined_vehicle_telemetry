// Copyright 2023 Google LLC. All Rights Reserved.

use log::error;
use sdv_telemetry_bindgen as rust_to_c;
use status_codes_bindgen as status_codes;
use std::ffi::{CStr, CString};
use std::marker::PhantomPinned;
use std::pin::Pin;
use tokio::sync::oneshot;

extern "C" fn on_config_added(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: This function is always safe to call
    let config_manager = unsafe { &mut *(listener as *mut MetricsConfigManager) };

    // SAFETY: This function is always safe to call
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    if let Some(sender) = config_manager.add_config_sender.take() {
        let _ = sender.send((
            status_code == status_codes::TelemetryServiceStatusCodes_ADD_METRICS_CONFIG_SUCCEEDED,
            msg,
        ));
    }
}

extern "C" fn on_config_activated(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: This function is always safe to call
    let config_manager = unsafe { &mut *(listener as *mut MetricsConfigManager) };

    // SAFETY: This function is always safe to call
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    if let Some(sender) = config_manager.activate_config_sender.take() {
        let _ = sender.send((
            status_code
                == status_codes::TelemetryServiceStatusCodes_ACTIVATE_METRICS_CONFIG_SUCCEEDED,
            msg,
        ));
    }
}

extern "C" fn on_config_removed(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: This function is always safe to call
    let config_manager = unsafe { &mut *(listener as *mut MetricsConfigManager) };

    // SAFETY: This function is always safe to call
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    if let Some(sender) = config_manager.remove_config_sender.take() {
        let _ = sender.send((
            status_code
                == status_codes::TelemetryServiceStatusCodes_REMOVE_METRICS_CONFIG_SUCCEEDED,
            msg,
        ));
    }
}

#[repr(C)]
pub struct MetricsConfigManager {
    service: *mut rust_to_c::ServiceHolder,
    add_config_sender: Option<oneshot::Sender<(bool, String)>>,
    activate_config_sender: Option<oneshot::Sender<(bool, String)>>,
    remove_config_sender: Option<oneshot::Sender<(bool, String)>>,
    _pin: PhantomPinned,
}

impl MetricsConfigManager {
    pub fn new(service: *mut rust_to_c::ServiceHolder) -> Pin<Box<Self>> {
        Box::pin(MetricsConfigManager {
            service,
            add_config_sender: None,
            activate_config_sender: None,
            remove_config_sender: None,
            _pin: PhantomPinned,
        })
    }

    pub async fn add_metrics_config(
        self: Pin<&mut Self>,
        serialized_config: &[u8],
    ) -> Result<String, String> {
        // SAFETY: This function is always safe to call.
        let mut_ref = unsafe { self.get_unchecked_mut() };

        let (add_config_sender, add_config_receiver) = oneshot::channel();
        mut_ref.add_config_sender = Some(add_config_sender);

        // Unsafe block needed to be able to call C API functions
        // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
        // SAFETY: This function is always safe to call.
        unsafe {
            rust_to_c::add_metrics_config(
                mut_ref.service,
                serialized_config.as_ptr() as *mut u8,
                serialized_config.len(),
                mut_ref as *mut _ as *mut std::ffi::c_void,
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

    pub async fn activate_metrics_config(
        self: Pin<&mut Self>,
        config_uuid: &str,
    ) -> Result<String, String> {
        // SAFETY: This function is always safe to call.
        let mut_ref = unsafe { self.get_unchecked_mut() };

        let (activate_config_sender, activate_config_receiver) = oneshot::channel();
        mut_ref.activate_config_sender = Some(activate_config_sender);

        let uuid_cstring = CString::new(config_uuid).unwrap();
        // Unsafe block needed to be able to call C API functions
        // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
        // SAFETY: This function is always safe to call.
        unsafe {
            rust_to_c::activate_metrics_config(
                mut_ref.service,
                uuid_cstring.as_ptr(),
                mut_ref as *mut _ as *mut std::ffi::c_void,
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

    pub async fn remove_metrics_config(
        self: Pin<&mut Self>,
        config_uuid: &str,
    ) -> Result<String, String> {
        // SAFETY: This function is always safe to call.
        let mut_ref = unsafe { self.get_unchecked_mut() };

        let (remove_config_sender, remove_config_receiver) = oneshot::channel();
        mut_ref.remove_config_sender = Some(remove_config_sender);

        let uuid_cstring = CString::new(config_uuid).unwrap();
        // Unsafe block needed to be able to call C API functions
        // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
        // SAFETY: This function is always safe to call.
        unsafe {
            rust_to_c::remove_metrics_config(
                mut_ref.service,
                uuid_cstring.as_ptr(),
                mut_ref as *mut _ as *mut std::ffi::c_void,
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
