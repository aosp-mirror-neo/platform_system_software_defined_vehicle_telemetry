// Copyright 2023 Google LLC. All Rights Reserved.

use crate::TelemetryServiceHolder;

use sdv_telemetry_bindgen as rust_to_c;
use status_codes_bindgen as status_codes;
use std::ffi::{c_void, CStr, CString};
use tokio::sync::oneshot;

/// Callback that handles config added event.
///
/// # Safety
///
/// * `status_msg` must point to C raw strings that satisfy `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the add_metrics_config api call. Listener will not be valid after the function completes
unsafe extern "C" fn api_callback(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe { Box::from_raw(listener as *mut oneshot::Sender<(i32, String)>) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    sender.send((status_code, msg)).unwrap();
}

pub async fn add_metrics_config(
    service: &TelemetryServiceHolder,
    serialized_config: &[u8],
) -> Result<String, (i32, String)> {
    let (add_config_sender, add_config_receiver) = oneshot::channel();
    let sender_wrapper = Box::new(add_config_sender);

    // SAFETY: safe to use as all pointers needed to satisfy safety requirements
    // for api_callback are passed.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper) as *mut c_void;
        let status_code = rust_to_c::add_metrics_config(
            service.0,
            serialized_config.as_ptr() as *mut u8,
            serialized_config.len(),
            raw_ptr,
            Some(api_callback),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr as *mut oneshot::Sender<(i32, String)>);
        }
    }

    let (status, message) = add_config_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(message)
    } else {
        Err((status, message))
    }
}

pub async fn activate_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> Result<String, (i32, String)> {
    let (activate_config_sender, activate_config_receiver) = oneshot::channel();
    let sender_wrapper = Box::new(activate_config_sender);

    let uuid_cstring = CString::new(config_uuid).unwrap();

    // SAFETY: safe to use as all pointers needed to satisfy safety requirements
    // for api_callback are passed.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper) as *mut c_void;
        let status_code = rust_to_c::activate_metrics_config(
            service.0,
            uuid_cstring.as_ptr(),
            raw_ptr,
            Some(api_callback),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr as *mut oneshot::Sender<(i32, String)>);
        }
    }

    let (status, message) = activate_config_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(message)
    } else {
        Err((status, message))
    }
}

pub async fn deactivate_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> Result<String, (i32, String)> {
    let (deactivate_config_sender, deactivate_config_receiver) = oneshot::channel();
    let sender_wrapper = Box::new(deactivate_config_sender);

    let uuid_cstring = CString::new(config_uuid).unwrap();

    // SAFETY: safe to use as all pointers needed to satisfy safety requirements
    // for api_callback are passed.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper) as *mut c_void;
        let status_code = rust_to_c::deactivate_metrics_config(
            service.0,
            uuid_cstring.as_ptr(),
            raw_ptr,
            Some(api_callback),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr as *mut oneshot::Sender<(i32, String)>);
        }
    }

    let (status, message) = deactivate_config_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(message)
    } else {
        Err((status, message))
    }
}

pub async fn remove_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> Result<String, (i32, String)> {
    let (remove_config_sender, remove_config_receiver) = oneshot::channel();
    let sender_wrapper = Box::new(remove_config_sender);

    let uuid_cstring = CString::new(config_uuid).unwrap();

    // SAFETY: safe to use as all pointers needed to satisfy safety requirements
    // for api_callback are passed.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper) as *mut c_void;
        let status_code = rust_to_c::remove_metrics_config(
            service.0,
            uuid_cstring.as_ptr(),
            raw_ptr,
            Some(api_callback),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr as *mut oneshot::Sender<(i32, String)>);
        }
    }

    let (status, message) = remove_config_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(message)
    } else {
        Err((status, message))
    }
}

pub async fn remove_all_metrics_configs(
    service: &TelemetryServiceHolder,
) -> Result<(), (i32, String)> {
    let (remove_config_sender, remove_config_receiver) = oneshot::channel();
    let sender_wrapper = Box::new(remove_config_sender);

    // SAFETY: safe to use as all pointers needed to satisfy safety requirements
    // for api_callback are passed.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper) as *mut c_void;
        let status_code =
            rust_to_c::remove_all_metrics_configs(service.0, raw_ptr, Some(api_callback));
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr as *mut oneshot::Sender<(i32, String)>);
        }
    }

    let (status, message) = remove_config_receiver.await.unwrap();
    if status == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        Ok(())
    } else {
        Err((status, message))
    }
}
