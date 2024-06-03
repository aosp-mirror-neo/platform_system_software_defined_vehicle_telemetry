// Copyright 2023 Google LLC. All Rights Reserved.

use crate::api_callback;
use crate::TelemetryError;
use crate::TelemetryResult;
use crate::TelemetryServiceHolder;

use log::error;
use sdv_telemetry_bindgen as rust_to_c;
use sdv_telemetry_status_codes_bindgen as status_codes;
use std::ffi::{c_char, c_void, CStr, CString};
use tokio::sync::oneshot;

/// Callback used to send config list back to client.
///
/// # Safety
///
/// `status_msg` must point to C raw strings that satisfy `CStr::from_ptr` safety requirements.
///
/// `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the get_config_list api call. Listener will not be valid after the function completes
///
/// `config_list` must point to valid pointer array containing metrics config uuids
/// `size` must contain the length of the byte array that can be parsed from config_list
unsafe extern "C" fn on_config_list_received(
    listener: *mut c_void,
    status_code: i32,
    status_msg: *const c_char,
    config_list: *mut *const c_char,
    count: usize,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe {
        Box::from_raw(listener as *mut oneshot::Sender<Result<Vec<String>, (i32, String)>>)
    };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let c_str = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        CStr::from_ptr(status_msg)
    };
    // Convert &CStr to String
    let msg = c_str.to_str().unwrap().to_string();

    if status_code != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        error!("Error in receiving config list: {}", msg);
        sender.send(Err((status_code, msg))).unwrap();
        return;
    }

    // SAFETY: The caller has provided pointers that satisfy `slice::from_raw_parts`
    // safety requirements.
    let c_str_array = unsafe { std::slice::from_raw_parts(config_list, count) };

    let mut rust_strings = Vec::new();
    for &c_str in c_str_array {
        // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
        // safety requirements
        let rust_string = unsafe { CStr::from_ptr(c_str) }.to_str().unwrap().to_string();
        rust_strings.push(rust_string);
    }

    sender.send(Ok(rust_strings)).unwrap();
}

pub async fn add_metrics_config(
    service: &TelemetryServiceHolder,
    serialized_config: &[u8],
) -> TelemetryResult<String> {
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
        Err(TelemetryError { status, message })
    }
}

pub async fn activate_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> TelemetryResult<String> {
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
        Err(TelemetryError { status, message })
    }
}

pub async fn deactivate_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> TelemetryResult<String> {
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
        Err(TelemetryError { status, message })
    }
}

pub async fn remove_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> TelemetryResult<String> {
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
        Err(TelemetryError { status, message })
    }
}

pub async fn remove_all_metrics_configs(service: &TelemetryServiceHolder) -> TelemetryResult<()> {
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
        Err(TelemetryError { status, message })
    }
}

pub async fn get_active_metrics_config_uuids(
    service: &TelemetryServiceHolder,
) -> Result<Vec<String>, (i32, String)> {
    let (get_config_list_sender, get_config_list_receiver) =
        oneshot::channel::<Result<Vec<String>, (i32, String)>>();
    let sender_wrapper = Box::new(get_config_list_sender);

    // SAFETY: safe to use as all pointers needed to satisfy safety requirements
    // for on_config_list_received are passed.
    // sender_wrapper is allocated on the heap using Box::new(), and we take ownership by calling
    // Box::into_raw. That memory is released by calling Box::from_raw in case of API failure.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper);
        let status_code = rust_to_c::get_active_metrics_config_uuids(
            service.0,
            raw_ptr as *mut c_void,
            Some(on_config_list_received),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr);
        }
    }
    get_config_list_receiver.await.unwrap()
}

pub async fn get_inactive_metrics_config_uuids(
    service: &TelemetryServiceHolder,
) -> Result<Vec<String>, (i32, String)> {
    let (get_config_list_sender, get_config_list_receiver) =
        oneshot::channel::<Result<Vec<String>, (i32, String)>>();
    let sender_wrapper = Box::new(get_config_list_sender);

    // SAFETY: safe to use as all pointers needed to satisfy safety requirements
    // for on_config_list_received are passed.
    // sender_wrapper is allocated on the heap using Box::new(), and we take ownership by calling
    // Box::into_raw. That memory is released by calling Box::from_raw in case of API failure.
    unsafe {
        let raw_ptr = Box::into_raw(sender_wrapper);
        let status_code = rust_to_c::get_inactive_metrics_config_uuids(
            service.0,
            raw_ptr as *mut c_void,
            Some(on_config_list_received),
        );
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            // Releasing memory in case of API call failure
            let _ = Box::from_raw(raw_ptr);
        }
    }

    get_config_list_receiver.await.unwrap()
}
