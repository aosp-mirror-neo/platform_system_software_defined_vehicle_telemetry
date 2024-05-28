// Copyright 2023 Google LLC. All Rights Reserved.

use crate::api_callback;
use crate::TelemetryError;
use crate::TelemetryResult;
use crate::TelemetryServiceHolder;

use sdv_telemetry_bindgen as rust_to_c;
use status_codes_bindgen as status_codes;
use std::ffi::{c_void, CString};
use tokio::sync::oneshot;

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
