// Copyright 2023 Google LLC

use crate::callbacks::{call_api_with_config_list_callback, call_api_with_status_callback};
use crate::TelemetryResult;
use crate::TelemetryServiceHolder;

use sdv_telemetry_bindgen as rust_to_c;
use std::ffi::CString;

pub async fn add_metrics_config(
    service: &TelemetryServiceHolder,
    serialized_config: &[u8],
) -> TelemetryResult<String> {
    call_api_with_status_callback(move |callback_data, callback| {
        // SAFETY: Safe to call as `service.0` is a valid pointer to `ServiceHolder`,
        // `serialized_config.*` arguments point to a valid slice, and validity of `callback_data`
        // and `callback` is ensured by the `call_api_with_status_callback` wrapper.
        unsafe {
            rust_to_c::add_metrics_config(
                service.0,
                serialized_config.as_ptr() as *mut u8,
                serialized_config.len(),
                callback_data,
                callback,
            )
        }
    })
    .await
}

pub async fn activate_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> TelemetryResult<String> {
    let uuid_cstring = CString::new(config_uuid).unwrap();
    call_api_with_status_callback(move |callback_data, callback| {
        // SAFETY: Safe to call as `service.0` is a valid pointer to `ServiceHolder`,
        // `uuid_string.as_ptr()` points to a valid `CString`, and validity of `callback_data` and
        // `callback` is ensured by the `call_api_with_status_callback` wrapper.
        unsafe {
            rust_to_c::activate_metrics_config(
                service.0,
                uuid_cstring.as_ptr(),
                callback_data,
                callback,
            )
        }
    })
    .await
}

pub async fn deactivate_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> TelemetryResult<String> {
    let uuid_cstring = CString::new(config_uuid).unwrap();
    call_api_with_status_callback(move |callback_data, callback| {
        // SAFETY: Safe to call as `service.0` is a valid pointer to `ServiceHolder`,
        // `uuid_string.as_ptr()` points to a valid `CString`, and validity of `callback_data` and
        // `callback` is ensured by the `call_api_with_status_callback` wrapper.
        unsafe {
            rust_to_c::deactivate_metrics_config(
                service.0,
                uuid_cstring.as_ptr(),
                callback_data,
                callback,
            )
        }
    })
    .await
}

pub async fn remove_metrics_config(
    service: &TelemetryServiceHolder,
    config_uuid: &str,
) -> TelemetryResult<String> {
    let uuid_cstring = CString::new(config_uuid).unwrap();
    call_api_with_status_callback(move |callback_data, callback| {
        // SAFETY: Safe to call as `service.0` is a valid pointer to `ServiceHolder`,
        // `uuid_string.as_ptr()` points to a valid `CString`, and validity of `callback_data` and
        // `callback` is ensured by the `call_api_with_status_callback` wrapper.
        unsafe {
            rust_to_c::remove_metrics_config(
                service.0,
                uuid_cstring.as_ptr(),
                callback_data,
                callback,
            )
        }
    })
    .await
}

pub async fn remove_all_metrics_configs(
    service: &TelemetryServiceHolder,
) -> TelemetryResult<String> {
    call_api_with_status_callback(move |callback_data, callback| {
        // SAFETY: Safe to call as `service.0` is a valid pointer to `ServiceHolder` and validity of
        // `callback_data` and `callback` is ensured by the `call_api_with_status_callback` wrapper.
        unsafe { rust_to_c::remove_all_metrics_configs(service.0, callback_data, callback) }
    })
    .await
}

pub async fn get_active_metrics_config_uuids(
    service: &TelemetryServiceHolder,
) -> TelemetryResult<Vec<String>> {
    call_api_with_config_list_callback(move |callback_data, callback| {
        // SAFETY: Safe to call as `service.0` is a valid pointer to `ServiceHolder` and validity of
        // `callback_data` and `callback` is ensured by the `call_api_with_config_list_callback`
        // wrapper.
        unsafe { rust_to_c::get_active_metrics_config_uuids(service.0, callback_data, callback) }
    })
    .await
}

pub async fn get_inactive_metrics_config_uuids(
    service: &TelemetryServiceHolder,
) -> TelemetryResult<Vec<String>> {
    call_api_with_config_list_callback(move |callback_data, callback| {
        // SAFETY: Safe to call as `service.0` is a valid pointer to `ServiceHolder` and validity of
        // `callback_data` and `callback` is ensured by the `call_api_with_config_list_callback`
        // wrapper.
        unsafe { rust_to_c::get_inactive_metrics_config_uuids(service.0, callback_data, callback) }
    })
    .await
}
