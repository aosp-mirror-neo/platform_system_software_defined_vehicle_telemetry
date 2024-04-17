// Copyright 2023 Google LLC. All Rights Reserved.

//! This module acts as a wrapper around C Client library for telemetry service

mod managers;

use crate::rust_to_c::ServiceHolder;
use log::{error, info};
use sdv_telemetry_bindgen as rust_to_c;
use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;
use status_codes_bindgen as status_codes;
use std::ffi::{c_void, CStr};
use std::sync::Arc;

use managers::metrics_config_manager;
use managers::metrics_report_manager;

/// Struct used to wrap ServiceHolder received from C in a typesafe manner
#[repr(transparent)]
pub struct TelemetryServiceHolder(*mut ServiceHolder);

/// Callback function that receives status from telemetry service
unsafe extern "C" fn on_status(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: The caller sends back the reference to StatusListener that
    // was originally sent to the caller during set_status_listener call.
    let status_listener = unsafe { Box::from_raw(listener as *mut StatusListener) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let status_msg = unsafe { CStr::from_ptr(status_msg).to_str().unwrap().to_string() };

    info!("Status code: {} status message: {}", status_code, status_msg);

    (status_listener.status_callback)(status_code, status_msg);
}

/// Struct used to wrap status_callback received from Client
struct StatusListener {
    status_callback: Arc<dyn Fn(i32, String) + Send + Sync>,
}

impl TelemetryServiceHolder {
    /// Gets an instance of telemetry_service and type casts it to an opaque
    /// pointer. This is a blocking call and it will wait until the telemetry binder service
    /// is ready. The returned value will never be null, and will always hold an instance
    /// of telemetry service.
    ///
    /// @return an opaque pointer holding a reference to the telemetry_service

    pub fn get() -> Result<TelemetryServiceHolder, (i32, String)> {
        info!("Connecting to service");

        // SAFETY: safe to use as all pointers needed to satify safety requirements
        // for on_status are passed.
        let service = unsafe {
            let service_raw = rust_to_c::get_telemetry_service();
            if !service_raw.is_null() {
                // Convert to an immutable reference
                service_raw
            } else {
                return Err((
                    status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_UNKNOWN_ERROR,
                    "Unable to get telemetry service".to_string(),
                ));
            }
        };
        Ok(TelemetryServiceHolder(service))
    }

    /// Adds a metrics config to the telemetry service passed as a byte array. Any metrics
    /// config added is inactive by default and needs to be activated explicitly. Client can receive
    /// errors related to the config if a status listener is added to the telemetry service.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    /// @param serialized_config Pointer to the serialized metrics config
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure

    pub async fn add_metrics_config(
        &self,
        serialized_config: &[u8],
    ) -> Result<String, (i32, String)> {
        metrics_config_manager::add_metrics_config(self, serialized_config).await
    }

    /// Activates a previously added metrics config or a deactivated metrics config.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    /// @param config_uuid UUID of the metrics config that needs to be activated.
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure

    pub async fn activate_metrics_config(
        &self,
        config_uuid: &str,
    ) -> Result<String, (i32, String)> {
        metrics_config_manager::activate_metrics_config(self, config_uuid).await
    }

    /// Deactivates an active metrics config.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    /// @param config_uuid UUID of the metrics config that needs to be deactivated.
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure

    pub async fn deactivate_metrics_config(
        &self,
        config_uuid: &str,
    ) -> Result<String, (i32, String)> {
        metrics_config_manager::deactivate_metrics_config(self, config_uuid).await
    }

    /// Removes a previously added metrics config. The config being removed will be
    /// deactivated if it's in active state before being removed.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    /// @param config_uuid UUID of the metrics config that needs to be removed.
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure

    pub async fn remove_metrics_config(&self, config_uuid: &str) -> Result<String, (i32, String)> {
        metrics_config_manager::remove_metrics_config(self, config_uuid).await
    }

    /// Removes all previously added metrics config. The configs be deactivated
    /// if it's in active state before being removed.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    ///
    /// @return an error code and error message in case of failure

    pub async fn remove_all_metrics_configs(&self) -> Result<(), (i32, String)> {
        metrics_config_manager::remove_all_metrics_configs(self).await
    }

    /// Sets a report ready listener that gets called whenever there is a metrics report
    /// ready. Telemetry service will call the CbFunc and pass the listener object back to the
    /// client, along with the uuid of the metrics config for which the report is ready.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    /// @param report_ready_callback function closure gets called when a report is ready. The
    /// report_ready_callback takes String, String and i32 that specify
    /// metrics config uuid, metrics report name and report number of the report that is ready, respectively.
    ///
    /// @return an error code and error message in case of incorrect parameters

    pub async fn set_report_ready_listener(
        &self,
        report_ready_callback: impl Fn(String, String, i32) + Send + Sync + 'static,
    ) -> Result<(), (i32, String)> {
        metrics_report_manager::set_report_ready_listener(self, report_ready_callback).await
    }

    /// Clears a previously set report ready listener. Client can receive errors related to the
    /// params passed to the API call of the config if a status listener is added to the
    /// telemetry service.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    ///
    /// @return an error code and error message in case of incorrect parameters

    pub async fn clear_report_ready_listener(&self) -> Result<(), (i32, String)> {
        metrics_report_manager::clear_report_ready_listener(self).await
    }

    /// Tells the telemetry service to send a metrics report with the given uuid and report number.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    /// @param metrics_config_uuid uuid of the metrics configuration of the requested report
    /// @param report_config_name name of the metrics report config of the requested report
    /// @param report_number report number of the requested metrics report
    ///
    /// @return MetricsReport in case of success, an error code and error message in case of failure

    pub async fn get_metrics_report(
        &self,
        metrics_config_uuid: String,
        report_config_name: String,
        report_number: i32,
    ) -> Result<MetricsReport, (i32, String)> {
        metrics_report_manager::get_metrics_report(
            self,
            metrics_config_uuid,
            report_config_name,
            report_number,
        )
        .await
    }

    /// Sets a status listener that receives status codes and messages for misc errors (such as FILE IO errors) related
    /// to the telemetry service. API calls which do not take a listener and callback function
    /// also send their status back to the client using the status listener.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    /// @param status_callback function closure that gets called everytime a status catering to telemetry service needs to be shared with the client. The
    /// status_callback takes an i32 status_code and a String status_message as params.
    ///
    /// @return an error code and error message in case of incorrect parameters

    pub async fn set_status_listener(
        &self,
        status_callback: impl Fn(i32, String) + Send + Sync + 'static,
    ) -> Result<(), (i32, String)> {
        let listener = Box::new(StatusListener { status_callback: Arc::new(status_callback) });

        // SAFETY: `report_manager_raw` points to MetricsReportManager that is pinned
        // and thus will not move in memory, and remain a valid reference making the
        // function safe to call.
        unsafe {
            let raw_ptr = Box::into_raw(listener) as *mut c_void;
            let status_code = rust_to_c::set_status_listener(self.0, raw_ptr, Some(on_status));
            if status_code
                != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
            {
                // Releasing memory in case of API call failure
                let _ = Box::from_raw(raw_ptr as *mut StatusListener);
                error!("Error in setting status listener");
                return Err((status_code, "Invalid params".to_string()));
            }
        }

        Ok(())
    }

    /// Clears a previously set status listener.
    ///
    /// @param service An opaque pointer holding a reference to the telemetry_service
    ///
    /// @return an error code and error message in case of incorrect parameters

    pub async fn clear_status_listener(&self) -> Result<(), (i32, String)> {
        // SAFETY: safe to use as all pointers needed to satify safety requirements
        // for cb_func are passed.
        unsafe {
            rust_to_c::clear_status_listener(self.0);
        }
        Ok(())
    }
}
