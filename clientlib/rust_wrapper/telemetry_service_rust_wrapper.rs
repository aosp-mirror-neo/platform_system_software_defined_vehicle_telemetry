// Copyright 2023 Google LLC. All Rights Reserved.

//! This module acts as a wrapper around C Client library for telemetry service

mod managers;

use crate::rust_to_c::ServiceHolder;
use log::{error, info};
use sdv_telemetry_bindgen as rust_to_c;
use sdv_telemetry_status_codes_bindgen as status_codes;
use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;
pub use status_codes::*;
use std::ffi::{c_void, CStr};
use tokio::sync::oneshot;

use managers::metrics_config_manager;
use managers::metrics_report_manager;

/// Represents an error with an error code and error message.
#[derive(thiserror::Error, Debug)]
#[error("{status}: {message}")]
pub struct TelemetryError {
    status: i32,
    message: String,
}

/// Represents the result of a telemetry service call.
pub type TelemetryResult<T> = Result<T, TelemetryError>;

/// Struct used to wrap ServiceHolder received from C in a typesafe manner
#[repr(transparent)]
pub struct TelemetryServiceHolder(*mut ServiceHolder);

/// Safety:
///
/// This implementation allows the `TelemetryServiceHolder` to be safely sent (transferred) between threads.
/// The underlying C `ServiceHolder` is designed for thread-safe ownership transfer, meaning that
/// transferring it between threads will not lead to any undefined behavior or data races.
unsafe impl Send for TelemetryServiceHolder {}

/// Safety:
///
/// This implementation allows the `TelemetryServiceHolder` to be safely shared (referenced) across threads.
/// The underlying C `ServiceHolder` is designed for thread-safe shared access. This implies that
/// concurrent read operations on the `ServiceHolder` from multiple threads are well-defined and will not result in
/// data races or undefined behavior.
unsafe impl Sync for TelemetryServiceHolder {}

/// Callback that receives status for API calls.
///
/// # Safety
///
/// * `status_msg` must point to a C raw string that satisfies `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the add_metrics_config api call. Listener will not be valid after the function completes
pub unsafe extern "C" fn api_callback(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender =
        unsafe { Box::from_raw(listener as *mut oneshot::Sender<TelemetryResult<String>>) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe { CStr::from_ptr(status_msg) }.to_str().unwrap().to_owned();

    if status_code == status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS {
        sender.send(Ok(msg)).unwrap();
    } else {
        sender.send(Err(TelemetryError { status: status_code, message: msg })).unwrap();
    }
}

/// Callback function that receives status from telemetry service
///
/// # Safety
///
/// * `status_msg` must point to a C raw string that satisfies `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `StatusListener` originally sent to the caller as
///    part of the `set_misc_status_listener` API call.
unsafe extern "C" fn on_status(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: The caller sends back the reference to `StatusListener` that was
    // originally sent to the caller during as part of the `set_misc_status_listener` API call.
    //
    // We do not use `Box::from_raw` here, because we do not want to take ownership, so that future
    // calls to `on_status` can still access the same `listener`.
    let status_listener = unsafe { &mut *(listener as *mut MiscStatusListener) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let status_msg = unsafe { CStr::from_ptr(status_msg).to_str().unwrap().to_string() };

    info!("Status code: {} status message: {}", status_code, status_msg);

    (status_listener.callback)(status_code, status_msg);
}

/// Struct used to wrap `callback` received from client.
struct MiscStatusListener {
    callback: Box<dyn Fn(i32, String) + Send + Sync>,
}

impl MiscStatusListener {
    fn new(callback: Box<dyn Fn(i32, String) + Send + Sync>) -> Self {
        Self { callback }
    }
}

impl TelemetryServiceHolder {
    /// Gets an instance of telemetry_service and type casts it to an opaque
    /// pointer. This is a blocking call and it will wait until the telemetry binder service
    /// is ready. The returned value will never be null, and will always hold an instance
    /// of telemetry service. The telemetry service supports only a single instance of the service
    /// holder. Calling this method more than once may result in undefined or unexpected behavior.
    ///
    /// @return an opaque pointer holding a reference to the telemetry_service
    pub fn get() -> TelemetryResult<TelemetryServiceHolder> {
        info!("Connecting to service");

        // SAFETY: Safe to use as calling `get_telemetry_service` is always safe.
        let service = unsafe { rust_to_c::get_telemetry_service() };
        if service.is_null() {
            return Err(TelemetryError {
                status: status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_UNKNOWN_ERROR,
                message: "Unable to get telemetry service".to_owned(),
            });
        }
        Ok(TelemetryServiceHolder(service))
    }

    /// Initializes telemetry service by calling the set methods for the misc status callback and
    /// the report ready listener. This prevents the user from registering the callbacks multiple
    /// times.
    ///
    /// This method must be called only once at the start of the application.
    ///
    /// @param `misc_status_callback` function closure that gets called every time a status catering
    /// to telemetry service needs to be shared with the client. The `misc_status_callback` takes an
    /// i32 status_code and a String status_message as params.
    ///
    /// @param `report_ready_callback` function closure gets called when a report is ready. The
    /// `report_ready_callback` takes String, String and i32 that specify metrics config uuid,
    /// metrics report name and report number of the report that is ready, respectively.
    ///
    /// @return an error code and error message in case of incorrect parameters
    pub async fn init(
        &self,
        misc_status_callback: impl Fn(i32, String) + Send + Sync + 'static,
        report_ready_callback: impl Fn(String, String, i32) + Send + Sync + 'static,
    ) -> TelemetryResult<()> {
        info!("Initializing telemetry service");

        self.set_misc_status_listener(misc_status_callback).await?;
        self.set_report_ready_listener(report_ready_callback).await?;

        Ok(())
    }

    /// Finishes telemetry service by calling the clear methods for the misc status callback and
    /// the report ready listener.
    ///
    /// This method must be called only once at the end of the application.
    ///
    /// @return an error code and error message in case of failure to remove listeners
    pub fn finish(&self) -> TelemetryResult<()> {
        info!("Finishing telemetry service");

        self.clear_misc_status_listener()?;
        self.clear_report_ready_listener()?;

        Ok(())
    }

    /// Adds a metrics config to the telemetry service passed as a byte array. Any metrics
    /// config added is inactive by default and needs to be activated explicitly. Client can receive
    /// errors related to the config if a status listener is added to the telemetry service.
    ///
    /// @param serialized_config Pointer to the serialized metrics config
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure
    pub async fn add_metrics_config(&self, serialized_config: &[u8]) -> TelemetryResult<String> {
        metrics_config_manager::add_metrics_config(self, serialized_config).await
    }

    /// Activates a previously added metrics config or a deactivated metrics config.
    ///
    /// @param config_uuid UUID of the metrics config that needs to be activated.
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure
    pub async fn activate_metrics_config(&self, config_uuid: &str) -> TelemetryResult<String> {
        metrics_config_manager::activate_metrics_config(self, config_uuid).await
    }

    /// Deactivates an active metrics config.
    ///
    /// @param config_uuid UUID of the metrics config that needs to be deactivated.
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure
    pub async fn deactivate_metrics_config(&self, config_uuid: &str) -> TelemetryResult<String> {
        metrics_config_manager::deactivate_metrics_config(self, config_uuid).await
    }

    /// Removes a previously added metrics config. The config being removed will be
    /// deactivated if it's in active state before being removed.
    ///
    /// @param config_uuid UUID of the metrics config that needs to be removed.
    ///
    /// @return metrics config uuid in case of success, and a error code and error message in case of failure
    pub async fn remove_metrics_config(&self, config_uuid: &str) -> TelemetryResult<String> {
        metrics_config_manager::remove_metrics_config(self, config_uuid).await
    }

    /// Removes all previously added metrics config. The configs be deactivated
    /// if it's in active state before being removed.
    ///
    ///
    /// @return an error code and error message in case of failure
    pub async fn remove_all_metrics_configs(&self) -> TelemetryResult<()> {
        metrics_config_manager::remove_all_metrics_configs(self).await
    }

    /// Returns a list of all metrics configs currently in the active state in telemetry service
    ///
    ///
    /// @return s vector of strings containing the requested list
    pub async fn get_active_metrics_config_uuids(&self) -> TelemetryResult<Vec<String>> {
        metrics_config_manager::get_active_metrics_config_uuids(self).await
    }

    /// Returns a list of all metrics configs currently in the inactive state in telemetry service
    ///
    ///
    /// @return a vector of strings containing the requested list
    pub async fn get_inactive_metrics_config_uuids(&self) -> TelemetryResult<Vec<String>> {
        metrics_config_manager::get_inactive_metrics_config_uuids(self).await
    }

    /// Sets a report ready listener that gets called whenever there is a metrics report
    /// ready. Telemetry service will call the CbFunc and pass the listener object back to the
    /// client, along with the uuid of the metrics config for which the report is ready.
    ///
    /// @param report_ready_callback function closure gets called when a report is ready. The
    /// report_ready_callback takes String, String and i32 that specify
    /// metrics config uuid, metrics report name and report number of the report that is ready, respectively.
    ///
    /// @return an error code and error message in case of incorrect parameters
    async fn set_report_ready_listener(
        &self,
        report_ready_callback: impl Fn(String, String, i32) + Send + Sync + 'static,
    ) -> TelemetryResult<String> {
        metrics_report_manager::set_report_ready_listener(self, report_ready_callback).await
    }

    /// Clears a previously set report ready listener. Client can receive errors related to the
    /// params passed to the API call of the config if a status listener is added to the
    /// telemetry service.
    ///
    ///
    /// @return an error code and error message in case of incorrect parameters
    fn clear_report_ready_listener(&self) -> TelemetryResult<()> {
        metrics_report_manager::clear_report_ready_listener(self)
    }

    /// Tells the telemetry service to send a metrics report with the given uuid and report number.
    ///
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
    ) -> TelemetryResult<MetricsReport> {
        metrics_report_manager::get_metrics_report(
            self,
            metrics_config_uuid,
            report_config_name,
            report_number,
        )
        .await
    }

    /// Sets a status listener that receives status codes and messages for misc errors (such as file
    /// IO errors) related to the telemetry service. API calls which do not take a listener and
    /// callback function also send their status back to the client using the status listener.
    ///
    /// @param `misc_status_callback` function closure that gets called every time a status catering
    /// to telemetry service needs to be shared with the client. The `misc_status_callback` takes an
    /// i32 status_code and a String status_message as params.
    ///
    /// @return an error code and error message in case of incorrect parameters
    async fn set_misc_status_listener(
        &self,
        misc_status_callback: impl Fn(i32, String) + Send + Sync + 'static,
    ) -> TelemetryResult<String> {
        let listener = MiscStatusListener::new(Box::new(misc_status_callback));

        let (set_listener_sender, set_listener_receiver) =
            oneshot::channel::<TelemetryResult<String>>();

        let raw_listener: *mut MiscStatusListener = Box::into_raw(Box::new(listener));
        let raw_sender = Box::into_raw(Box::new(set_listener_sender));
        let mut previous_raw_listener = std::ptr::null_mut::<MiscStatusListener>();
        // SAFETY: `self.0` points to a valid `ServiceHolder`. `raw_listener` points to a valid
        // `MiscStatusListener` and `raw_sender` points to a valid `oneshot::Sender`, both of
        // which are created using `Box::into_raw` and remain a valid reference in case of
        // successful `rust_to_c` API calls. In case of a failed API call, both are
        // released to avoid memory leaks. The output argument, `previous_raw_listener`, is a
        // null-initialized pointer to a `MiscStatusListener`. A valid non-null pointer to this
        // pointer is passed to `set_misc_status_listener`. This makes the function safe to call.

        unsafe {
            let status_code = rust_to_c::set_misc_status_listener(
                self.0,
                raw_listener as *mut c_void,
                Some(on_status),
                raw_sender as *mut c_void,
                Some(api_callback),
                (&mut previous_raw_listener) as *mut *mut _ as *mut *mut c_void,
            );
            if !previous_raw_listener.is_null() {
                // Regardless of whether the call succeeded or failed, if `previous_raw_listener` is
                // updated, take back ownership of it to reclaim memory.
                let _ = Box::from_raw(previous_raw_listener);
            }
            if status_code
                != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
            {
                // Releasing memory in case of API call failure
                let _ = Box::from_raw(raw_listener);
                let _ = Box::from_raw(raw_sender);
                error!("Error while setting status listener: {status_code}");
                return Err(TelemetryError {
                    status: status_code,
                    message: "Invalid params".to_owned(),
                });
            }
        };

        set_listener_receiver.await.unwrap().inspect_err(|_| {
            // Releasing memory in case of API call failure
            // SAFETY: safe to call since the caller is responsible managing raw_listener's memory
            // raw_listener was created using Box::into_raw above.
            let _ = unsafe { Box::from_raw(raw_listener) };
        })
    }

    /// Clears a previously set status listener.
    ///
    ///
    /// @return an error code and error message in case of failure to remove listener
    fn clear_misc_status_listener(&self) -> TelemetryResult<()> {
        let mut previous_raw_listener = std::ptr::null_mut::<MiscStatusListener>();
        // SAFETY: `self.0` points to a valid `ServiceHolder`. The output argument,
        // `previous_raw_listener`, is a null-initialized pointer to a `MiscStatusListener`. A
        // valid non-null pointer to this pointer is passed to `set_misc_status_listener`. This makes
        // the function safe to call.
        let status_code = unsafe {
            rust_to_c::clear_misc_status_listener(
                self.0,
                (&mut previous_raw_listener) as *mut *mut _ as *mut *mut c_void,
            )
        };
        if !previous_raw_listener.is_null() {
            // SAFETY: If `clear_misc_status_listener` sets this pointer to a non-null value, it is
            // guaranteed to point to a `MiscStatusListener` set in a previous call to
            // `set_misc_status_listener`. Therefore, it is safe to reclaim the memory.
            let _ = unsafe { Box::from_raw(previous_raw_listener) };
        }
        if status_code
            != status_codes::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_API_CALL_SUCCESS
        {
            error!("Error while clearing misc status listener: {status_code}");
            return Err(TelemetryError {
                status: status_code,
                message: "Error while clearing misc status listener".to_owned(),
            });
        }

        Ok(())
    }
}
