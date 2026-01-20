// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module acts as a wrapper around C Client library for telemetry service

mod callbacks;
mod managers;
mod status;

pub use sdv_telemetry_status_codes_bindgen::TelemetryServiceStatusCodes;
pub use status::{
    AggregationPublisherErrorDetails, ConditionalTriggerErrorDetails, FileIOErrorDetails,
    MetricsReportGeneratorErrorDetails, ServiceErrorDetails, ServicePublisherErrorDetails,
    StatusDetails, TelemetryServiceStatus,
};

use crate::rust_to_c::ServiceHolder;
use callbacks::call_api_with_status_callback;
use log::info;
use sdv_telemetry_bindgen as rust_to_c;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use std::ffi::{c_void, CStr};

use managers::metrics_config_manager;
use managers::metrics_report_manager;

/// Represents an error with an error code and error message. The messages are not guaranteed to be
/// stable and may change or be removed at any point.
#[derive(thiserror::Error, Debug)]
#[error("{status:?}: {message}")]
pub struct TelemetryError {
    /// The error code.
    pub status: TelemetryServiceStatusCodes,
    /// The error message.
    pub message: String,
}

/// Represents the result of a telemetry service call.
pub type TelemetryResult<T> = Result<T, TelemetryError>;

/// Represents the callback argument of the report ready callback.
#[derive(Debug)]
pub struct ReportInfo {
    /// The metrics config UUID
    pub config_uuid: String,
    /// The metrics report UUID
    pub report_uuid: String,
}

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

/// Wraps a raw pointer and makes it `Send + Sync`.
#[repr(transparent)]
struct SendSyncPtr<T: Send + Sync>(*mut T);

/// Safety: Raw pointers are not `Send` and `Sync` by default mostly as more of a lint [1]. However,
/// in our case, we need to use a raw pointer across an `await` boundary, which requires it to be
/// `Send + Sync`. This is safe, because `SendSyncPtr` requires `T` to be `Send + Sync`, and because
/// dereferencing a raw pointer is `unsafe` anyway.
///
/// [1]: https://doc.rust-lang.org/nomicon/send-and-sync.html
unsafe impl<T: Send + Sync> Send for SendSyncPtr<T> {}

/// Safety: Raw pointers are not `Send` and `Sync` by default mostly as more of a lint [1]. However,
/// in our case, we need to use a raw pointer across an `await` boundary, which requires it to be
/// `Send + Sync`. This is safe, because `SendSyncPtr` requires `T` to be `Send + Sync`, and because
/// dereferencing a raw pointer is `unsafe` anyway.
///
/// [1]: https://doc.rust-lang.org/nomicon/send-and-sync.html
unsafe impl<T: Send + Sync> Sync for SendSyncPtr<T> {}

impl<T: Send + Sync> SendSyncPtr<T> {
    fn as_void_ptr(&self) -> *mut c_void {
        self.0 as *mut c_void
    }
}

fn get_aggregation_publisher_error_details(
    details: sdv_telemetry_bindgen::AggregationPublisherErrorDetails,
) -> status::StatusDetails {
    StatusDetails::AggregationPublisherErrorDetails(AggregationPublisherErrorDetails {
        // SAFETY: AggregationPublisherErrorDetails holds a pointer to metrics_config_uuid
        // that satisfies`CStr::from_ptr` safety requirements.
        metrics_config_uuid: unsafe { CStr::from_ptr(details.metrics_config_uuid) }
            .to_str()
            .unwrap()
            .to_string(),
        // SAFETY: AggregationPublisherErrorDetails holds a pointer to publisher_name
        // that satisfies`CStr::from_ptr` safety requirements.
        publisher_name: unsafe { CStr::from_ptr(details.publisher_name) }
            .to_str()
            .unwrap()
            .to_string(),
        // SAFETY: AggregationPublisherErrorDetails holds a pointer to field_name
        // that satisfies`CStr::from_ptr` safety requirements.
        field_name: unsafe { CStr::from_ptr(details.field_name) }.to_str().unwrap().to_string(),
    })
}

fn get_service_publisher_error_details(
    details: sdv_telemetry_bindgen::ServicePublisherErrorDetails,
) -> status::StatusDetails {
    StatusDetails::ServicePublisherErrorDetails(ServicePublisherErrorDetails {
        // SAFETY: ServicePublisherErrorDetails holds a pointer to metrics_config_uuid
        // that satisfies`CStr::from_ptr` safety requirements.
        metrics_config_uuid: unsafe { CStr::from_ptr(details.metrics_config_uuid) }
            .to_str()
            .unwrap()
            .to_string(),
        // SAFETY: ServicePublisherErrorDetails holds a pointer to publisher_name
        // that satisfies`CStr::from_ptr` safety requirements.
        publisher_name: unsafe { CStr::from_ptr(details.publisher_name) }
            .to_str()
            .unwrap()
            .to_string(),
    })
}

fn get_conditional_trigger_error_details(
    details: sdv_telemetry_bindgen::ConditionalTriggerErrorDetails,
) -> status::StatusDetails {
    StatusDetails::ConditionalTriggerErrorDetails(ConditionalTriggerErrorDetails {
        // SAFETY: ConditionalTriggerErrorDetails holds a pointer to metrics_config_uuid
        // that satisfies`CStr::from_ptr` safety requirements.
        metrics_config_uuid: unsafe { CStr::from_ptr(details.metrics_config_uuid) }
            .to_str()
            .unwrap()
            .to_string(),
        // SAFETY: ConditionalTriggerErrorDetails holds a pointer to trigger_name
        // that satisfies`CStr::from_ptr` safety requirements.
        trigger_name: unsafe { CStr::from_ptr(details.trigger_name) }.to_str().unwrap().to_string(),
    })
}

fn get_metrics_report_generator_error_details(
    details: sdv_telemetry_bindgen::MetricsReportGeneratorErrorDetails,
) -> status::StatusDetails {
    StatusDetails::MetricsReportGeneratorErrorDetails(MetricsReportGeneratorErrorDetails {
        // SAFETY: MetricsReportGeneratorErrorDetails holds a pointer to metrics_config_uuid
        // that satisfies`CStr::from_ptr` safety requirements.
        metrics_config_uuid: unsafe { CStr::from_ptr(details.metrics_config_uuid) }
            .to_str()
            .unwrap()
            .to_string(),
        // SAFETY: MetricsReportGeneratorErrorDetails holds a pointer to metrics_report_name
        // that satisfies`CStr::from_ptr` safety requirements.
        metrics_report_name: unsafe { CStr::from_ptr(details.metrics_report_name) }
            .to_str()
            .unwrap()
            .to_string(),
        // SAFETY: MetricsReportGeneratorErrorDetails holds a pointer to field_name
        // that satisfies`CStr::from_ptr` safety requirements.
        field_name: unsafe { CStr::from_ptr(details.field_name) }.to_str().unwrap().to_string(),
    })
}

fn get_service_error_details(
    details: sdv_telemetry_bindgen::ServiceErrorDetails,
) -> status::StatusDetails {
    StatusDetails::ServiceErrorDetails(ServiceErrorDetails {
        // SAFETY: ServiceErrorDetails holds a pointer to service_name
        // that satisfies`CStr::from_ptr` safety requirements.
        service_name: unsafe { CStr::from_ptr(details.service_name) }.to_str().unwrap().to_string(),
    })
}

fn get_file_io_error_details(
    details: sdv_telemetry_bindgen::FileIOErrorDetails,
) -> status::StatusDetails {
    StatusDetails::FileIOErrorDetails(FileIOErrorDetails {
        // SAFETY: FileIOErrorDetails holds a pointer to path
        // that satisfies`CStr::from_ptr` safety requirements.
        path: unsafe { CStr::from_ptr(details.path) }.to_str().unwrap().to_string(),
    })
}

fn get_status_details(
    details_with_tag: *const sdv_telemetry_bindgen::StatusDetailsWithTag,
) -> Option<status::StatusDetails> {
    // SAFETY: The caller has provided a valid pointer to `StatusDetailsWithTag`
    let details_with_tag = unsafe { *details_with_tag };

    match details_with_tag.tag {
        sdv_telemetry_bindgen::StatusDetailsTag_AggregationPublisherErrorDetailsTag => {
            // SAFETY: The caller has provided a valid tag to access details union field
            let details = unsafe { details_with_tag.details.aggregation_publisher_error_details };
            Some(get_aggregation_publisher_error_details(details))
        }

        sdv_telemetry_bindgen::StatusDetailsTag_ServicePublisherErrorDetailsTag => {
            // SAFETY: The caller has provided a valid tag to access details union field
            let details = unsafe { details_with_tag.details.service_publisher_error_details };
            Some(get_service_publisher_error_details(details))
        }

        sdv_telemetry_bindgen::StatusDetailsTag_ConditionalTriggerErrorDetailsTag => {
            // SAFETY: The caller has provided a valid tag to access details union field
            let details = unsafe { details_with_tag.details.conditional_trigger_error_details };
            Some(get_conditional_trigger_error_details(details))
        }

        sdv_telemetry_bindgen::StatusDetailsTag_MetricsReportGeneratorErrorDetailsTag => {
            // SAFETY: The caller has provided a valid tag to access details union field
            let details =
                unsafe { details_with_tag.details.metrics_report_generator_error_details };
            Some(get_metrics_report_generator_error_details(details))
        }

        sdv_telemetry_bindgen::StatusDetailsTag_ServiceErrorDetailsTag => {
            // SAFETY: The caller has provided a valid tag to access details union field
            let details = unsafe { details_with_tag.details.service_error_details };
            Some(get_service_error_details(details))
        }

        sdv_telemetry_bindgen::StatusDetailsTag_FileIOErrorDetailsTag => {
            // SAFETY: The caller has provided a valid tag to access details union field
            let details = unsafe { details_with_tag.details.file_io_error_details };
            Some(get_file_io_error_details(details))
        }
        _ => None,
    }
}

/// Callback function that receives status from telemetry service
/// # Safety
///
/// * `listener` points to a valid `StatusListener` originally sent to the caller as
///   part of the `set_misc_status_listener` API call.
///
/// * `status` points to a valid `TelemetryServiceStatus` instance.
unsafe extern "C" fn on_misc_status(
    listener: *mut std::ffi::c_void,
    status: *const sdv_telemetry_bindgen::TelemetryServiceStatus,
) {
    // SAFETY: The caller sends back the reference to `StatusListener` that was originally sent to
    // the caller as part of the `set_misc_status_listener` API call.
    //
    // We do not use `Box::from_raw` here, because we do not want to take ownership, so that future
    // calls to `on_misc_status` can still access the same `listener`.
    let status_listener = unsafe { &mut *(listener as *mut MiscStatusListener) };

    // SAFETY: The caller has provided a valid pointer to `TelemetryServiceStatus`
    let status = unsafe { *status };

    let id = status.id as usize;
    let code = TelemetryServiceStatusCodes(status.code);
    // SAFETY: The caller has provided a pointer that satisfies `CStr::from_ptr` safety
    // requirements.
    let message = unsafe { CStr::from_ptr(status.message) }.to_str().unwrap().to_string();
    let details = get_status_details(status.details);

    let status = TelemetryServiceStatus { id, code, message, details };

    info!("Status: {status:?}");
    (status_listener.callback)(status);
}

/// Struct used to wrap `callback` received from client.
struct MiscStatusListener {
    callback: Box<dyn Fn(TelemetryServiceStatus) + Send + Sync>,
}

impl MiscStatusListener {
    fn new(callback: Box<dyn Fn(TelemetryServiceStatus) + Send + Sync>) -> Self {
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
                status: TelemetryServiceStatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR,
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
    /// to telemetry service needs to be shared with the client. The `misc_status_callback` takes a
    /// `TelemetryServiceStatus` as a parameter.
    ///
    /// @param `report_ready_callback` function closure gets called when a report is ready. The
    /// `report_ready_callback` takes ReportInfo containing two Strings that specify metrics config
    /// uuid and metrics report uuid of the report that is ready.
    ///
    /// @return an error code and error message in case of incorrect parameters
    pub async fn init(
        &self,
        misc_status_callback: impl Fn(TelemetryServiceStatus) + Send + Sync + 'static,
        report_ready_callback: impl Fn(ReportInfo) + Send + Sync + 'static,
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
    pub async fn remove_all_metrics_configs(&self) -> TelemetryResult<String> {
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
    /// @param `report_ready_callback` function closure gets called when a report is ready. The
    /// `report_ready_callback` takes ReportInfo containing two Strings that specify metrics config
    /// uuid and metrics report uuid of the report that is ready.
    ///
    /// @return an error code and error message in case of incorrect parameters
    async fn set_report_ready_listener(
        &self,
        report_ready_callback: impl Fn(ReportInfo) + Send + Sync + 'static,
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
    /// @param report_uuid UUID of the requested report
    ///
    /// @return MetricsReport in case of success, an error code and error message in case of failure
    pub async fn get_metrics_report(&self, report_uuid: &str) -> TelemetryResult<MetricsReport> {
        metrics_report_manager::get_metrics_report(self, report_uuid).await
    }

    /// Tells the telemetry service to send info of the reports which are ready.
    ///
    /// @return in case of success, a list of ReportInfo objects containing metrics config and
    /// metrics report uuids, or an error code and error message in case of failure
    pub async fn get_ready_reports_info(&self) -> TelemetryResult<Vec<ReportInfo>> {
        metrics_report_manager::get_ready_reports_info(self).await
    }

    /// Sets a status listener that receives miscellaneous statuses (for example, in case of file
    /// IO errors) related to the telemetry service. API calls which do not take a listener and
    /// callback function also send their status back to the client using the status listener.
    ///
    /// @param `misc_status_callback` function closure that gets called every time a status catering
    /// to telemetry service needs to be shared with the client. The `misc_status_callback` takes a
    /// `TelemetryServiceStatus` as a parameter.
    /// @return an error code and error message in case of incorrect parameters
    async fn set_misc_status_listener(
        &self,
        misc_status_callback: impl Fn(TelemetryServiceStatus) + Send + Sync + 'static,
    ) -> TelemetryResult<String> {
        let raw_listener = SendSyncPtr(Box::into_raw(Box::new(MiscStatusListener::new(Box::new(
            misc_status_callback,
        )))));
        call_api_with_status_callback(|status_callback_data, status_callback| {
            let mut previous_raw_listener = std::ptr::null_mut::<MiscStatusListener>();

            // SAFETY: `self.0` points to a valid `ServiceHolder`. `raw_listener` points to a valid
            // `StatusListener` created using `Box::into_raw` and remains a valid reference in case
            // of successful `rust_to_c` API calls. In case of a failed API call, it is released to
            // avoid memory leaks. The output argument, `previous_raw_listener`, is a
            // null-initialized pointer to a `StatusListener`. A valid non-null pointer to this
            // pointer is passed to `set_misc_status_listener`. The validity of
            // `status_callback_data` and `status_callback` is ensured by
            // `call_api_with_status_callback`. This makes the function safe to call.
            unsafe {
                let status_code = rust_to_c::set_misc_status_listener(
                    self.0,
                    raw_listener.as_void_ptr(),
                    Some(on_misc_status),
                    status_callback_data,
                    status_callback,
                    (&mut previous_raw_listener) as *mut *mut _ as *mut *mut c_void,
                );
                if !previous_raw_listener.is_null() {
                    // Regardless of whether the call succeeded or failed, if
                    // `previous_raw_listener` is updated, take back ownership of it to reclaim
                    // memory.
                    let _ = Box::from_raw(previous_raw_listener);
                }
                status_code
            }
        })
        .await
        .inspect_err(|_| {
            // Releasing memory in case of API call failure.
            //
            //  SAFETY: safe to call since the `raw_listener.0` was created using `Box::into_raw`
            // above and it is guaranteed to not be used if by the C code if the API call fails.
            let _ = unsafe { Box::from_raw(raw_listener.0) };
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
        let status_code = TelemetryServiceStatusCodes(status_code);
        if status_code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
            return Err(TelemetryError {
                status: status_code,
                message: "Error while clearing misc status listener".to_owned(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use googletest::assert_that;
    use googletest::matchers::{all, eq, field, none, pat, some};
    use std::ffi::CString;

    const UUID: &str = "uuid";
    const PUBLISHER_NAME: &str = "publisher";
    const FIELD_NAME: &str = "field";
    const REPORT_NAME: &str = "report";
    const TRIGGER_NAME: &str = "trigger";
    const SERVICE_NAME: &str = "service";
    const PATH: &str = "/path/to/file";

    /// Compile-time check to verify that the passed argument implements `Send + Sync`.
    const fn assert_send_sync<T: Send + Sync>(t: T) -> T {
        t
    }

    /// Compile-time check to verify that all return types of all public functions are `Send +
    /// Sync`. This is especially important for the `async` public functions, because whether their
    /// returned `Future`s implement `Send` or `Sync` depends on their implementation (see
    /// b/353202112).
    ///
    /// This function is deliberately not run as part of a test, since its sole purpose is to verify
    /// the invariant described above.
    #[expect(unused)]
    fn check_all_function_results_are_send_sync() {
        let service = assert_send_sync(TelemetryServiceHolder::get()).unwrap();

        assert_send_sync(service.init(|_| {}, |_| {}));
        assert_send_sync(service.finish());

        assert_send_sync(service.set_report_ready_listener(|_| {}));
        assert_send_sync(service.clear_report_ready_listener());

        assert_send_sync(service.set_misc_status_listener(|_| {}));
        assert_send_sync(service.clear_misc_status_listener());

        assert_send_sync(service.add_metrics_config(&[]));
        assert_send_sync(service.remove_metrics_config(""));

        assert_send_sync(service.activate_metrics_config(""));
        assert_send_sync(service.deactivate_metrics_config(""));

        assert_send_sync(service.remove_all_metrics_configs());

        assert_send_sync(service.get_active_metrics_config_uuids());
        assert_send_sync(service.get_inactive_metrics_config_uuids());

        assert_send_sync(service.get_metrics_report(&String::default()));

        assert_send_sync(service.get_ready_reports_info());
    }

    #[test]
    fn test_get_status_details_aggregation_publisher_error() {
        let uuid = CString::new(UUID).unwrap();
        let publisher_name = CString::new(PUBLISHER_NAME).unwrap();
        let field_name = CString::new(FIELD_NAME).unwrap();
        let details = sdv_telemetry_bindgen::AggregationPublisherErrorDetails {
            metrics_config_uuid: uuid.as_ptr(),
            publisher_name: publisher_name.as_ptr(),
            field_name: field_name.as_ptr(),
        };
        let details_with_tag = sdv_telemetry_bindgen::StatusDetailsWithTag {
            tag: sdv_telemetry_bindgen::StatusDetailsTag_AggregationPublisherErrorDetailsTag,
            details: sdv_telemetry_bindgen::StatusDetails {
                aggregation_publisher_error_details: details,
            },
        };

        let result = get_status_details(&details_with_tag);
        assert_that!(
            result,
            some(pat!(StatusDetails::AggregationPublisherErrorDetails(all!(
                field!(AggregationPublisherErrorDetails.metrics_config_uuid, eq(UUID)),
                field!(AggregationPublisherErrorDetails.publisher_name, eq(PUBLISHER_NAME)),
                field!(AggregationPublisherErrorDetails.field_name, eq(FIELD_NAME))
            ))))
        );
    }

    #[test]
    fn test_get_status_details_service_publisher_error() {
        let uuid = CString::new(UUID).unwrap();
        let publisher_name = CString::new(PUBLISHER_NAME).unwrap();
        let details = sdv_telemetry_bindgen::ServicePublisherErrorDetails {
            metrics_config_uuid: uuid.as_ptr(),
            publisher_name: publisher_name.as_ptr(),
        };
        let details_with_tag = sdv_telemetry_bindgen::StatusDetailsWithTag {
            tag: sdv_telemetry_bindgen::StatusDetailsTag_ServicePublisherErrorDetailsTag,
            details: sdv_telemetry_bindgen::StatusDetails {
                service_publisher_error_details: details,
            },
        };

        let result = get_status_details(&details_with_tag);
        assert_that!(
            result,
            some(pat!(StatusDetails::ServicePublisherErrorDetails(all!(
                field!(ServicePublisherErrorDetails.metrics_config_uuid, eq(UUID)),
                field!(ServicePublisherErrorDetails.publisher_name, eq(PUBLISHER_NAME)),
            ))))
        );
    }

    #[test]
    fn test_get_status_details_conditional_trigger_error() {
        let uuid = CString::new(UUID).unwrap();
        let trigger_name = CString::new(TRIGGER_NAME).unwrap();
        let details = sdv_telemetry_bindgen::ConditionalTriggerErrorDetails {
            metrics_config_uuid: uuid.as_ptr(),
            trigger_name: trigger_name.as_ptr(),
        };
        let details_with_tag = sdv_telemetry_bindgen::StatusDetailsWithTag {
            tag: sdv_telemetry_bindgen::StatusDetailsTag_ConditionalTriggerErrorDetailsTag,
            details: sdv_telemetry_bindgen::StatusDetails {
                conditional_trigger_error_details: details,
            },
        };

        let result = get_status_details(&details_with_tag);
        assert_that!(
            result,
            some(pat!(StatusDetails::ConditionalTriggerErrorDetails(all!(
                field!(ConditionalTriggerErrorDetails.metrics_config_uuid, eq(UUID)),
                field!(ConditionalTriggerErrorDetails.trigger_name, eq(TRIGGER_NAME)),
            ))))
        );
    }

    #[test]
    fn test_get_status_details_metrics_report_generator_error() {
        let uuid = CString::new(UUID).unwrap();
        let report_name = CString::new(REPORT_NAME).unwrap();
        let field_name = CString::new(FIELD_NAME).unwrap();
        let details = sdv_telemetry_bindgen::MetricsReportGeneratorErrorDetails {
            metrics_config_uuid: uuid.as_ptr(),
            metrics_report_name: report_name.as_ptr(),
            field_name: field_name.as_ptr(),
        };
        let details_with_tag = sdv_telemetry_bindgen::StatusDetailsWithTag {
            tag: sdv_telemetry_bindgen::StatusDetailsTag_MetricsReportGeneratorErrorDetailsTag,
            details: sdv_telemetry_bindgen::StatusDetails {
                metrics_report_generator_error_details: details,
            },
        };

        let result = get_status_details(&details_with_tag);
        assert_that!(
            result,
            some(pat!(StatusDetails::MetricsReportGeneratorErrorDetails(all!(
                field!(MetricsReportGeneratorErrorDetails.metrics_config_uuid, eq(UUID)),
                field!(MetricsReportGeneratorErrorDetails.metrics_report_name, eq(REPORT_NAME)),
                field!(MetricsReportGeneratorErrorDetails.field_name, eq(FIELD_NAME)),
            ))))
        );
    }

    #[test]
    fn test_get_status_details_service_error() {
        let service_name = CString::new(SERVICE_NAME).unwrap();
        let details =
            sdv_telemetry_bindgen::ServiceErrorDetails { service_name: service_name.as_ptr() };
        let details_with_tag = sdv_telemetry_bindgen::StatusDetailsWithTag {
            tag: sdv_telemetry_bindgen::StatusDetailsTag_ServiceErrorDetailsTag,
            details: sdv_telemetry_bindgen::StatusDetails { service_error_details: details },
        };

        let result = get_status_details(&details_with_tag);
        assert_that!(
            result,
            some(pat!(StatusDetails::ServiceErrorDetails(all!(field!(
                ServiceErrorDetails.service_name,
                eq(SERVICE_NAME)
            )))))
        );
    }

    #[test]
    fn test_get_status_details_file_io_error() {
        let path = CString::new(PATH).unwrap();
        let details = sdv_telemetry_bindgen::FileIOErrorDetails { path: path.as_ptr() };
        let details_with_tag = sdv_telemetry_bindgen::StatusDetailsWithTag {
            tag: sdv_telemetry_bindgen::StatusDetailsTag_FileIOErrorDetailsTag,
            details: sdv_telemetry_bindgen::StatusDetails { file_io_error_details: details },
        };

        let result = get_status_details(&details_with_tag);
        assert_that!(
            result,
            some(pat!(StatusDetails::FileIOErrorDetails(all!(field!(
                FileIOErrorDetails.path,
                eq(PATH)
            )))))
        );
    }

    #[test]
    fn test_get_status_details_unknown_tag() {
        let details_with_tag = sdv_telemetry_bindgen::StatusDetailsWithTag {
            tag: sdv_telemetry_bindgen::StatusDetailsTag_UnknownDetailsTag,
            details: sdv_telemetry_bindgen::StatusDetails {
                unknown_details: sdv_telemetry_bindgen::UnknownDetails {},
            },
        };

        let result = get_status_details(&details_with_tag);
        assert_that!(result, none());
    }
}
