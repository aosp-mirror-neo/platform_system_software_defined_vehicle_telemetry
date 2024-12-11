// Copyright 2023 Google LLC

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_LIB_H
#define TELEMETRY_CLIENTLIB_INCLUDE_LIB_H

#include <stddef.h>
#include <stdint.h>

// extern C functions should not be combined with namespaces, because extern "C" functions in
// different namespaces refer to the same entity and only one definition is allowed

#ifdef __cplusplus
extern "C" {
#endif

// Opaque Pointer holding an instance of the telemetry_service.
typedef struct ServiceHolder ServiceHolder;

// Information of telemetry metrics reports
typedef struct MetricsReportInfo {
    // uuid of the metrics configuration
    const char* metrics_config_uuid;

    // name of the metrics report config
    const char* report_config_name;

    // report number of the report
    const int32_t report_number;
} MetricsReportInfo;

/**
 * The status callback function relays the status of the API call back
 * to the client it was associated with.
 *
 * @param void* Pointer to the callback object associated with the callback function.
 * @param int32_t status code indicating the outcome of the API call
 * @param const char* status message adding more context to the outcome of the API call. These
 * status messages are not guaranteed to be stable and may change or be removed at any point.
 */
typedef void (*StatusCallbackFunction)(void*, int32_t, const char*);

/**
 * The report ready listener is called every time a new Metrics Report is ready to be downloaded.
 *
 * @param void* Pointer to the callback object associated with the callback function.
 * @param metrics_config_uuid uuid of the metrics configuration of the ready report.
 * @param report_config_name name of the metrics report config of the ready report.
 * @param report_number report number of the ready metrics report.
 *
 */
typedef void (*ReportReadyCallbackFunction)(void*, const char*, const char*, int32_t);

/**
 * The get report callback function relays the status of the get_metrics_report API call
 * If the API call was successful it also sends the serialized metrics report back to
 * the client
 *
 * @param void* Pointer to the callback object associated with the callback function.
 * @param int32_t status code indicating the outcome of the API call
 * @param const char* status message adding more context to the outcome of the API call.
 * @param const uint8_t* pointer to the serialized byte array pointing to the metrics report.
 * Null if the API call failed
 * @param size_t size of the metrics report byte array.
 *
 */
typedef void (*GetSerializedDataCallbackFunction)(void*, int, const char*, const uint8_t*, size_t);

/**
 * The get ready reports info callback function relays the status of the get_ready_reports_info API
 call.
 * If the API call was successful it also sends info of the ready reports.
 *
 * @param void* Pointer to the callback object associated with the callback function.
 * @param int32_t status code indicating the outcome of the API call
 * @param const char* status message adding more context to the outcome of the API call.
 * @param const MetricsReportInfo* pointer to the array of MetricsReportInfo containing info of
   ready metrics reports.
 * @param size_t number of ready reports.
 *
 */
typedef void (*ReadyReportsInfoCallbackFunction)(void*, int, const char*, const MetricsReportInfo*,
                                                 size_t);

/**
 * The get config list callback function relays the status of the
 * get_(active/inactive)_metrics_config API call. If the API call was successful it also sends the
 * list of config uuids as a char**
 *
 * @param void* Pointer to the callback object associated with the callback function.
 * @param int32_t status code indicating the outcome of the API call
 * @param const char* status message adding more context to the outcome of the API call.
 * @param const char** pointer to the array of strings containing config uuids
 * Null if the API call failed
 * @param size_t size of the string array
 *
 */
typedef void (*ConfigListCallbackFunction)(void*, int, const char*, const char**, size_t);

/**
 * Gets an instance of telemetry_service and type casts it to an opaque
 * pointer. This is a blocking call and it will wait until the telemetry binder service
 * is ready. The returned value can be null in case of an error, such as when the telemetry binder
 * service cannot be found, or when status listeners for it cannot be registered.
 *
 * This function is thread safe.
 *
 * @return an opaque pointer holding a reference to the telemetry_service
 */
ServiceHolder* get_telemetry_service();

/**
 * Adds a metrics config to the telemetry service passed as a byte array. Any metrics
 * config added is inactive by default and needs to be activated explicitly. Client can receive
 * errors related to the config if a status listener is added to the telemetry service.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param metrics_config Pointer to the serialized metrics config
 * @param size Size of the serialized metrics config
 * @param listener pointer to listener passed back to client when callback function is triggered
 * @param cbFunc pointer to callback function. callback function is called to share the status of
 * the API call
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t add_metrics_config(ServiceHolder* holder, uint8_t* metrics_config, size_t size,
                           void* listener, StatusCallbackFunction cbFunc);

/**
 * Activates a previously added metrics config or a deactivated metrics config.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param uuid UUID of the metrics config that needs to be activated.
 * @param listener pointer to listener passed back to client when callback function is triggered
 * @param cbFunc pointer to callback function. callback function is called to share the status of
 * the API call
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t activate_metrics_config(ServiceHolder* holder, const char* uuid, void* listener,
                                StatusCallbackFunction cbFunc);

/**
 * Deactivates an active metrics config. Client can receive errors related to the
 * params passed to the API call of the config if a status listener is added to the
 * telemetry service.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param uuid UUID of the metrics config that needs to be deactivated.
 * @param listener pointer to listener passed back to client when callback function is triggered
 * @param cbFunc pointer to callback function. callback function is called to share the status of
 * the API call
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t deactivate_metrics_config(ServiceHolder* holder, const char* uuid, void* listener,
                                  StatusCallbackFunction cbFunc);

/**
 * Retrieves a list of metrics configs currently in the active state from telemetry service
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param listener Reference to config list listener object.
 * @param cbFunc pointer to callback function. Callback function is used to send the list of active
 * configs as a byte array along with the size of the list, and the config list listener object
 * passed.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t get_active_metrics_config_uuids(ServiceHolder* holder, void* listener,
                                        ConfigListCallbackFunction cbFunc);

/**
 * Retrieves a list of metrics configs currently in the inactive state from telemetry service
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 *
 * @param listener Reference to config list listener object.
 * @param cbFunc pointer to callback function. Callback function is used to send the list of active
 * configs as a byte array along with the size of the list, and the config list listener object
 * passed.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t get_inactive_metrics_config_uuids(ServiceHolder* holder, void* listener,
                                          ConfigListCallbackFunction cbFunc);

/**
 * Removes a previously added metrics config. The config being removed will be
 * deactivated if it's in active state before being removed. Client can receive
 * errors related to the params passed to the API call of the config if a status listener is
 * added to the telemetry service.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param uuid UUID of the metrics config that needs to be removed.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t remove_metrics_config(ServiceHolder* holder, const char* uuid, void* listener,
                              StatusCallbackFunction cbFunc);

/**
 * Removes all previously added metrics config. The configs be deactivated
 * if it's in active state before being removed. Client can receive
 * errors related to the params passed to the API call of the config if a status listener is
 * added to the telemetry service.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param listener pointer to listener passed back to client when callback function is triggered
 * @param cbFunc pointer to callback function. callback function is called to share the status of
 * the API call
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t remove_all_metrics_configs(ServiceHolder* holder, void* listener,
                                   StatusCallbackFunction cbFunc);

/**
 * Sets a report ready listener that gets called whenever there is a metrics report
 * ready. telemetry service will call the CbFunc and pass the listener object back to the
 * client, along with the uuid of the metrics config for which the report is ready.
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param reportListener Reference to report ready listener object.
 * @param reportReadyCbFunc Function pointer for the callback function that gets called when a
 * report is ready. The function takes a void pointer that points to the report listener sent to the
 * telemetry service by the client app, a const char* uuid, a const char* name and a int32_t that
 * specify metrics config, metrics report name and report number of the report that is ready,
 * respectively.
 *
 * @param apiListener pointer to listener passed back to client when api callback function is
 * triggered
 * @param apiCbFunc pointer to callback function. callback function is called to share the status of
 * the API call

 * @param outPreviousReportListener Reference to a pointer that will be set to point to the
 * `reportListener` set in a previous call to `set_report_ready_listener`, if any. Will be ignored
 * if it is `nullptr`.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t set_report_ready_listener(ServiceHolder* holder, void* reportListener,
                                  ReportReadyCallbackFunction reportReadyCbFunc, void* apiListener,
                                  StatusCallbackFunction apiCbFunc,
                                  void** outPreviousReportListener);

/**
 * Clears a previously set report ready listener. Client can receive errors related to the
 * params passed to the API call of the config if a status listener is added to the
 * telemetry service.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 *
 * @param outPreviousReportListener Reference to a pointer that will be set to point to the
 * `reportListener` set in a previous call to `set_report_ready_listener`, if any. Will be ignored
 * if it is `nullptr`.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t clear_report_ready_listener(ServiceHolder* holder, void** outPreviousReportListener);

/**
 * Tells the telemetry service to send a metrics report with the given uuid and report number.
 * The report if found is sent back to the client using the callback function, along with the
 * report listener object. If report is not found, an error is sent to the status listener.
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param metrics_config_uuid uuid of the metrics configuration of the requested report
 * @param report_config_name name of the metrics report config of the requested report
 * @param report_number report number of the requested metrics report
 *
 * @param listener Reference to report listener object.
 * @param cbFunc pointer to callback function. Callback function is used to send the metrics report
 * as a byte array along with the size of the report, and the report listener object passed.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t get_metrics_report(ServiceHolder* holder, const char* metrics_config_uuid,
                           const char* report_config_name, int32_t report_number, void* listener,
                           GetSerializedDataCallbackFunction cbFunc);

/**
 * Tells the telemetry service to send information of metrics reports which are ready.
 * The IDs are sent back to the client using the callback function, along with the
 * ready reports info listener object.
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service.
 * @param listener Reference to ready reports info listener object.
 * @param cbFunc pointer to callback function. Callback function is used to send the IDs of metrics
 * report.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t get_ready_reports_info(ServiceHolder* holder, void* listener,
                               ReadyReportsInfoCallbackFunction cbFunc);

/**
 * Sets a status listener that receives status codes and messages for misc errors related
 * to the telemetry service.
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param status_listener pointer to listener passed back to client when the misc status callback
 * function is triggered
 * @param statusCbFunc Function pointer for the cbFunc that is set the misc status listener in
 * telemetry service and is called to send back misc status codes back to the client
 *
 * @param api_listener pointer to listener passed back to the rust wrapper when api status callback
 * function is triggered
 * @param apiCbFunc pointer to api status callback function. Callback function is called by
 * telemetry serviceto share the status of the current API call
 *
 * @param outPreviousStatusListener Reference to a pointer that will be set to point to the
 * `status_listener` set in a previous call to `set_misc_status_listener`, if any. Will be ignored
 * if it is `nullptr`.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t set_misc_status_listener(ServiceHolder* holder, void* status_listener,
                                 StatusCallbackFunction statusCbFunc, void* api_listener,
                                 StatusCallbackFunction apiCbFunc,
                                 void** outPreviousStatusListener);

/**
 * Clears a previously set status listener.
 *
 * TODO: b/298501354: Handle case when status listener is not set and errors need
 * to be shared with client.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param outPreviousStatusListener Reference to a pointer that will be set to point to the
 * `status_listener` set in a previous call to `set_misc_status_listener`, if any. Will be ignored
 * if it is `nullptr`.
 *
 * @return an integer status code that validates the parameters provided to the API
 */
int32_t clear_misc_status_listener(ServiceHolder* holder, void** outPreviousStatusListener);

#ifdef __cplusplus
}
#endif

#endif // TELEMETRY_CLIENTLIB_INCLUDE_LIB_H
