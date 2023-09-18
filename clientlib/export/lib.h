// Copyright 2023 Google LLC. All Rights Reserved.

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

/**
 * Gets an instance of telemetry_service and type casts it to an opaque
 * pointer. This is a blocking call and it will wait until the telemetry binder service
 * is ready. The returned value will never be null, and will always hold an instance
 * of telemetry service.
 *
 * @return an opaque pointer holding a reference to the telemetry_service
 */
ServiceHolder* get_telemetry_service();

/**
 * Disconnects the telemetry service binder connection held by ServiceHolder, and tells the
 * telemetry binder service to release all resources to free up memory and cpu. A stopped
 * service can be started again by calling get_telemetry_service defined above.
 * TODO: b/298456137: Trigger release of resources in telemetry binder service when
 * stop telemetry service is called.
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 */
void stop_telemetry_service(ServiceHolder* holder);

/**
 * Adds a metrics config to the telemetry service passed as a byte array. Any metrics
 * config added is inactive by default and needs to be activated explicitly. Client can receive
 * errors related to the config if a status listener is added to the telemetry service.
 * TODO: b/296147474: Exhaustive list of all status codes to be shared with the OEM
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param metrics_config Pointer to the serialized metrics config
 * @param size Size of the serialized metrics config
 */
void add_metrics_config(ServiceHolder* holder, uint8_t* metrics_config, size_t size);

/**
 * Activates a previously added metrics config or a deactivated metrics config. Client can
 * receive errors related to the params passed to the API call of the config if a status
 * listener is added to the telemetry service.
 * TODO: b/296147474: Exhaustive list of all status codes to be shared with the OEM
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param uuid UUID of the metrics config that needs to be activated.
 */
void activate_metrics_config(ServiceHolder* holder, const char* uuid);

/**
 * Deactivates an active metrics config. Client can receive errors related to the
 * params passed to the API call of the config if a status listener is added to the
 * telemetry service.
 * TODO: b/296147474: Exhaustive list of all status codes to be shared with the OEM
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param uuid UUID of the metrics config that needs to be deactivated.
 */
void deactivate_metrics_config(ServiceHolder* holder, const char* uuid);

/**
 * Removes a previously added metrics config. The config being removed will be
 * deactivated if it's in active state before being removed. Client can receive
 * errors related to the params passed to the API call of the config if a status listener is
 * added to the telemetry service.
 * TODO: b/296147474: Exhaustive list of all status codes to be shared with the OEM
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param uuid UUID of the metrics config that needs to be removed.
 */
void remove_metrics_config(ServiceHolder* holder, const char* uuid);

/**
 * Removes all previously added metrics config. The configs be deactivated
 * if it's in active state before being removed. Client can receive
 * errors related to the params passed to the API call of the config if a status listener is
 * added to the telemetry service.
 * TODO: b/296147474: Exhaustive list of all status codes to be shared with the OEM
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 */
void remove_all_metrics_configs(ServiceHolder* holder);

/**
 * Sets a report ready listener that gets called whenever there is a metrics report
 * ready. telemetry service will call the CbFunc and pass the listener object back to the
 * client, along with the uuid of the metrics config for which the report is ready.
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param listener Reference to report ready listener object.
 * @param cbFunc Function pointer for the cbFunc that gets called when a report is ready. The
 * cbFunc takes a void pointer which points to the report listener sent to the telemetry
 * service by the client app and a const char* uuid which identifies the metrics config that
 * produced a report as params.
 */
void set_report_ready_listener(ServiceHolder* holder, void* listener,
                               void (*cbFunc)(void*, const char*, int32_t));

/**
 * Clears a previously set report ready listener. Client can receive errors related to the
 * params passed to the API call of the config if a status listener is added to the
 * telemetry service.
 * TODO: b/296147474: Exhaustive list of all status codes to be shared with the OEM
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 *
 */
void clear_report_ready_listener(ServiceHolder* holder);

/**
 * Sets a status listener that receives status codes and messages for all API calls
 * sent to the telemetry service. No statuses will be available to the client unless
 * the status listener is set.
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 * @param cbFunc Function pointer for the cbFunc that gets called when after an API call. The
 * cbFunc takes an int status_code and a const char* status_message as params.
 */
void set_status_listener(ServiceHolder* holder, void (*cbFunc)(int, const char*));

/**
 * Clears a previously set status listener.
 *
 * TODO: b/298501354: Handle case when status listener is not set and errors need
 * to be shared with client.
 * TODO: b/298456859: Handle case when ServiceHolder is null or invalid
 * TODO: b/298457300: Handle cases when API calls are made after service is stopped
 *
 * @param holder An opaque pointer holding a reference to the telemetry_service
 *
 */
void clear_status_listener(ServiceHolder* holder);

#ifdef __cplusplus
}
#endif

#endif // TELEMETRY_CLIENTLIB_INCLUDE_LIB_H
