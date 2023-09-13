// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(dead_code)]

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ServiceHolder {
    _unused: [u8; 0],
}

extern "C" {
    pub fn get_telemetry_service() -> *mut ServiceHolder;
    pub fn stop_telemetry_service(holder: *mut ServiceHolder);
    pub fn add_metrics_config(holder: *mut ServiceHolder, metrics_config: *mut u8, size: usize);
    pub fn activate_metrics_config(holder: *mut ServiceHolder, uuid: *const ::std::os::raw::c_char);
    pub fn deactivate_metrics_config(
        holder: *mut ServiceHolder,
        uuid: *const ::std::os::raw::c_char,
    );
    pub fn remove_metrics_config(holder: *mut ServiceHolder, uuid: *const ::std::os::raw::c_char);
    pub fn remove_all_metrics_configs(holder: *mut ServiceHolder);

    pub fn set_report_ready_listener(
        holder: *mut ServiceHolder,
        listener: *mut ::std::os::raw::c_void,
        cbFunc: ::std::option::Option<
            unsafe extern "C" fn(
                arg1: *mut ::std::os::raw::c_void,
                arg2: *const ::std::os::raw::c_char,
                arg3: ::std::os::raw::c_int,
            ),
        >,
    );

    pub fn clear_report_ready_listener(holder: *mut ServiceHolder);

    pub fn set_status_listener(
        holder: *mut ServiceHolder,
        cbFunc: ::std::option::Option<
            unsafe extern "C" fn(arg1: ::std::os::raw::c_int, arg2: *const ::std::os::raw::c_char),
        >,
    );

    pub fn clear_status_listener(holder: *mut ServiceHolder);

}
