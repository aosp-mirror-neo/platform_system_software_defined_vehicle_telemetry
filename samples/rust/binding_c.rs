// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(dead_code)]

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ServiceHolder {
    _unused: [u8; 0],
}

extern "C" {
    pub fn start_telemetry_service() -> *mut ServiceHolder;
    pub fn stop_telemetry_service(holder: *mut ServiceHolder);
    pub fn addMetricsConfig(holder: *mut ServiceHolder, metrics_config: *mut u8, size: usize);
    pub fn removeMetricsConfig(holder: *mut ServiceHolder, uuid: *const ::std::os::raw::c_char);
    pub fn removeAllMetricsConfigs(holder: *mut ServiceHolder);

    pub fn setReportReadyListener(
        holder: *mut ServiceHolder,
        listener: *mut ::std::os::raw::c_void,
        cbFunc: ::std::option::Option<
            unsafe extern "C" fn(
                arg1: *mut ::std::os::raw::c_void,
                arg2: *const ::std::os::raw::c_char,
                arg3: usize,
            ),
        >,
    );
}
