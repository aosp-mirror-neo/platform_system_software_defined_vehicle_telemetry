// Copyright 2023 Google LLC
//! This module contains descriptors for well known protobuf types.
use lazy_static::lazy_static;
use protobuf::{reflect::FileDescriptor, well_known_types};

lazy_static! {
    /// An array of file descriptors for well known protobuf types.
    pub static ref WELL_KNOWN_FILE_DESCRIPTORS: Vec<FileDescriptor> = vec![
        well_known_types::any::file_descriptor().clone(),
        well_known_types::api::file_descriptor().clone(),
        well_known_types::duration::file_descriptor().clone(),
        well_known_types::empty::file_descriptor().clone(),
        well_known_types::field_mask::file_descriptor().clone(),
        well_known_types::source_context::file_descriptor().clone(),
        well_known_types::struct_::file_descriptor().clone(),
        well_known_types::timestamp::file_descriptor().clone(),
        well_known_types::type_::file_descriptor().clone(),
        well_known_types::wrappers::file_descriptor().clone()
    ];
}
