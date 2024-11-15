// Copyright 2023 Google LLC
//! This module contains descriptors for well known protobuf types.
use lazy_static::lazy_static;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types;
use std::borrow::Cow;

lazy_static! {
    /// An array of file descriptors for well known protobuf types.
    pub static ref WELL_KNOWN_FILE_DESCRIPTORS: Vec<FileDescriptor> = vec![
        protobuf::descriptor::file_descriptor().clone(),
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

/// Finds a message descriptor by its full name.
pub fn find_message_by_full_name(
    file_descriptors: &[FileDescriptor],
    full_name: &str,
) -> Option<MessageDescriptor> {
    let full_name = if full_name.starts_with('.') {
        Cow::Borrowed(full_name)
    } else {
        Cow::Owned(format!(".{}", full_name))
    };

    // iterate through all file_descriptors, find MessageDescriptor for the given message_type
    // message_by_full_name() method requires the name to start with dot, it returns a clone
    // of the MessageDescriptor, so we can return it as-is without cloning again
    file_descriptors.iter().find_map(|fd| fd.message_by_full_name(&full_name))
}
