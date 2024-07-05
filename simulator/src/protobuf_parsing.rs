// Copyright 2024 Google LLC

use protobuf::{
    reflect::{FileDescriptor, MessageDescriptor},
    MessageDyn,
};
use sdv_telemetry_proto_rust::metrics_configuration::{MetricsConfig, MetricsReport};
use std::collections::{HashMap, HashSet};

use crate::{ReportInfo, SimulatorError};

pub fn parse_message_descriptors(
    config: &MetricsConfig,
    metrics_config_file_name: String,
) -> Result<HashMap<String, MessageDescriptor>, SimulatorError> {
    let decoding_message_descriptor_names: HashSet<String> = config
        .metrics_report_configs
        .iter()
        // Get the message descriptor name without the leading dot.
        .map(|report_config| report_config.message_builder.message_type.clone().split_off(1))
        .collect();

    let mut message_descriptors = HashMap::<String, MessageDescriptor>::new();
    let file_descriptors = FileDescriptor::new_dynamic_fds(config.descriptor_protos.to_vec(), &[])
        .map_err(|error| SimulatorError::FileParsingError {
            file_name: metrics_config_file_name.clone(),
            error_message: format!("Failed to parse descriptor_protos: {}", error),
        })?;
    for file_descriptor in &file_descriptors {
        for message in file_descriptor.messages() {
            if message_descriptors.contains_key(message.name()) {
                return Err(SimulatorError::DuplicateDescriptorProtoMessage {
                    message_name: message.name().to_owned(),
                    file_name: metrics_config_file_name,
                });
            }
            if decoding_message_descriptor_names.contains(message.name()) {
                message_descriptors.insert(message.name().to_owned(), message.clone());
            }
        }
    }

    for message_name in &decoding_message_descriptor_names {
        if !message_descriptors.contains_key(message_name) {
            return Err(SimulatorError::MissingDescriptorProto {
                file_name: metrics_config_file_name,
                message_name: message_name.clone(),
            });
        }
    }
    Ok(message_descriptors)
}

pub fn decode_report(
    report: &MetricsReport,
    message_descriptor: &MessageDescriptor,
    report_info: &ReportInfo,
) -> Result<Box<dyn MessageDyn>, SimulatorError> {
    // unpack_dyn returns a Result<Option<Box<dyn MessageDyn>>> where the result returns an error if
    // the content of the Any messages matches the message descriptor but it fails to parse the
    // message and the option is None if the Any message does not match the message descriptor.
    report
        .report_data
        .unpack_dyn(message_descriptor)
        .map_err(|error| SimulatorError::FailedToDecodeReport {
            report_config_name: report_info.report_config_name.clone(),
            error,
        })?
        .ok_or_else(|| SimulatorError::ProgrammingError {
            error_message: format!(
                "Could not decode report for report config \"{}\" with descriptor proto \"{}\"",
                report_info.report_config_name.clone(),
                message_descriptor.name()
            ),
        })
}
