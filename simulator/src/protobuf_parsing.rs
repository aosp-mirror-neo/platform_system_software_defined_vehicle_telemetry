// Copyright 2024 Google LLC

use protobuf::{
    reflect::{FileDescriptor, MessageDescriptor, ReflectValueBox, RuntimeType},
    MessageDyn,
};
use sdv_telemetry_proto_rust::metrics_configuration::{MetricsConfig, MetricsReport};
use sdv_telemetry_rust_wrapper::ReportInfo;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use crate::{
    publisher_config::ParsedDataEntry,
    publisher_config_parser::{FieldNode, MessageDescriptorTree},
    SimulatorError,
};

pub const DELAY_FIELD_NAME: &str = "_delay_before_publishing";

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
    let file_descriptors = FileDescriptor::new_dynamic_fds(
        config.descriptor_protos.to_vec(),
        &WELL_KNOWN_FILE_DESCRIPTORS,
    )
    .map_err(|error| SimulatorError::FailedToParseFile {
        file_name: metrics_config_file_name.clone(),
        error_message: format!("Failed to parse descriptor_protos: {}", error),
    })?;
    for file_descriptor in &file_descriptors {
        for message in file_descriptor.messages() {
            if message_descriptors.contains_key(message.full_name()) {
                return Err(SimulatorError::DuplicateDescriptorProtoMessage {
                    message_name: message.full_name().to_owned(),
                    file_name: metrics_config_file_name,
                });
            }
            if decoding_message_descriptor_names.contains(message.full_name()) {
                message_descriptors.insert(message.full_name().to_owned(), message.clone());
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

pub struct CsvToProtoMessagesParser {
    headers: Vec<String>,
    file_name: String,
    default_delay: Duration,
    message_descriptor_tree: MessageDescriptorTree,
}

impl CsvToProtoMessagesParser {
    pub fn new(
        headers: Vec<String>,
        file_name: String,
        default_delay: Duration,
        mut message_descriptor_tree: MessageDescriptorTree,
    ) -> Self {
        trim_message_descriptor_tree(&headers, &mut message_descriptor_tree);
        Self { headers, file_name, default_delay, message_descriptor_tree }
    }

    pub fn parse(&self, records: Vec<Vec<String>>) -> Result<Vec<ParsedDataEntry>, SimulatorError> {
        let mut parsed_data = Vec::<ParsedDataEntry>::new();
        for (record_num, record) in records.into_iter().enumerate() {
            let mut delay = self.default_delay;
            let fields = self.map_field_names_values_to(record);
            let delay_value = &fields[DELAY_FIELD_NAME];
            if !delay_value.is_empty() {
                delay = self.parse_delay(delay_value)?;
            }

            let mut message: Box<dyn MessageDyn> =
                self.message_descriptor_tree.root.message_descriptor.new_instance();
            for node in &self.message_descriptor_tree.root.children {
                self.set_message_field(node, message.as_mut(), &fields, record_num + 1)?;
            }

            let message_bytes =
                message.write_to_bytes_dyn().map_err(|error| SimulatorError::Error {
                    error_message: format!(
                        "Failed to write parsed proto message {message:?} to bytes: {error}"
                    ),
                })?;
            parsed_data.push(ParsedDataEntry { message, message_bytes, delay });
        }
        Ok(parsed_data)
    }

    fn map_field_names_values_to(&self, record: Vec<String>) -> HashMap<String, String> {
        self.headers.iter().cloned().zip(record).collect::<HashMap<String, String>>()
    }

    fn parse_delay(&self, value: &str) -> Result<Duration, SimulatorError> {
        let duration_int =
            value.parse::<u64>().map_err(|error| SimulatorError::FailedToParseFile {
                file_name: self.file_name.clone(),
                error_message: format!(
                    "Could not parse \"{DELAY_FIELD_NAME}\" value \"{value}\" as u64: {error}"
                ),
            })?;
        Ok(Duration::from_millis(duration_int))
    }

    fn set_message_field(
        &self,
        node: &FieldNode,
        message: &mut dyn MessageDyn,
        fields_map: &HashMap<String, String>,
        record_num: usize,
    ) -> Result<bool, SimulatorError> {
        if let Some(internal_node_info) = &node.message_field_info {
            let mut child_message: Box<dyn MessageDyn> =
                internal_node_info.message_descriptor.new_instance();
            let mut any_field_was_set = false;
            for child_node in &internal_node_info.children {
                any_field_was_set = self.set_message_field(
                    child_node,
                    child_message.as_mut(),
                    fields_map,
                    record_num,
                )? || any_field_was_set;
            }
            if any_field_was_set {
                node.field_descriptor
                    .set_singular_field(message, ReflectValueBox::Message(child_message));
                return Ok(true);
            }
            Ok(false)
        } else {
            if !fields_map.contains_key(&node.csv_field_name) {
                // Ignore this field because it has an unsupported type.
                return Ok(false);
            }
            let csv_value = &fields_map[&node.csv_field_name];
            if csv_value.is_empty() {
                return Ok(false);
            }
            let value = parse_value(
                csv_value,
                node.field_descriptor.singular_runtime_type(),
                |value_type: &str| {
                    SimulatorError::FailedToParseFile {
                file_name: self.file_name.clone(),
                error_message: format!(
                    "Could not parse field \"{}\" for record #{record_num}: Could not parse \"{csv_value}\" as {}",
                    node.csv_field_name,
                    value_type.to_owned()
                ),
            }
                },
            )?;
            node.field_descriptor.set_singular_field(message, value);
            Ok(true)
        }
    }
}

/// Removes every child node for which the csv file does not contain any data.
fn trim_message_descriptor_tree(
    headers: &Vec<String>,
    message_descriptor_tree: &mut MessageDescriptorTree,
) {
    for child in &mut message_descriptor_tree.root.children {
        remove_unused_child_nodes(child, headers);
    }
}

/// Returns true if data is available for itself or one of its children. Removes
/// any child nodes with no data available.
fn remove_unused_child_nodes(node: &mut FieldNode, headers: &Vec<String>) -> bool {
    if headers.contains(&node.csv_field_name) {
        return true;
    }
    if let Some(ref mut message_field_info) = &mut node.message_field_info {
        message_field_info
            .children
            .retain_mut(|child_node| remove_unused_child_nodes(child_node, headers));
        return !message_field_info.children.is_empty();
    }
    false
}

fn parse_value(
    value: &str,
    run_time_type: RuntimeType,
    value_parsing_error: impl FnOnce(&str) -> SimulatorError,
) -> Result<ReflectValueBox, SimulatorError> {
    match run_time_type {
        RuntimeType::I32 => {
            Ok(ReflectValueBox::I32(value.parse::<i32>().map_err(|_| value_parsing_error("i32"))?))
        }
        RuntimeType::I64 => {
            Ok(ReflectValueBox::I64(value.parse::<i64>().map_err(|_| value_parsing_error("i64"))?))
        }
        RuntimeType::U32 => {
            Ok(ReflectValueBox::U32(value.parse::<u32>().map_err(|_| value_parsing_error("u32"))?))
        }
        RuntimeType::U64 => {
            Ok(ReflectValueBox::U64(value.parse::<u64>().map_err(|_| value_parsing_error("u64"))?))
        }
        RuntimeType::F32 => {
            Ok(ReflectValueBox::F32(value.parse::<f32>().map_err(|_| value_parsing_error("f32"))?))
        }
        RuntimeType::F64 => {
            Ok(ReflectValueBox::F64(value.parse::<f64>().map_err(|_| value_parsing_error("f64"))?))
        }
        RuntimeType::Bool => Ok(ReflectValueBox::Bool(
            value.parse::<bool>().map_err(|_| value_parsing_error("bool"))?,
        )),
        RuntimeType::String => Ok(ReflectValueBox::String(value.to_owned())),
        RuntimeType::VecU8 => Err(SimulatorError::NotImplemented {
            feature: "Byte array type in simulation publishers".to_owned(),
        }),
        RuntimeType::Enum(_) => Err(SimulatorError::NotImplemented {
            feature: "Enum types in simulation publishers".to_owned(),
        }),
        RuntimeType::Message(_) => Err(SimulatorError::NotImplemented {
            feature: "Message types in simulation publishers".to_owned(),
        }),
    }
}
