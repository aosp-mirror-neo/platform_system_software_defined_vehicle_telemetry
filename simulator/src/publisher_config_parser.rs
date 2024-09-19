// Copyright 2024 Google LLC

use log::warn;
use protobuf::{
    descriptor::FileDescriptorProto,
    reflect::{FieldDescriptor, FileDescriptor, MessageDescriptor, RuntimeType},
};
use sdv_telemetry_proto_simulation_publisher::simulation_publisher::data_generator::Data_source;
use sdv_telemetry_utils::duration_utils::try_into_duration;
use std::{collections::HashSet, path::Path};

use crate::{
    io::{parse_csv_file, read_publisher_config_from_file},
    protobuf_parsing::{CsvToProtoMessagesParser, DELAY_FIELD_NAME},
    publisher_config::PublisherConfig,
    simulator_errors::SimulatorError,
};

pub fn parse_publisher_config(file_path: &Path) -> Result<PublisherConfig, SimulatorError> {
    let (file_name, publisher_config_proto) = read_publisher_config_from_file(file_path)?;

    let message_descriptor = get_message_descriptor(
        &publisher_config_proto.data_format_protos,
        &publisher_config_proto.data_format_message_name,
        &file_name,
    )?;

    // TODO(b/351183822): Allow more than one data generator.
    let data_generator = &publisher_config_proto.data_generators[0];

    if !data_generator.configuration.is_none() {
        return Err(SimulatorError::NotImplemented {
            feature: "Custom data per configuration".to_owned(),
        });
    };

    let default_delay = try_into_duration(&data_generator.default_delay).map_err(|_| {
        SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: "Could not parse default delay".to_owned(),
        }
    })?;
    match &data_generator.data_source {
        Some(Data_source::CsvFile(csv_file)) => {
            let &[delimiter] = csv_file.delimiter.as_bytes() else {
                return Err(SimulatorError::FailedToParseFile {
                    file_name: file_name.to_owned(),
                    error_message: "CSV file delimiter must be a single character".to_owned(),
                });
            };
            let (tree, mut expected_csv_headers) =
                get_message_descriptor_tree_and_expected_headers(&message_descriptor)?;
            expected_csv_headers.insert(DELAY_FIELD_NAME.to_owned());

            let (ordered_headers, records) =
                parse_csv_file(&csv_file.file_path, delimiter, &file_name, expected_csv_headers)?;

            let csv_to_proto_parser =
                CsvToProtoMessagesParser::new(ordered_headers, file_name, default_delay, tree);

            let parsed_data = csv_to_proto_parser.parse(records)?;
            Ok(PublisherConfig {
                service_name: publisher_config_proto.service_name,
                loop_data: data_generator.loop_data,
                parsed_data,
                message_descriptor,
            })
        }
        _ => Err(SimulatorError::FailedToParseFile {
            file_name,
            error_message: "Failed to read data in data generator".to_owned(),
        }),
    }
}

pub struct MessageDescriptorTree {
    pub root: MessageFieldInfo,
}

pub struct FieldNode {
    pub csv_field_name: String,
    pub field_descriptor: FieldDescriptor,
    pub message_field_info: Option<MessageFieldInfo>,
}

pub struct MessageFieldInfo {
    pub children: Vec<FieldNode>,
    pub message_descriptor: MessageDescriptor,
}

impl FieldNode {
    fn new(csv_field_name: String, field_descriptor: FieldDescriptor) -> Self {
        Self { csv_field_name, field_descriptor, message_field_info: None }
    }

    fn set_message_field_info(
        &mut self,
        children: Vec<FieldNode>,
        message_descriptor: MessageDescriptor,
    ) {
        self.message_field_info = Some(MessageFieldInfo { children, message_descriptor });
    }
}

fn get_message_descriptor_tree_and_expected_headers(
    message_descriptor: &MessageDescriptor,
) -> Result<(MessageDescriptorTree, HashSet<String>), SimulatorError> {
    let (children, headers) = get_message_descriptor_tree_and_expected_headers_recursively(
        message_descriptor,
        "".to_owned(),
    )?;
    let tree = MessageDescriptorTree {
        root: MessageFieldInfo { children, message_descriptor: message_descriptor.clone() },
    };
    Ok((tree, headers))
}

fn get_message_descriptor_tree_and_expected_headers_recursively(
    message_descriptor: &MessageDescriptor,
    base_name: String,
) -> Result<(Vec<FieldNode>, HashSet<String>), SimulatorError> {
    let mut expected_csv_headers = HashSet::new();
    let mut nodes = Vec::<FieldNode>::new();

    for field_descriptor in message_descriptor.fields() {
        if !field_descriptor.is_singular() {
            warn!("Repeated fields in simulation publisher message descriptor are not supported yet, field \'{}\' in message \'{}\' will be ignored.\n", field_descriptor.name(), message_descriptor.full_name());
            continue;
        }
        let field_name = base_name.clone() + field_descriptor.name().trim();
        let mut node = FieldNode::new(field_name.clone(), field_descriptor.clone());

        if let RuntimeType::Message(nested_message_descriptor) =
            field_descriptor.singular_runtime_type()
        {
            let (children, headers) = get_message_descriptor_tree_and_expected_headers_recursively(
                &nested_message_descriptor,
                field_name + ".",
            )?;
            expected_csv_headers.extend(headers);

            node.set_message_field_info(children, nested_message_descriptor.clone());
        } else {
            expected_csv_headers.insert(field_name);
        }
        nodes.push(node);
    }

    Ok((nodes, expected_csv_headers))
}

fn get_message_descriptor(
    file_descriptor_protos: &[FileDescriptorProto],
    message_name: &str,
    file_name: &str,
) -> Result<MessageDescriptor, SimulatorError> {
    let file_descriptors = FileDescriptor::new_dynamic_fds(file_descriptor_protos.to_vec(), &[])
        .map_err(|error| SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: error.to_string(),
        })?;

    for file_descriptor in &file_descriptors {
        if let Some(message_descriptor) =
            file_descriptor.message_by_package_relative_name(message_name)
        {
            return Ok(message_descriptor);
        }
    }
    Err(SimulatorError::MissingDescriptorProto {
        file_name: file_name.to_owned(),
        message_name: message_name.to_owned(),
    })
}
