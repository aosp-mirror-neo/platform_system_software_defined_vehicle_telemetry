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

use log::warn;
use protobuf::{
    descriptor::FileDescriptorProto,
    reflect::{FieldDescriptor, MessageDescriptor, RuntimeType},
};
use sdv_telemetry_simulator_proto::simulation_publisher::{
    data_generator::Data_source,
    publishing_strategy::ConfigurablePublisherRegistry as ConfigurablePublisherRegistryProto,
    publishing_strategy::SdvCommsDataTunnel as SdvCommsDataTunnelProto,
    publishing_strategy::SdvCommsRpc as SdvCommsRpcProto, publishing_strategy::Type as TypeProto,
    PublishingStrategy as PublishingStrategyProto, SimulationPublisher,
};
use sdv_telemetry_simulator_publishing_strategy::PublishingStrategyType;
use sdv_telemetry_utils::{
    descriptor_utils::FileDescriptorsAndWkts, duration_utils::try_into_duration, proto_reader,
};
use std::ffi::OsStr;
use std::{collections::HashSet, path::Path};

use crate::{
    io::parse_csv_file,
    protobuf_parsing::{CsvToProtoMessagesParser, DELAY_FIELD_NAME},
    publisher_config::PublisherConfig,
    simulator_errors::SimulatorError,
};

fn get_publishing_strategy(
    publishing_strategy: Option<&PublishingStrategyProto>,
) -> Result<PublishingStrategyType, ()> {
    Ok(match publishing_strategy.and_then(|strategy| strategy.type_.as_ref()) {
        None
        | Some(TypeProto::ConfigurablePublisherRegistry(ConfigurablePublisherRegistryProto {
            special_fields: _,
        })) => PublishingStrategyType::ConfigurablePublisherRegistry,
        Some(TypeProto::SdvCommsDataTunnel(SdvCommsDataTunnelProto {
            message_count,
            message_size,
            special_fields: _,
        })) => PublishingStrategyType::SdvCommsDataTunnel {
            message_count: *message_count,
            message_size: *message_size,
        },
        Some(TypeProto::SdvCommsRpc(SdvCommsRpcProto { special_fields: _ })) => {
            PublishingStrategyType::SdvCommsRpc
        }
        Some(_) => return Err(()),
    })
}

pub fn parse_publisher_config(file_path: &Path) -> Result<PublisherConfig, SimulatorError> {
    let file_name = file_path
        .file_name()
        .ok_or(SimulatorError::InvalidFilePath { file_path: file_path.to_owned() })?;

    let publisher_config_proto = proto_reader::read_as_message::<SimulationPublisher>(file_path)
        .map_err(|err| SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: err.to_string(),
        })?;

    let message_descriptor = get_message_descriptor(
        &publisher_config_proto.data_format_protos,
        &publisher_config_proto.data_format_message_name,
        file_name,
    )?;

    let publishing_strategy = get_publishing_strategy(
        publisher_config_proto.publishing_strategy.as_ref(),
    )
    .map_err(|()| SimulatorError::FailedToParseFile {
        file_name: file_name.to_owned(),
        error_message: "Invalid publishing strategy".to_owned(),
    })?;

    // TODO(b/351183822): Allow more than one data generator.
    let [ref data_generator] = publisher_config_proto.data_generators[..] else {
        return Err(SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: format!(
                "Expected exactly one data generator, but got {}.",
                publisher_config_proto.data_generators.len()
            ),
        });
    };

    if !data_generator.configuration.is_none() {
        match publishing_strategy {
            PublishingStrategyType::SdvCommsDataTunnel { .. } => {
                return Err(SimulatorError::FailedToParseFile {
                    file_name: file_name.to_owned(),
                    error_message: "Specifying a publisher configuration is not allowed for SDV Comms Data Tunnel publishers.".to_owned(),
                });
            }
            PublishingStrategyType::SdvCommsRpc
            | PublishingStrategyType::ConfigurablePublisherRegistry => {
                return Err(SimulatorError::NotImplemented {
                    feature: "Custom data per configuration".to_owned(),
                });
            }
        }
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

            let (ordered_headers, records) = parse_csv_file(
                Path::new(&csv_file.file_path),
                delimiter,
                file_name,
                expected_csv_headers,
            )?;

            let csv_to_proto_parser =
                CsvToProtoMessagesParser::new(ordered_headers, file_name, default_delay, tree);

            let parsed_data = csv_to_proto_parser.parse(records)?;
            Ok(PublisherConfig {
                service_name: publisher_config_proto.service_name,
                loop_data: data_generator.loop_data,
                parsed_data,
                message_name: message_descriptor.full_name().to_owned(),
                file_descriptor_protos: publisher_config_proto.data_format_protos.to_owned(),
                publishing_strategy,
            })
        }
        _ => Err(SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
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
    file_name: &OsStr,
) -> Result<MessageDescriptor, SimulatorError> {
    FileDescriptorsAndWkts::try_from(file_descriptor_protos.to_vec())
        .map_err(|error| SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: error.to_string(),
        })?
        .find_message_by_full_name(message_name)
        .ok_or_else(|| SimulatorError::MissingDescriptorProto {
            file_name: file_name.to_owned(),
            message_name: message_name.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::{container_eq, eq, err, ok, pat};
    use protobuf::{text_format, MessageFull};
    use sdv_telemetry_simulator_proto::simulation_publisher::{
        CSVFile, DataGenerator, SimulationPublisher,
    };
    use sdv_telemetry_simulator_test_proto::test::{
        TestMessage, TestMessageWithDependencyInOtherFile, TestMessageWithWellKnownType,
    };
    use sdv_telemetry_simulator_test_proto::test_dep;
    use sdv_telemetry_test_utils::parameterized_test;
    use std::fs;
    use tempfile::tempdir;

    parameterized_test!(
        #[test]
        parse_publisher_data_format_protos_succeeds,

        |message_name: &str, data_format_protos: &[&FileDescriptorProto]| {
            let tmp_dir = tempdir().unwrap();
            let config_path = tmp_dir.path().join("config.txtpb");
            let data_path = tmp_dir.path().join("data.csv");

            let simulation_publisher = SimulationPublisher {
                service_name: "test_service".to_owned(),
                data_format_message_name: format!(".{message_name}"),
                data_format_protos: data_format_protos.iter().copied().cloned().collect(),
                data_generators: vec![
                    DataGenerator {
                        data_source: Some(Data_source::CsvFile (CSVFile {
                            file_path: data_path.to_str().unwrap().to_owned(),
                            delimiter: ",".to_owned(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }
                ],
                ..Default::default()
            };

            fs::write(&data_path, "").unwrap();
            fs::write(
                &config_path,
                text_format::print_to_string_pretty(&simulation_publisher)
            ).unwrap();

            let result = parse_publisher_config(&config_path);

            assert_that!(
                result,
                ok(pat!(PublisherConfig {
                    service_name: eq("test_service"),
                    message_name: eq(message_name),
                    file_descriptor_protos: container_eq(data_format_protos.iter().copied().cloned().collect::<Vec<_>>()),
                    ..
                }))
            );
        },

        well_known_type: ("google.protobuf.Empty", &[]),
        message: ("TestMessage", &[TestMessage::descriptor().file_descriptor_proto(), test_dep::file_descriptor().proto()]),
        message_with_well_known_type: ("TestMessageWithWellKnownType", &[TestMessageWithWellKnownType::descriptor().file_descriptor_proto(), test_dep::file_descriptor().proto()]),
        message_with_dependency_in_other_file: ("TestMessageWithDependencyInOtherFile", &[TestMessageWithDependencyInOtherFile::descriptor().file_descriptor_proto(), test_dep::file_descriptor().proto()]),
    );

    parameterized_test!(
        #[test]
        test_read_publisher_config_from,

        |path, matcher| {
            let result = parse_publisher_config(Path::new(path));
            assert_that!(result, matcher);
        },

        textproto: ("valid_publisher_config.textproto", ok(pat!(PublisherConfig { service_name: eq("TestPublisher"), loop_data: eq(&true), .. }))),
        binary: ("valid_publisher_config.pb", ok(pat!(PublisherConfig { service_name: eq("TestPublisher"), loop_data: eq(&true), .. }))),
        invalid_path: ("invalid_path/.", err(pat!(SimulatorError::FailedToParseFile { .. }))),
        invalid_extension: ("valid_publisher_config.txt", err(pat!(SimulatorError::FailedToParseFile { .. }))),
        invalid_textproto: ("invalid_publisher_config.textproto", err(pat!(SimulatorError::FailedToParseFile { .. }))),
    );

    parameterized_test!(
        #[test]
        parse_publishing_strategy_succeeds,

        |textproto_content, publishing_strategy_type_matcher| {
            let tmp_dir = tempdir().unwrap();
            let config_path = tmp_dir.path().join("config.txtpb");
            let data_path = tmp_dir.path().join("data.csv");

            fs::write(&data_path, "").unwrap();
            fs::write(
                &config_path,
                format!(
                    r#"
                    service_name: "test_service"
                    data_format_message_name: ".google.protobuf.Empty"
                    data_generators {{
                        csv_file {{
                            file_path: "{0}"
                            delimiter: ","
                        }}
                    }}
                    {textproto_content}
                    "#,
                    data_path.display()
                ),
            )
            .unwrap();

            let result = parse_publisher_config(&config_path);

            assert_that!(
                result,
                ok(pat!(&PublisherConfig {
                    service_name: ref eq("test_service"),
                    publishing_strategy: ref publishing_strategy_type_matcher,
                    ..
                }))
            );
        },

        unspecified: (
            "",
            pat!(PublishingStrategyType::ConfigurablePublisherRegistry)
        ),
        no_type: (
            "publishing_strategy {}",
            pat!(PublishingStrategyType::ConfigurablePublisherRegistry)
        ),
        configurable_publisher_registry: (
            "publishing_strategy { configurable_publisher_registry {} }",
            pat!(PublishingStrategyType::ConfigurablePublisherRegistry)
        ),
        sdv_comms_data_tunnel: (
            "publishing_strategy { sdv_comms_data_tunnel {message_count: 3 message_size: 5} }",
            pat!(PublishingStrategyType::SdvCommsDataTunnel { message_count: &3, message_size: &5 })
        ),
        sdv_comms_rpc: (
            "publishing_strategy { sdv_comms_rpc {} }",
            pat!(PublishingStrategyType::SdvCommsRpc)
        ),
    );

    #[test]
    fn parse_publisher_config_fails_for_data_tunnel_with_configuration() {
        let tmp_dir = tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.txtpb");
        let data_path = tmp_dir.path().join("data.csv");

        fs::write(&data_path, "").unwrap();
        fs::write(
            &config_path,
            format!(
                r#"
                service_name: "test_service"
                data_format_message_name: ".google.protobuf.Empty"
                data_generators {{
                    configuration {{
                        type_url: "type.googleapis.com/google.protobuf.Empty"
                        value: ""
                    }}
                    csv_file {{
                        file_path: "{0}"
                        delimiter: ","
                    }}
                }}
                publishing_strategy {{
                    sdv_comms_data_tunnel {{}}
                }}
                "#,
                data_path.display()
            ),
        )
        .unwrap();

        let result = parse_publisher_config(&config_path);

        assert_that!(
            result,
            err(pat!(SimulatorError::FailedToParseFile {
                error_message: eq("Specifying a publisher configuration is not allowed for SDV Comms Data Tunnel publishers."),
                ..
            }))
        );
    }
}
