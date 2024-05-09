// Copyright 2024 Google LLC. All Rights Reserved.

//! This module contains helper functions to read metrics configurations from a file.
use protobuf::{text_format::parse_from_str, Message};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

use anyhow::{anyhow, Context};
use std::fs;
use std::path::Path;
use std::path::PathBuf;

/// Reads the metrics config from either a .bin or legacy formatted .textproto file into a bytes
/// array.
pub fn read(path: &PathBuf) -> anyhow::Result<Vec<u8>> {
    match Path::new(&path).extension().and_then(|o| o.to_str()) {
        Some("bin") => {
            // Read binary into a vector
            fs::read(path)
                .with_context(|| format!("Failed to read binary file: {}", &path.display()))
        }
        Some("textproto") => {
            // Note that go/textformat-spec currently only supports the legacy text proto format.
            move || -> anyhow::Result<Vec<u8>> {
                let textproto = fs::read_to_string(path)?;
                let mc: MetricsConfig = parse_from_str::<MetricsConfig>(&textproto)?;
                Ok(mc.write_to_bytes()?)
            }()
            .with_context(|| format!("Failed to read textproto file: {}", &path.display()))
        }
        Some(unknown_ext) => Err(anyhow!(
            "Unsupported config file extension {}. Use .textproto or .bin.",
            unknown_ext
        )),
        None => Err(anyhow!("Unable to read file extension.")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialized_bytes_match_legacy_textproto_file_contents() {
        let serialized_legacy_textproto_config =
            read(&PathBuf::from("sample_protos/metric_configJan10.textproto")).unwrap();
        let config: MetricsConfig =
            protobuf::Message::parse_from_bytes(&serialized_legacy_textproto_config).unwrap();

        assert_eq!(&config.uuid, "d3456chu-8fde-436a-9f29-c12f06a10c102504");

        assert_eq!(config.descriptor_protos.len(), 1);
        assert_eq!(config.descriptor_protos[0].name.as_ref().unwrap(), "multi_config_test.proto");
        assert_eq!(
            config.descriptor_protos[0].package.as_ref().unwrap(),
            "android.automotive.telemetry.messages"
        );
        assert_eq!(config.descriptor_protos[0].message_type.len(), 1);

        let msg_type0 = config.descriptor_protos[0].message_type[0].clone();
        assert_eq!(msg_type0.name.unwrap(), "VehicleConnectivityReport");
        assert_eq!(msg_type0.field[0].name.as_ref().unwrap(), "vector_vehicle_connection_status");

        assert_eq!(config.expression_nodes.len(), 1);
        assert_eq!(config.expression_nodes[0].constant_leaf_node().int32_value(), 0);

        assert_eq!(config.triggers.len(), 2);
        assert_eq!(config.triggers[0].name, "start_trigger");
        assert_eq!(config.triggers[0].periodic_trigger().interval_ms, 3000);

        assert_eq!(config.triggers[1].name, "period_3s");
        assert_eq!(config.triggers[1].periodic_trigger().interval_ms, 3000);

        assert_eq!(&config.start_trigger_name, "start_trigger");

        let report_config = config.metrics_report_configs.clone();
        assert_eq!(report_config.len(), 1);
        assert_eq!(report_config[0].name, "multi_config_report");
        assert_eq!(report_config[0].trigger_names[0], "period_3s");
        assert_eq!(
            report_config[0].message_builder.message_type,
            ".android.automotive.telemetry.messages.VehicleConnectivityReport"
        );
        assert_eq!(report_config[0].message_builder.field_assignments.len(), 1);
        assert_eq!(
            report_config[0].message_builder.field_assignments[0].field_name,
            "vector_vehicle_connection_status"
        );
        assert_eq!(
            report_config[0].message_builder.field_assignments[0]
                .no_aggregation()
                .expression_node_index
                .unwrap(),
            0
        );
    }
}
