// Copyright 2024 Google LLC

//! This module contains helper functions to read metrics configurations from a file.

use anyhow::bail;
use protobuf::{text_format, MessageFull};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;

/// Reads a proto from either a binary or textproto file into a message.
pub fn read_as_message<M: MessageFull>(path: &Path) -> anyhow::Result<M> {
    let Some(extension) = path.extension().and_then(OsStr::to_str) else {
        bail!("Unable to read file extension.");
    };

    Ok(match extension {
        "bin" | "binpb" | "pb" => {
            let bytes = fs::read(path)?;
            M::parse_from_bytes(&bytes)?
        }
        "textproto" | "txtpb" => {
            let textproto = fs::read_to_string(path)?;
            text_format::parse_from_str::<M>(&textproto)?
        }
        unknown_ext => bail!(
            "Unsupported config file extension {unknown_ext}. Use .textproto, .txtpb, .binpb, .pb or .bin.",
        ),
    })
}

/// Reads a proto from either a binary or textproto file into a bytes array. Returns an error if the
/// bytes do not correspond to a valid Message
pub fn read_as_bytes<M: MessageFull>(path: &Path) -> anyhow::Result<Vec<u8>> {
    let message = read_as_message::<M>(path)?;
    Ok(message.write_to_bytes()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::{Message, MessageField};
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig;
    use std::time::Duration;

    use crate::duration_utils::try_into_proto_duration;

    #[test]
    fn serialized_bytes_match_textproto_file_contents() {
        let serialized_legacy_textproto_config = read_as_bytes::<MetricsConfig>(Path::new(
            "config_without_service_publishers.textproto",
        ))
        .unwrap();
        let config = MetricsConfig::parse_from_bytes(&serialized_legacy_textproto_config).unwrap();

        assert_eq!(&config.uuid, "d3456cf0-8fde-436a-9f29-c12f16d10c10");

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
        assert_eq!(
            config.triggers[0].periodic_trigger().interval,
            MessageField::some(try_into_proto_duration(&Duration::from_secs(3)).unwrap())
        );

        assert_eq!(config.triggers[1].name, "period_3s");
        assert_eq!(
            config.triggers[1].periodic_trigger().interval,
            MessageField::some(try_into_proto_duration(&Duration::from_secs(3)).unwrap())
        );

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
