// Copyright 2024 Google LLC. All Rights Reserved.

//! This module contains helper functions to read metrics configurations from a file.
use protobuf::{text_format::parse_from_str, Message};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

use log::error;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

/// Reads the metrics config from either a .bin or legacy formatted .textproto file into a bytes
/// array.
pub fn read(path: &PathBuf) -> Option<Vec<u8>> {
    match Path::new(&path).extension().and_then(|o| o.to_str()) {
        Some("bin") => {
            // Read binary into a vector
            fs::read(path).ok()
        }
        Some("textproto") => {
            // Note that go/textformat-spec currently only supports the legacy text proto format.
            let str_config = fs::read_to_string(path).unwrap();
            let mc: MetricsConfig =
                parse_from_str::<MetricsConfig>(&str_config).map_err(|e| e.to_string()).unwrap();
            mc.write_to_bytes().ok()
        }
        Some(unknown_ext) => {
            error!("Unsupported config file extension {}. Use .textproto or .bin.", unknown_ext);
            None
        }
        None => {
            error!("Unable to read file extension.");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialized_bytes_from_legacy_textproto_and_bin_file_match() {
        let serialized_binary_config = fs::read("sample_protos/metric_configJan10.bin").unwrap();
        let serialized_legacy_textproto_config =
            read(&PathBuf::from("sample_protos/metric_configJan10.textproto")).unwrap();

        // The bytes generated from the .textproto file should match with the bytes read from
        // corresponding manually generated .bin file.
        assert!(!serialized_binary_config.is_empty());
        assert_eq!(serialized_binary_config, serialized_legacy_textproto_config);

        // The deserialized bytes should contain the same information as the read .textproto.
        // Since the bytes already match, it's enough to check the contents of either of the files.
        let config: MetricsConfig =
            protobuf::Message::parse_from_bytes(&serialized_binary_config).unwrap();
        assert_eq!(&config.uuid, "d3456chu-8fde-436a-9f29-c12f06a10c102504");
        assert_eq!(&config.start_trigger_name, "start_trigger");
        assert_eq!(config.triggers.len(), 2);
        assert_eq!(config.descriptor_protos.len(), 1);
        assert_eq!(config.expression_nodes.len(), 1);
        assert_eq!(config.metrics_report_configs.len(), 1);
    }
}
