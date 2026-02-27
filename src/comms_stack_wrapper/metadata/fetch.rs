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

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use log::debug;
use protobuf::Message;
use sdv_comms::sd::ServiceUnitDefinition;
use sdv_comms::sd::UnitMetadata;
use sdv_config_provider::vsidl_provider_api::VsidlConfigProvider;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_utils::ServiceName;
use sdv_telemetry_utils::descriptor_utils::FileDescriptorsAndWkts;
use std::future::Future;
use std::sync::Arc;

use super::Metadata;
use crate::data_tunnel_subscriber;
use crate::sdv_rpc_client;

/// Fetches the metadata for a given service.
pub(crate) async fn fetch_metadata<F>(
    service_name: &ServiceName,
    service_unit_definition: &ServiceUnitDefinition,
    vsidl_provider_factory: impl FnOnce() -> F,
) -> anyhow::Result<Metadata>
where
    F: Future<Output = Option<Arc<dyn VsidlConfigProvider + Send + Sync>>>,
{
    fetch_metadata_impl(service_name, service_unit_definition, vsidl_provider_factory)
        .await
        .with_context(|| format!("Failed to fetch metadata for {service_name}"))
}

async fn fetch_metadata_impl<F>(
    service_name: &ServiceName,
    service_unit_definition: &ServiceUnitDefinition,
    vsidl_provider_factory: impl FnOnce() -> F,
) -> anyhow::Result<Metadata>
where
    F: Future<Output = Option<Arc<dyn VsidlConfigProvider + Send + Sync>>>,
{
    let app_metadata = service_unit_definition.get_application_metadata();
    if !app_metadata.is_empty() {
        debug!("Service unit definition for {service_name} contains application metadata, using it to retrieve descriptor");

        fetch_metadata_from_service_unit_definition_app_metadata(&app_metadata)
    } else if let Some(ref vsidl_provider) = vsidl_provider_factory().await {
        debug!("Querying VSIDL provider to retrieve descriptor for {service_name}");

        fetch_metadata_from_vsidl_provider(service_unit_definition, vsidl_provider.as_ref()).await
    } else {
        debug!("Neither service unit definition application metadata, nor the VSIDL provider are available for {service_name}");

        bail!("Neither service unit definition application metadata, nor the VSIDL provider are available.");
    }
}

fn fetch_metadata_from_service_unit_definition_app_metadata(
    app_metadata: &[u8],
) -> anyhow::Result<Metadata> {
    let publisher_metadata = MessageMetadata::parse_from_bytes(app_metadata)
        .context("Failed to parse publisher app metadata")?;

    // Build the provided proto file descriptors.
    let file_descriptors_len = publisher_metadata.file_descriptors.len();
    let file_descriptors = FileDescriptorsAndWkts::try_from(publisher_metadata.file_descriptors)
        .context("Invalid Descriptor Protos")?;

    let Some(metadata) = file_descriptors
        .find_message_by_full_name(&publisher_metadata.message_full_name)
        .map(|message_descriptor| Metadata { config_descriptor: None, message_descriptor })
    else {
        bail!(
            "Message named '{}' not found in file descriptors ({file_descriptors_len} file descriptors provided)",
            publisher_metadata.message_full_name
        );
    };

    Ok(metadata)
}

async fn fetch_metadata_from_vsidl_provider(
    service_unit_definition: &ServiceUnitDefinition,
    vsidl_provider: &dyn VsidlConfigProvider,
) -> anyhow::Result<Metadata> {
    match service_unit_definition.get_unit_metadata() {
        UnitMetadata::Publisher(_) => {
            data_tunnel_subscriber::get_message_descriptor(vsidl_provider, service_unit_definition)
                .await
                .map(|maybe_descriptor| {
                    maybe_descriptor.map(|message_descriptor| Metadata {
                        config_descriptor: None,
                        message_descriptor,
                    })
                })
        }
        UnitMetadata::Server(_) => {
            sdv_rpc_client::get_message_descriptor(vsidl_provider, service_unit_definition)
                .await
                .map(|maybe_descriptors| {
                    maybe_descriptors.map(|(config_descriptor, message_descriptor)| Metadata {
                        config_descriptor: Some(config_descriptor),
                        message_descriptor,
                    })
                })
        }
    }
    .context("Invalid descriptor(s) retrieved from VSIDL provider")?
    .ok_or_else(|| anyhow!("VSIDL provider was unable to provide a descriptor"))
}
