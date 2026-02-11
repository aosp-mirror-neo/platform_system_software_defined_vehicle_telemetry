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

use sdv_comms::id::UnitIdentifier;
use sdv_comms::sd::UnitMetadata;
use sdv_telemetry_comms_utils::debug_format_unit_identifier;
use sdv_telemetry_utils::atomic::SequenceNumber;
use sdv_telemetry_utils::atomic::SequenceNumberType;
use std::fmt::Debug;

use super::Metadata;

/// A unique identifier for a `MetadataGeneration`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MetadataGenerationId(SequenceNumberType);

impl From<SequenceNumberType> for MetadataGenerationId {
    fn from(value: SequenceNumberType) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum MetadataServiceType {
    Dt,
    Rpc,
}

impl MetadataServiceType {
    pub(crate) fn new(unit_metadata: &UnitMetadata) -> Self {
        match unit_metadata {
            UnitMetadata::Publisher(_) => Self::Dt,
            UnitMetadata::Server(_) => Self::Rpc,
        }
    }
}

/// The result of a successful metadata fetch operation, tied to a specific unit identifier.
/// A service's `MetadataServiceType`, `Metadata` and `UnitIdentifier` may change over time.
/// We call each of these changes a "metadata generation".
#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct MetadataGeneration {
    pub(crate) id: MetadataGenerationId,
    pub(crate) service_type: MetadataServiceType,
    pub(crate) unit_identifier: UnitIdentifier,
    pub(crate) metadata: Metadata,
}

impl MetadataGeneration {
    pub(crate) fn new(
        service_type: MetadataServiceType,
        unit_identifier: UnitIdentifier,
        metadata: Metadata,
    ) -> Self {
        static ID_FACTORY: SequenceNumber = SequenceNumber::new();
        Self { id: ID_FACTORY.next().into(), service_type, unit_identifier, metadata }
    }

    pub(crate) fn eq_ignoring_id(&self, other: &Self) -> bool {
        let Self { service_type, unit_identifier, metadata, id: _ } = self;
        let Self {
            service_type: other_service_type,
            unit_identifier: other_unit_identifier,
            metadata: other_metadata,
            id: _,
        } = other;
        service_type == other_service_type
            && unit_identifier == other_unit_identifier
            && metadata == other_metadata
    }
}

impl Debug for MetadataGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataGeneration")
            .field("id", &self.id)
            .field("service_type", &self.service_type)
            .field("unit_identifier", &debug_format_unit_identifier!(&self.unit_identifier))
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[cfg(all(test, unit_tests))]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::eq;
    use googletest::matchers::not;
    use sdv_comms::sd::PublisherUnitMetadata;
    use sdv_comms::sd::ServerUnitMetadata;

    #[googletest::test]
    fn test_metadata_generation_id_from() {
        let id1 = MetadataGenerationId::from(1);
        let id2 = MetadataGenerationId::from(2);
        expect_that!(id1.0, eq(1));
        expect_that!(id2.0, eq(2));
        expect_that!(id1, not(eq(id2)));
    }

    #[googletest::test]
    fn test_metadata_service_type_dt() {
        let unit_metadata = UnitMetadata::Publisher(PublisherUnitMetadata::new());
        let service_type = MetadataServiceType::new(&unit_metadata);
        expect_that!(service_type, eq(MetadataServiceType::Dt));
    }

    #[googletest::test]
    fn test_metadata_service_type_rpc() {
        let unit_metadata = UnitMetadata::Server(ServerUnitMetadata::new());
        let service_type = MetadataServiceType::new(&unit_metadata);
        expect_that!(service_type, eq(MetadataServiceType::Rpc));
    }
}

#[cfg(all(test, integration_tests))]
mod integration_tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{eq, is_false, is_true, not};
    use protobuf::well_known_types::any::Any;
    use protobuf::MessageFull;

    use crate::test_utils;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn test_metadata_generation_new() {
        binder::ProcessState::start_thread_pool();

        let service_name_1 = test_utils::make_unique_service_name();
        let (_pub1, gen1) = test_utils::create_metadata_generation(service_name_1).await;

        let service_name_2 = test_utils::make_unique_service_name();
        let (_pub2, gen2) = test_utils::create_metadata_generation(service_name_2).await;

        // IDs should be unique given the ID factory logic
        expect_that!(gen1.id, not(eq(gen2.id)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn test_metadata_generation_eq_ignoring_id() {
        binder::ProcessState::start_thread_pool();

        let service_name_1 = test_utils::make_unique_service_name();
        let (_pub1, gen1) = test_utils::create_metadata_generation(service_name_1).await;

        let mut gen1_modified_id = gen1.clone();
        gen1_modified_id.id = MetadataGenerationId::from(9999);

        expect_that!(gen1.eq_ignoring_id(&gen1_modified_id), is_true());

        let service_name_2 = test_utils::make_unique_service_name();
        let (_pub2, gen2) = test_utils::create_metadata_generation(service_name_2).await;

        // Change unit identifier
        let mut gen1_modified_unit_identifier = gen1.clone();
        gen1_modified_unit_identifier.unit_identifier = gen2.unit_identifier.clone();
        expect_that!(gen1.eq_ignoring_id(&gen1_modified_unit_identifier), is_false());

        // Change service type
        let mut gen1_modified_service_type = gen1.clone();
        gen1_modified_service_type.service_type = MetadataServiceType::Rpc;
        expect_that!(gen1.eq_ignoring_id(&gen1_modified_service_type), is_false());

        // Change metadata message descriptor
        let mut gen1_modified_metadata = gen1.clone();
        gen1_modified_metadata.metadata.message_descriptor = Any::descriptor();
        expect_that!(gen1.eq_ignoring_id(&gen1_modified_metadata), is_false());
    }
}
