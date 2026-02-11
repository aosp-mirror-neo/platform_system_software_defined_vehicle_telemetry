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
use sdv_telemetry_comms_utils::debug_format_unit_identifier;
use std::fmt;
use std::ops::Not;

use super::MetadataGeneration;
use super::MetadataGenerationId;
use super::MetadataServiceType;

/// An event that represents a change in the metadata generations for a service.
pub(crate) enum MetadataGenerationEvent {
    /// A metadata generation needs to be added or updated.
    Update(MetadataGeneration),
    /// A metadata generation needs to be removed by unit identifier and service type.
    RemoveByUnitIdentifierAndServiceType {
        unit_identifier: UnitIdentifier,
        service_type: MetadataServiceType,
    },
    /// A metadata generation needs to be removed by id.
    RemoveById(MetadataGenerationId),
}

impl fmt::Debug for MetadataGenerationEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Update(metadata) => f.debug_tuple("Update").field(metadata).finish(),
            Self::RemoveByUnitIdentifierAndServiceType { unit_identifier, service_type } => f
                .debug_struct("RemoveByUnitIdentifierAndServiceType")
                .field("unit_identifier", &debug_format_unit_identifier!(unit_identifier))
                .field("service_type", service_type)
                .finish(),
            Self::RemoveById(id) => f.debug_tuple("RemoveById").field(id).finish(),
        }
    }
}

/// A collection of `MetadataGeneration`s for a service. A service may have multiple
/// `MetadataGeneration`s at the same time, e.g., if it is deployed to multiple VMs.
#[derive(Debug, Clone)]
pub(crate) struct CurrentMetadataGenerations(Vec<MetadataGeneration>);

impl CurrentMetadataGenerations {
    pub(crate) fn new(metadata_generations: Vec<MetadataGeneration>) -> Self {
        Self(metadata_generations)
    }

    /// Handles an event and returns `true` if the collection of metadata generations changed.
    #[must_use]
    pub(crate) fn handle(&mut self, event: MetadataGenerationEvent) -> bool {
        match event {
            MetadataGenerationEvent::Update(metadata_generation) => {
                let idx = self.0.iter().position(|generation| {
                    generation.unit_identifier == metadata_generation.unit_identifier
                        && generation.service_type == metadata_generation.service_type
                });
                if let Some(idx) = idx {
                    self.0[idx] = metadata_generation;
                } else {
                    self.0.push(metadata_generation);
                }
                true
            }
            MetadataGenerationEvent::RemoveByUnitIdentifierAndServiceType {
                unit_identifier,
                service_type,
            } => self
                .0
                .extract_if(.., |generation| {
                    generation.unit_identifier == unit_identifier
                        && generation.service_type == service_type
                })
                .collect::<Vec<_>>()
                .is_empty()
                .not(),
            MetadataGenerationEvent::RemoveById(generation_id) => self
                .0
                .extract_if(.., |generation| generation.id == generation_id)
                .collect::<Vec<_>>()
                .is_empty()
                .not(),
        }
    }

    /// Get a reference to the `MetadataGeneration` if we currently have exactly one. Otherwise
    /// return `None`.
    ///
    /// This is due to the fact that Telemetry currently only handles services that have exactly one
    /// instance running at a time.
    pub(crate) fn get(&self) -> Option<&MetadataGeneration> {
        if let [ref metadata_generation] = self.0[..] {
            Some(metadata_generation)
        } else {
            None
        }
    }

    /// Consume `Self` and get the `MetadataGeneration` if we currently have exactly one. Otherwise
    /// return `None`.
    ///
    /// This is due to the fact that Telemetry currently only handles services that have exactly one
    /// instance running at a time.
    pub(crate) fn get_owned(mut self) -> Option<MetadataGeneration> {
        if self.0.len() == 1 {
            self.0.pop()
        } else {
            None
        }
    }
}

#[cfg(all(test, integration_tests))]
mod integration_tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{
        contains, elements_are, empty, eq, is_false, is_true, len, none, some,
    };

    use crate::test_utils;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn test_new_and_get() {
        binder::ProcessState::start_thread_pool();

        let service_name_1 = test_utils::make_unique_service_name();
        let (_pub1, gen1) = test_utils::create_metadata_generation(service_name_1).await;

        let service_name_2 = test_utils::make_unique_service_name();
        let (_pub2, gen2) = test_utils::create_metadata_generation(service_name_2).await;

        let empty = CurrentMetadataGenerations::new(vec![]);
        expect_that!(empty.get(), none());
        expect_that!(empty.get_owned(), none());

        let single = CurrentMetadataGenerations::new(vec![gen1.clone()]);
        expect_that!(single.get(), some(eq(&gen1)));
        expect_that!(single.clone().get_owned(), some(eq(&gen1)));

        let multiple = CurrentMetadataGenerations::new(vec![gen1.clone(), gen2.clone()]);
        expect_that!(multiple.get(), none());
        expect_that!(multiple.get_owned(), none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn test_handle_update() {
        binder::ProcessState::start_thread_pool();

        let service_name_1 = test_utils::make_unique_service_name();
        let (_pub1, gen1) = test_utils::create_metadata_generation(service_name_1).await;

        let service_name_2 = test_utils::make_unique_service_name();
        let (_pub2, gen2) = test_utils::create_metadata_generation(service_name_2).await;

        let mut generations = CurrentMetadataGenerations::new(vec![]);

        // Add first
        let changed = generations.handle(MetadataGenerationEvent::Update(gen1.clone()));
        expect_that!(changed, is_true());
        expect_that!(generations.0, len(eq(1)));

        // Add second
        let changed2 = generations.handle(MetadataGenerationEvent::Update(gen2.clone()));
        expect_that!(changed2, is_true());
        expect_that!(generations.0, len(eq(2)));

        // Update first (replace)
        let mut gen1_modified = gen1.clone();
        gen1_modified.id = MetadataGenerationId::from(999);
        let changed3 = generations.handle(MetadataGenerationEvent::Update(gen1_modified.clone()));
        expect_that!(changed3, is_true());
        expect_that!(generations.0, len(eq(2)));

        expect_that!(generations.0, contains(eq(&gen1_modified)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn test_handle_remove_by_unit_identifier_and_service_type() {
        binder::ProcessState::start_thread_pool();

        let service_name_1 = test_utils::make_unique_service_name();
        let (_pub1, gen1) = test_utils::create_metadata_generation(service_name_1).await;

        let service_name_2 = test_utils::make_unique_service_name();
        let (_pub2, gen2) = test_utils::create_metadata_generation(service_name_2).await;

        let mut generations =
            CurrentMetadataGenerations::new(vec![gen1.clone(), gen2.clone(), gen1.clone()]);

        let removed =
            generations.handle(MetadataGenerationEvent::RemoveByUnitIdentifierAndServiceType {
                unit_identifier: gen1.unit_identifier.clone(),
                service_type: gen1.service_type,
            });
        expect_that!(removed, is_true());
        expect_that!(generations.0, elements_are![&gen2]);

        // Second removal should return false
        let removed_again =
            generations.handle(MetadataGenerationEvent::RemoveByUnitIdentifierAndServiceType {
                unit_identifier: gen1.unit_identifier.clone(),
                service_type: gen1.service_type,
            });
        expect_that!(removed_again, is_false());
        expect_that!(generations.0, elements_are![&gen2]);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn test_handle_remove_by_id() {
        binder::ProcessState::start_thread_pool();

        let service_name_1 = test_utils::make_unique_service_name();
        let (_pub1, gen1) = test_utils::create_metadata_generation(service_name_1).await;

        let service_name_2 = test_utils::make_unique_service_name();
        let (_pub2, gen2) = test_utils::create_metadata_generation(service_name_2).await;

        let mut generations =
            CurrentMetadataGenerations::new(vec![gen1.clone(), gen2.clone(), gen1.clone()]);

        let removed = generations.handle(MetadataGenerationEvent::RemoveById(gen1.id));
        expect_that!(removed, is_true());
        expect_that!(generations.0, elements_are![&gen2]);

        let removed_again = generations.handle(MetadataGenerationEvent::RemoveById(gen1.id));
        expect_that!(removed_again, is_false());
        expect_that!(generations.0, elements_are![&gen2]);

        let removed = generations.handle(MetadataGenerationEvent::RemoveById(gen2.id));
        expect_that!(removed, is_true());
        expect_that!(generations.0, empty());
    }
}
