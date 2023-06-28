// Copyright 2023 Google LLC. All Rights Reserved.

mod publisher;
mod trigger;

use sdvtelemetry_proto_rust::metrics_configuration::{
    MetricsConfig as MetricsConfigProto, Publisher as PublisherProto, Trigger as TriggerProto,
};
use std::{collections::HashMap, sync::Mutex};
use thiserror::Error;

use crate::collection_extension::HashMapExtension;
use publisher::{Publisher, PublisherFactory, PublisherFactoryImpl};
use trigger::{Trigger, TriggerFactory, TriggerFactoryImpl};

/// Interface of a service that orchestrates lifecycle of Metrics Configurations.
pub trait MetricsConfigOrchestrator {
    /// Initialize lifecycle of the specified Metrics Configuration.
    fn initialize_config(
        &self,
        config: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError>;
    /// Finish lifecycle of Metrics Configuration with the specified uuid.
    fn finish_config(&self, config_uuid: &str);
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsConfigOrchestratorError {
    #[error("Duplicate definition of publisher {publisher_name}")]
    DuplicatePublisherDefinition { publisher_name: String },
    #[error("Duplicate definition of trigger {trigger_name}")]
    DuplicateTriggerDefinition { trigger_name: String },
    #[error("Cycle detected in the publisher and trigger dependency graph")]
    CycledPublishersTriggers,
    #[error("Invalid dependency: {dependency_name} doesn't exist")]
    InvalidDependency { dependency_name: String },
    #[error("Duplicate metrics config identifier: {uuid}")]
    DuplicateMetricsConfigUuid { uuid: String },
}

/// Struct for default implementation of MetricsConfigOrchestrator.
pub struct MetricsConfigOrchestratorImpl {
    configs: Mutex<HashMap<String, MetricsConfig>>,
    publisher_factory: PublisherFactoryImpl,
    trigger_factory: TriggerFactoryImpl,
}

pub struct MetricsConfig {
    publishers: HashMap<String, Publisher>,
    triggers: HashMap<String, Trigger>,
}

impl MetricsConfigOrchestratorImpl {
    pub fn new() -> Self {
        MetricsConfigOrchestratorImpl {
            configs: Mutex::new(HashMap::new()),
            publisher_factory: PublisherFactoryImpl {},
            trigger_factory: TriggerFactoryImpl {},
        }
    }
}

impl MetricsConfigOrchestrator for MetricsConfigOrchestratorImpl {
    fn initialize_config(
        &self,
        config_proto: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError> {
        enum DependencyContainer<'a> {
            Publisher { publisher_proto: &'a PublisherProto, dependencies: Vec<&'a String> },
            Trigger { trigger_proto: &'a TriggerProto, dependencies: Vec<&'a String> },
        }

        // Build dependency map for topological sorting.
        let dependency_map = config_proto.publishers.iter().try_fold(
            HashMap::new(),
            |mut acc, publisher_proto| {
                acc.try_insert_value(
                    &publisher_proto.name,
                    DependencyContainer::Publisher {
                        publisher_proto,
                        dependencies: if publisher_proto.has_aggregation_publisher() {
                            publisher_proto.aggregation_publisher().trigger_names.iter().collect()
                        } else {
                            vec![]
                        },
                    },
                )
                .map_or_else(
                    |()| {
                        Err(MetricsConfigOrchestratorError::DuplicatePublisherDefinition {
                            publisher_name: publisher_proto.name.clone(),
                        })
                    },
                    |()| Ok(acc),
                )
            },
        )?;

        let dependency_map =
            config_proto.triggers.iter().try_fold(dependency_map, |mut acc, trigger_proto| {
                acc.try_insert_value(
                    &trigger_proto.name,
                    DependencyContainer::Trigger {
                        trigger_proto,
                        dependencies: if trigger_proto.has_data_trigger() {
                            vec![&trigger_proto.data_trigger().publisher_name]
                        } else if trigger_proto.has_conditional_trigger() {
                            trigger_proto.conditional_trigger().trigger_names.iter().collect()
                        } else {
                            vec![]
                        },
                    },
                )
                .map_or_else(
                    |()| {
                        Err(MetricsConfigOrchestratorError::DuplicateTriggerDefinition {
                            trigger_name: trigger_proto.name.clone(),
                        })
                    },
                    |()| Ok(acc),
                )
            })?;

        // Retrieve topologically sorted vector of keys.
        let sorted_vector = dependency_map
            .to_topological_sorted_vec(|dependency| match dependency {
                DependencyContainer::Trigger { dependencies, .. } => dependencies,
                DependencyContainer::Publisher { dependencies, .. } => dependencies,
            })
            .ok_or(MetricsConfigOrchestratorError::CycledPublishersTriggers)?;

        // Iterate over topologically sorted vector of keys and create publisher and trigger encapsulated objects.
        // Sorted order ensures that all dependencies of the entity are met by the time iterator reaches it.
        let config = sorted_vector.iter().try_fold(
            MetricsConfig { publishers: HashMap::new(), triggers: HashMap::new() },
            |mut acc, container_name| {
                match dependency_map.get(*container_name) {
                    Some(DependencyContainer::Publisher { publisher_proto, .. }) => {
                        let container = self.publisher_factory.create(publisher_proto, &acc);
                        acc.publishers.insert(container_name.to_string(), container);
                    }
                    Some(DependencyContainer::Trigger { trigger_proto, .. }) => {
                        let container = self.trigger_factory.create(trigger_proto, &acc);
                        acc.triggers.insert(container_name.to_string(), container);
                    }
                    None => {
                        return Err(MetricsConfigOrchestratorError::InvalidDependency {
                            dependency_name: container_name.to_string(),
                        })
                    }
                };
                Ok(acc)
            },
        )?;

        // Store MetricsConfig with encapsulated objects.
        let mut configs = self.configs.lock().unwrap();
        if configs.try_insert_value(config_proto.uuid.clone(), config).is_err() {
            return Err(MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid {
                uuid: config_proto.uuid.clone(),
            });
        }

        Ok(())
    }

    #[allow(unused_variables)]
    fn finish_config(&self, config_uuid: &str) {}
}
