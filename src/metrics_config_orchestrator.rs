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
pub struct MetricsConfigOrchestratorImpl<
    PubFactory: PublisherFactory = PublisherFactoryImpl,
    TrigFactory: TriggerFactory = TriggerFactoryImpl,
> {
    configs: Mutex<HashMap<String, MetricsConfig>>,
    publisher_factory: PubFactory,
    trigger_factory: TrigFactory,
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

impl<PubFactory: PublisherFactory, TrigFactory: TriggerFactory> MetricsConfigOrchestrator
    for MetricsConfigOrchestratorImpl<PubFactory, TrigFactory>
{
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

#[cfg(test)]
mod tests {
    use sdvtelemetry_proto_rust::metrics_configuration::{
        AggregationPublisher as AggregationPublisherProto,
        ConditionalTrigger as ConditionalTriggerProto, DataTrigger as DataTriggerProto,
        MetricsConfig as MetricsConfigProto, PeriodicTrigger as PeriodicTriggerProto,
        Publisher as PublisherProto, ServicePublisher as ServicePublisherProto,
        TaxoPublisher as TaxoPublisherProto, Trigger as TriggerProto,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::metrics_config_orchestrator::publisher::{
        Publisher, PublisherFactory, PublisherType,
    };
    use crate::metrics_config_orchestrator::trigger::{Trigger, TriggerFactory, TriggerType};
    use crate::MetricsConfigOrchestratorImpl;

    struct MockPublisherFactory {
        publisher_protos: Mutex<Vec<PublisherProto>>,
    }

    impl MockPublisherFactory {
        fn new() -> Self {
            MockPublisherFactory { publisher_protos: Mutex::new(Vec::new()) }
        }
    }

    impl PublisherFactory for MockPublisherFactory {
        fn create(&self, publisher_proto: &PublisherProto, _context: &MetricsConfig) -> Publisher {
            self.publisher_protos.lock().unwrap().push(publisher_proto.clone());
            Publisher {
                name: publisher_proto.name.clone(),
                publisher_type: PublisherType::Service {},
            }
        }
    }

    struct MockTriggerFactory {
        trigger_protos: Mutex<Vec<TriggerProto>>,
    }

    impl MockTriggerFactory {
        fn new() -> Self {
            MockTriggerFactory { trigger_protos: Mutex::new(Vec::new()) }
        }
    }

    impl TriggerFactory for MockTriggerFactory {
        fn create(&self, trigger_proto: &TriggerProto, _context: &MetricsConfig) -> Trigger {
            self.trigger_protos.lock().unwrap().push(trigger_proto.clone());
            Trigger {
                name: trigger_proto.name.clone(),
                trigger_type: TriggerType::Periodic { interval_ms: u32::MAX },
            }
        }
    }

    fn create_orchestrator(
    ) -> MetricsConfigOrchestratorImpl<MockPublisherFactory, MockTriggerFactory> {
        MetricsConfigOrchestratorImpl {
            configs: Mutex::new(HashMap::new()),
            publisher_factory: MockPublisherFactory::new(),
            trigger_factory: MockTriggerFactory::new(),
        }
    }

    #[test]
    fn initialize_config_duplicate_publisher_name_fails() {
        const NAME: &str = "Publisher1";
        let orchestrator = create_orchestrator();

        let mut publisher1 = PublisherProto::new();
        publisher1.name = NAME.to_owned();
        publisher1.set_taxo_publisher(TaxoPublisherProto::new());

        let mut publisher2 = PublisherProto::new();
        publisher2.name = publisher1.name.clone();
        publisher2.set_service_publisher(ServicePublisherProto::new());

        let mut config = MetricsConfigProto::new();
        config.publishers.push(publisher1);
        config.publishers.push(publisher2);

        assert!(matches!(
            orchestrator.initialize_config(&config),
            Err(MetricsConfigOrchestratorError::DuplicatePublisherDefinition { publisher_name }) if publisher_name == NAME
        ));
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_duplicate_trigger_name_fails() {
        const NAME: &str = "Trigger1";
        let orchestrator = create_orchestrator();

        let mut trigger1 = TriggerProto::new();
        trigger1.name = NAME.to_owned();
        trigger1.set_periodic_trigger(PeriodicTriggerProto::new());

        let mut trigger2 = TriggerProto::new();
        trigger2.name = trigger1.name.clone();
        trigger2.set_data_trigger(DataTriggerProto::new());

        let mut config = MetricsConfigProto::new();
        config.triggers.push(trigger1);
        config.triggers.push(trigger2);

        assert!(matches!(
            orchestrator.initialize_config(&config),
            Err(MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name }) if trigger_name == NAME
        ));
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_duplicate_publisher_trigger_name_fails() {
        const NAME: &str = "Publisher";
        let orchestrator = create_orchestrator();

        let mut publisher = PublisherProto::new();
        publisher.name = NAME.to_owned();
        publisher.set_service_publisher(ServicePublisherProto::new());

        let mut trigger = TriggerProto::new();
        trigger.name = publisher.name.clone();
        trigger.set_periodic_trigger(PeriodicTriggerProto::new());

        let mut config = MetricsConfigProto::new();
        config.publishers.push(publisher);
        config.triggers.push(trigger);

        assert!(matches!(
            orchestrator.initialize_config(&config),
            Err(MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name }) if trigger_name == NAME
        ));
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_publisher_trigger_cycle_fails() {
        let orchestrator = create_orchestrator();

        let mut trigger1 = TriggerProto::new();
        trigger1.name = "DataTrigger".to_owned();
        let mut trigger2 = TriggerProto::new();
        trigger2.name = "ConditionalTrigger".to_owned();
        let mut publisher = PublisherProto::new();
        publisher.name = "AggregationPublisher".to_owned();

        let mut data_trigger = DataTriggerProto::new();
        data_trigger.publisher_name = publisher.name.clone();
        trigger1.set_data_trigger(data_trigger);
        let mut conditional_trigger = ConditionalTriggerProto::new();
        conditional_trigger.trigger_names.push(trigger1.name.clone());
        trigger2.set_conditional_trigger(conditional_trigger);
        let mut aggregation_publisher = AggregationPublisherProto::new();
        aggregation_publisher.trigger_names.push(trigger2.name.clone());
        publisher.set_aggregation_publisher(aggregation_publisher);

        let mut config = MetricsConfigProto::new();
        config.publishers.push(publisher);
        config.triggers.push(trigger1);
        config.triggers.push(trigger2);

        assert!(matches!(
            orchestrator.initialize_config(&config),
            Err(MetricsConfigOrchestratorError::CycledPublishersTriggers)
        ));
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_publisher_missing_dependency_fails() {
        const INVALID_TRIGGER_NAME: &str = "Trigger";
        let orchestrator = create_orchestrator();

        let mut publisher = PublisherProto::new();
        publisher.name = "AggregationPublisher".to_owned();

        let mut aggregation_publisher = AggregationPublisherProto::new();
        aggregation_publisher.trigger_names.push(INVALID_TRIGGER_NAME.to_owned());
        publisher.set_aggregation_publisher(aggregation_publisher);

        let mut config = MetricsConfigProto::new();
        config.publishers.push(publisher);

        assert!(matches!(
            orchestrator.initialize_config(&config),
            Err(MetricsConfigOrchestratorError::InvalidDependency { dependency_name }) if dependency_name == INVALID_TRIGGER_NAME
        ));
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 1);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_duplicate_uuid_fails() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        let orchestrator = create_orchestrator();

        let mut config1 = MetricsConfigProto::new();
        config1.uuid = UUID.to_owned();
        let mut config2 = MetricsConfigProto::new();
        config2.uuid = config1.uuid.clone();

        assert!(orchestrator.initialize_config(&config1).is_ok());
        assert!(matches!(
            orchestrator.initialize_config(&config2),
            Err(MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid }) if uuid == UUID
        ));
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 0);
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 1);
    }

    #[test]
    fn initialize_config_success() {
        let orchestrator = create_orchestrator();

        let mut trigger1 = TriggerProto::new();
        trigger1.name = "DataTrigger".to_owned();
        let mut trigger2 = TriggerProto::new();
        trigger2.name = "ConditionalTrigger".to_owned();
        let mut publisher1 = PublisherProto::new();
        publisher1.name = "ServicePublisher".to_owned();
        let mut publisher2 = PublisherProto::new();
        publisher2.name = "AggregationPublisher".to_owned();

        let mut data_trigger = DataTriggerProto::new();
        data_trigger.publisher_name = publisher1.name.clone();
        trigger1.set_data_trigger(data_trigger);
        let mut conditional_trigger = ConditionalTriggerProto::new();
        conditional_trigger.trigger_names.push(trigger1.name.clone());
        trigger2.set_conditional_trigger(conditional_trigger);
        publisher1.set_service_publisher(ServicePublisherProto::new());

        let mut aggregation_publisher = AggregationPublisherProto::new();
        aggregation_publisher.trigger_names.push(trigger2.name.clone());
        publisher2.set_aggregation_publisher(aggregation_publisher);

        let mut config = MetricsConfigProto::new();
        config.uuid = "01bf0754-df95-46bb-bb35-522b6af66600".to_owned();
        config.publishers.push(publisher1);
        config.publishers.push(publisher2);
        config.triggers.push(trigger1);
        config.triggers.push(trigger2);

        assert!(orchestrator.initialize_config(&config).is_ok());
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 2);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 2);
        let contexts = orchestrator.configs.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        let context = &contexts[&config.uuid];
        assert_eq!(context.publishers.len(), 2);
        assert_eq!(context.triggers.len(), 2);
    }
}
