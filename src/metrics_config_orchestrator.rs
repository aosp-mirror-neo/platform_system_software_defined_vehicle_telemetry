// Copyright 2023 Google LLC. All Rights Reserved.

mod publisher;
mod publisher_factory;
mod trigger;
mod trigger_factory;

use protobuf::reflect::FileDescriptor;
use sdv_ast::{NodeFactory, NodeFactoryImpl};
use sdvtelemetry_proto_rust::metrics_configuration::{
    MetricsConfig as MetricsConfigProto, Publisher as PublisherProto, Trigger as TriggerProto,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

use crate::collection_extension::HashMapExtension;
use publisher::Publisher;
use publisher_factory::{
    PublisherFactory, PublisherFactoryContext, PublisherFactoryError, PublisherFactoryImpl,
};
use trigger::Trigger;
use trigger_factory::{
    TriggerFactory, TriggerFactoryContext, TriggerFactoryError, TriggerFactoryImpl,
};

/// Interface of a service that orchestrates lifecycle of Metrics Configurations.
pub trait MetricsConfigOrchestrator {
    /// Initialize lifecycle of the specified Metrics Configuration.
    fn initialize_config(
        &self,
        config: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError>;
    /// Finish lifecycle of Metrics Configuration with the specified uuid.
    fn finish_config(&self, config_uuid: &str) -> Result<(), MetricsConfigOrchestratorError>;
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
    #[error("Invalid definition for publisher {publisher_name}")]
    InvalidPublisherDefinition { publisher_name: String, factory_error: PublisherFactoryError },
    #[error("Invalid definition for trigger {trigger_name}")]
    InvalidTriggerDefinition { trigger_name: String, factory_error: TriggerFactoryError },
    #[error("Invalid descriptor protos")]
    InvalidDescriptorProtos {
        #[from]
        error: protobuf::Error,
    },
    #[error("Metric Config with UUID {uuid} doesn't exist")]
    InvalidConfigUuid { uuid: String },
    #[error("Invalid expression nodes. Reason: {error_detail}")]
    InvalidExpressionNodes { error_detail: String },
}

/// Struct for default implementation of MetricsConfigOrchestrator.
pub struct MetricsConfigOrchestratorImpl<
    NdFactory: NodeFactory = NodeFactoryImpl,
    PubFactory: PublisherFactory = PublisherFactoryImpl,
    TrigFactory: TriggerFactory = TriggerFactoryImpl,
> {
    configs: Mutex<HashMap<String, MetricsConfig>>,
    node_factory: NdFactory,
    publisher_factory: PubFactory,
    trigger_factory: TrigFactory,
}

pub struct MetricsConfig {
    publishers: HashMap<String, Arc<Publisher>>,
    triggers: HashMap<String, Arc<Trigger>>,
}

impl MetricsConfigOrchestratorImpl {
    pub fn new() -> Self {
        MetricsConfigOrchestratorImpl {
            configs: Mutex::new(HashMap::new()),
            node_factory: NodeFactoryImpl::new(),
            publisher_factory: PublisherFactoryImpl::new(),
            trigger_factory: TriggerFactoryImpl::new(),
        }
    }
}

impl<NdFactory: NodeFactory, PubFactory: PublisherFactory, TrigFactory: TriggerFactory>
    MetricsConfigOrchestrator
    for MetricsConfigOrchestratorImpl<NdFactory, PubFactory, TrigFactory>
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
        let cloned_descriptor_protos = config_proto.descriptor_protos.to_vec();
        let file_descriptors = FileDescriptor::new_dynamic_fds(cloned_descriptor_protos, &[])?;

        let expression_nodes = self
            .node_factory
            .create_nodes(&config_proto.expression_nodes)
            .map_err(|error_detail| MetricsConfigOrchestratorError::InvalidExpressionNodes {
                error_detail,
            })?;

        let config =
            sorted_vector.iter().try_fold(
                MetricsConfig { publishers: HashMap::new(), triggers: HashMap::new() },
                |mut acc, container_name| {
                    match dependency_map.get(*container_name) {
                        Some(DependencyContainer::Publisher { publisher_proto, .. }) => {
                            let context = PublisherFactoryContext {
                                config: &acc,
                                expression_nodes: &expression_nodes,
                                file_descriptors: &file_descriptors,
                            };
                            match self.publisher_factory.create(publisher_proto, &context) {
                                Ok(publisher) => acc
                                    .publishers
                                    .insert(publisher_proto.name.clone(), publisher.into()),
                                Err(factory_error) => return Err(
                                    MetricsConfigOrchestratorError::InvalidPublisherDefinition {
                                        publisher_name: publisher_proto.name.clone(),
                                        factory_error,
                                    },
                                ),
                            };
                        }
                        Some(DependencyContainer::Trigger { trigger_proto, .. }) => {
                            let context = TriggerFactoryContext {
                                config: &acc,
                                expression_nodes: &expression_nodes,
                            };
                            match self.trigger_factory.create(trigger_proto, &context) {
                                Ok(trigger) => {
                                    acc.triggers.insert(container_name.to_string(), trigger.into())
                                }
                                Err(factory_error) => {
                                    return Err(
                                        MetricsConfigOrchestratorError::InvalidTriggerDefinition {
                                            trigger_name: trigger_proto.name.clone(),
                                            factory_error,
                                        },
                                    )
                                }
                            };
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

    fn finish_config(&self, config_uuid: &str) -> Result<(), MetricsConfigOrchestratorError> {
        let mut configs = self.configs.lock().unwrap();
        match configs.remove(config_uuid) {
            Some(_) => Ok(()),
            _ => Err(MetricsConfigOrchestratorError::InvalidConfigUuid {
                uuid: config_uuid.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use sdv_ast::{Node, NodeValue};
    use sdvtelemetry_proto_rust::expression::Node as NodeProto;
    use sdvtelemetry_proto_rust::metrics_configuration::{
        AggregationPublisher as AggregationPublisherProto,
        ConditionalTrigger as ConditionalTriggerProto, DataTrigger as DataTriggerProto,
        MetricsConfig as MetricsConfigProto, PeriodicTrigger as PeriodicTriggerProto,
        Publisher as PublisherProto, ServicePublisher as ServicePublisherProto,
        SignalPublisher as SignalPublisherProto, Trigger as TriggerProto,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::metrics_config_orchestrator::publisher::PublisherType;
    use crate::metrics_config_orchestrator::publisher_factory::{
        PublisherFactory, PublisherFactoryError,
    };
    use crate::metrics_config_orchestrator::trigger::TriggerType;
    use crate::metrics_config_orchestrator::trigger_factory::TriggerFactory;
    use crate::MetricsConfigOrchestratorImpl;

    const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

    struct MockNodeFactory {
        fail_create_nodes: bool,
        num_nodes: Mutex<usize>,
    }

    impl MockNodeFactory {
        fn new() -> Self {
            MockNodeFactory { fail_create_nodes: false, num_nodes: Mutex::new(0) }
        }

        /// Cause `create_nodes` method to return an error.
        fn fail_create_nodes(&mut self) {
            self.fail_create_nodes = true;
        }
    }

    impl NodeFactory for MockNodeFactory {
        fn create_nodes(&self, proto_nodes: &[NodeProto]) -> Result<Vec<Arc<Node>>, String> {
            if self.fail_create_nodes {
                return Err("Simulated failure".to_string());
            }
            *self.num_nodes.lock().unwrap() = proto_nodes.len();
            Ok({
                let mut v = Vec::with_capacity(proto_nodes.len());
                (0..proto_nodes.len()).for_each(|_| v.push(Arc::new(create_node())));
                v
            })
        }
    }

    struct MockPublisherFactory {
        publisher_protos: Mutex<Vec<PublisherProto>>,
    }

    impl MockPublisherFactory {
        fn new() -> Self {
            MockPublisherFactory { publisher_protos: Mutex::new(Vec::new()) }
        }
    }

    impl PublisherFactory for MockPublisherFactory {
        fn create(
            &self,
            publisher_proto: &PublisherProto,
            _context: &PublisherFactoryContext,
        ) -> Result<Publisher, PublisherFactoryError> {
            self.publisher_protos.lock().unwrap().push(publisher_proto.clone());
            Ok(Publisher {
                name: publisher_proto.name.clone(),
                publisher_type: PublisherType::Service { service_name: "Foo".to_string() },
            })
        }

        fn validate(
            &self,
            _publisher_proto: &PublisherProto,
            _context: &PublisherFactoryContext,
        ) -> Result<(), PublisherFactoryError> {
            Ok(())
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
        fn create(
            &self,
            trigger_proto: &TriggerProto,
            _context: &TriggerFactoryContext,
        ) -> Result<Trigger, TriggerFactoryError> {
            self.trigger_protos.lock().unwrap().push(trigger_proto.clone());
            Ok(Trigger::new(
                trigger_proto.name.clone(),
                TriggerType::Periodic { interval_ms: u64::MAX },
            ))
        }

        fn validate(
            &self,
            _trigger_proto: &TriggerProto,
            _context: &TriggerFactoryContext,
        ) -> Result<(), TriggerFactoryError> {
            Ok(())
        }
    }

    fn create_orchestrator(
    ) -> MetricsConfigOrchestratorImpl<MockNodeFactory, MockPublisherFactory, MockTriggerFactory>
    {
        MetricsConfigOrchestratorImpl {
            configs: Mutex::new(HashMap::new()),
            node_factory: MockNodeFactory::new(),
            publisher_factory: MockPublisherFactory::new(),
            trigger_factory: MockTriggerFactory::new(),
        }
    }

    fn create_node() -> Node {
        Node::ConstantLeaf { value: NodeValue::Bool(false) }
    }

    #[test]
    fn initialize_config_duplicate_publisher_name_fails() {
        const NAME: &str = "Publisher1";
        let orchestrator = create_orchestrator();

        let mut publisher1 = PublisherProto::new();
        publisher1.name = NAME.to_owned();
        publisher1.set_signal_publisher(SignalPublisherProto::new());

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
    fn initialize_config_invalid_expression_nodes_fails() {
        let mut orchestrator = create_orchestrator();
        orchestrator.node_factory.fail_create_nodes();

        let mut config = MetricsConfigProto::new();
        config.expression_nodes.push(NodeProto::new());

        assert!(matches!(
            orchestrator.initialize_config(&config),
            Err(MetricsConfigOrchestratorError::InvalidExpressionNodes { .. })
        ));
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
        config.expression_nodes.push(NodeProto::new());

        assert!(orchestrator.initialize_config(&config).is_ok());
        assert_eq!(*orchestrator.node_factory.num_nodes.lock().unwrap(), 1);
        assert_eq!(orchestrator.publisher_factory.publisher_protos.lock().unwrap().len(), 2);
        assert_eq!(orchestrator.trigger_factory.trigger_protos.lock().unwrap().len(), 2);
        let contexts = orchestrator.configs.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        let context = &contexts[&config.uuid];
        assert_eq!(context.publishers.len(), 2);
        assert_eq!(context.triggers.len(), 2);
    }

    #[test]
    fn finish_config_invalid_config_uuid_fails() {
        let orchestrator = create_orchestrator();

        let err = orchestrator.finish_config(UUID).err().unwrap();
        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidConfigUuid { uuid } if uuid == UUID)
        )
    }

    #[test]
    fn finish_config_success() {
        let orchestrator = create_orchestrator();

        let mut metrics_config = MetricsConfigProto::new();
        metrics_config.uuid = UUID.to_owned();
        orchestrator.initialize_config(&metrics_config).unwrap();

        assert!(orchestrator.finish_config(UUID).is_ok());
    }
}
