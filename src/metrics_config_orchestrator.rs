// Copyright 2023 Google LLC. All Rights Reserved.

mod publisher;
mod publisher_factory;
mod trigger;
mod trigger_factory;

use protobuf::reflect::FileDescriptor;
use sdv_ast::{Node, NodeFactory, NodeFactoryImpl};
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto, trigger::Trigger_type as TriggerTypeProto,
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

    /// Verify that Metrics Configuration is valid.
    fn validate_config(
        &self,
        config: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError>;
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
pub(super) struct MetricsConfigOrchestratorImpl<
    NdFactory: NodeFactory = NodeFactoryImpl,
    PubFactory: PublisherFactory = PublisherFactoryImpl,
    TrigFactory: TriggerFactory = TriggerFactoryImpl,
> {
    configs: Mutex<HashMap<String, MetricsConfig>>,
    node_factory: NdFactory,
    publisher_factory: PubFactory,
    trigger_factory: TrigFactory,
}

pub(super) struct MetricsConfig {
    publishers: HashMap<String, Arc<Publisher>>,
    triggers: HashMap<String, Arc<Trigger>>,
}

impl MetricsConfigOrchestratorImpl {
    pub(super) fn new() -> Self {
        MetricsConfigOrchestratorImpl {
            configs: Mutex::new(HashMap::new()),
            node_factory: NodeFactoryImpl::new(),
            publisher_factory: PublisherFactoryImpl::new(),
            trigger_factory: TriggerFactoryImpl::new(),
        }
    }
}

impl<NdFactory: NodeFactory, PubFactory: PublisherFactory, TrigFactory: TriggerFactory>
    MetricsConfigOrchestratorImpl<NdFactory, PubFactory, TrigFactory>
{
    fn create_expression_nodes(
        &self,
        config_proto: &MetricsConfigProto,
    ) -> Result<Vec<Arc<Node>>, MetricsConfigOrchestratorError> {
        self.node_factory.create_nodes(&config_proto.expression_nodes).map_err(|error_detail| {
            MetricsConfigOrchestratorError::InvalidExpressionNodes { error_detail }
        })
    }

    /// Topologically sort publishers and triggers declared in the protobuf message
    /// and try to fold the sorted list using provided functions.
    fn try_fold_topoligically_sorted_publishers_triggers(
        config_proto: &MetricsConfigProto,
        publisher_func: impl Fn(
            &PublisherProto,
            &mut MetricsConfig,
        ) -> Result<(), MetricsConfigOrchestratorError>,
        trigger_func: impl Fn(
            &TriggerProto,
            &mut MetricsConfig,
        ) -> Result<(), MetricsConfigOrchestratorError>,
    ) -> Result<MetricsConfig, MetricsConfigOrchestratorError> {
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
                        dependencies: if let Some(PublisherTypeProto::AggregationPublisher(
                            aggregation_publisher,
                        )) = &publisher_proto.publisher_type
                        {
                            aggregation_publisher.trigger_names.iter().collect()
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
                        dependencies: match &trigger_proto.trigger_type {
                            Some(TriggerTypeProto::DataTrigger(data_trigger)) => {
                                if let Some(DependencyContainer::Publisher { .. }) =
                                    acc.get(&data_trigger.publisher_name)
                                {
                                    vec![&data_trigger.publisher_name]
                                } else {
                                    return Err(
                                        MetricsConfigOrchestratorError::InvalidDependency {
                                            dependency_name: data_trigger.publisher_name.clone(),
                                        },
                                    );
                                }
                            }
                            Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                                conditional_trigger.trigger_names.iter().collect()
                            }
                            _ => vec![],
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

        sorted_vector.iter().rev().try_fold(
            MetricsConfig { publishers: HashMap::new(), triggers: HashMap::new() },
            |mut config, container_name| {
                match dependency_map.get(*container_name) {
                    Some(DependencyContainer::Publisher { publisher_proto, .. }) => {
                        publisher_func(publisher_proto, &mut config)
                    }
                    Some(DependencyContainer::Trigger { trigger_proto, .. }) => {
                        trigger_func(trigger_proto, &mut config)
                    }
                    None => Err(MetricsConfigOrchestratorError::InvalidDependency {
                        dependency_name: container_name.to_string(),
                    }),
                }?;
                Ok(config)
            },
        )
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
        // Iterate over topologically sorted vector of publishers and triggers and create encapsulated objects.
        // Sorted order ensures that all dependencies of the entity are met by the time iterator reaches it.
        let cloned_descriptor_protos = config_proto.descriptor_protos.to_vec();
        let file_descriptors = FileDescriptor::new_dynamic_fds(cloned_descriptor_protos, &[])?;
        let expression_nodes = self.create_expression_nodes(config_proto)?;

        let config = Self::try_fold_topoligically_sorted_publishers_triggers(
            config_proto,
            |publisher_proto, config| {
                let context = PublisherFactoryContext {
                    config,
                    expression_nodes: &expression_nodes,
                    file_descriptors: &file_descriptors,
                };
                self.publisher_factory.create(publisher_proto, &context).map_or_else(
                    |factory_error| {
                        Err(MetricsConfigOrchestratorError::InvalidPublisherDefinition {
                            publisher_name: publisher_proto.name.clone(),
                            factory_error,
                        })
                    },
                    |publisher| {
                        config.publishers.insert(publisher_proto.name.clone(), publisher.into());
                        Ok(())
                    },
                )
            },
            |trigger_proto, config| {
                let context = TriggerFactoryContext { config, expression_nodes: &expression_nodes };
                self.trigger_factory.create(trigger_proto, &context).map_or_else(
                    |factory_error| {
                        Err(MetricsConfigOrchestratorError::InvalidTriggerDefinition {
                            trigger_name: trigger_proto.name.clone(),
                            factory_error,
                        })
                    },
                    |trigger| {
                        config.triggers.insert(trigger_proto.name.clone(), trigger.into());
                        Ok(())
                    },
                )
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

    fn validate_config(
        &self,
        config_proto: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError> {
        {
            let configs = self.configs.lock().unwrap();
            if configs.contains_key(&config_proto.uuid) {
                return Err(MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid {
                    uuid: config_proto.uuid.clone(),
                });
            }
        }

        // Topologically sort publishers and triggers to ensure dependencies exist and there are no cycles.
        let cloned_descriptor_protos = config_proto.descriptor_protos.to_vec();
        let file_descriptors = FileDescriptor::new_dynamic_fds(cloned_descriptor_protos, &[])?;
        let expression_nodes = self.create_expression_nodes(config_proto)?;

        Self::try_fold_topoligically_sorted_publishers_triggers(
            config_proto,
            |publisher_proto, config| {
                let context = PublisherFactoryContext {
                    config,
                    expression_nodes: &expression_nodes,
                    file_descriptors: &file_descriptors,
                };
                self.publisher_factory.validate(publisher_proto, &context).map_err(|e| {
                    MetricsConfigOrchestratorError::InvalidPublisherDefinition {
                        publisher_name: publisher_proto.name.clone(),
                        factory_error: e,
                    }
                })?;
                Ok(())
            },
            |trigger_proto, config| {
                let context = TriggerFactoryContext { config, expression_nodes: &expression_nodes };
                self.trigger_factory.validate(trigger_proto, &context).map_err(|e| {
                    MetricsConfigOrchestratorError::InvalidTriggerDefinition {
                        trigger_name: trigger_proto.name.clone(),
                        factory_error: e,
                    }
                })?;
                Ok(())
            },
        )?;

        Ok(())
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

    type MockedMetricsConfigOrchestratorImpl =
        MetricsConfigOrchestratorImpl<MockNodeFactory, MockPublisherFactory, MockTriggerFactory>;

    const AGGREGATION_PUBLISHER_NAME: &str = "AggregationPublisher1";
    const CONDITIONAL_TRIGGER_NAME: &str = "ConditionalTrigger1";
    const DATA_TRIGGER_NAME: &str = "DataTrigger1";
    const INVALID_PUBLISHER_NAME: &str = "Publisher~0";
    const INVALID_TRIGGER_NAME: &str = "Trigger~0";
    const SERVICE_PUBLISHER_NAME: &str = "ServicePublisher1";
    const SOME_PUBLISHER_NAME: &str = "Publisher1";
    const SOME_TRIGGER_NAME: &str = "Trigger1";
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
                (0..proto_nodes.len()).for_each(|_| {
                    v.push(Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(false) }))
                });
                v
            })
        }
    }

    struct MockPublisherFactory {
        create_callback: Option<
            Box<dyn Fn(&PublisherProto) -> Option<Result<Publisher, PublisherFactoryError>>>,
        >,
        validate_callback:
            Option<Box<dyn Fn(&PublisherProto) -> Option<Result<(), PublisherFactoryError>>>>,
    }

    impl MockPublisherFactory {
        fn new() -> Self {
            MockPublisherFactory { create_callback: None, validate_callback: None }
        }

        fn set_create_callback(
            &mut self,
            create_callback: impl Fn(&PublisherProto) -> Option<Result<Publisher, PublisherFactoryError>>
                + 'static,
        ) {
            self.create_callback = Some(Box::new(create_callback)
                as Box<
                    dyn Fn(&PublisherProto) -> Option<Result<Publisher, PublisherFactoryError>>,
                >);
        }

        fn set_validate_callback(
            &mut self,
            validate_callback: impl Fn(&PublisherProto) -> Option<Result<(), PublisherFactoryError>>
                + 'static,
        ) {
            self.validate_callback = Some(Box::new(validate_callback)
                as Box<dyn Fn(&PublisherProto) -> Option<Result<(), PublisherFactoryError>>>);
        }
    }

    impl PublisherFactory for MockPublisherFactory {
        fn create(
            &self,
            publisher_proto: &PublisherProto,
            _context: &PublisherFactoryContext,
        ) -> Result<Publisher, PublisherFactoryError> {
            if let Some(callback) = &self.create_callback {
                if let Some(res) = callback(publisher_proto) {
                    return res;
                }
            }
            Ok(Publisher {
                name: publisher_proto.name.clone(),
                publisher_type: PublisherType::Service { service_name: "Foo".to_string() },
            })
        }

        fn validate(
            &self,
            publisher_proto: &PublisherProto,
            _context: &PublisherFactoryContext,
        ) -> Result<(), PublisherFactoryError> {
            if let Some(callback) = &self.validate_callback {
                if let Some(res) = callback(publisher_proto) {
                    return res;
                }
            }
            Ok(())
        }
    }

    struct MockTriggerFactory {
        create_callback:
            Option<Box<dyn Fn(&TriggerProto) -> Option<Result<Trigger, TriggerFactoryError>>>>,
        validate_callback:
            Option<Box<dyn Fn(&TriggerProto) -> Option<Result<(), TriggerFactoryError>>>>,
    }

    impl MockTriggerFactory {
        fn new() -> Self {
            MockTriggerFactory { create_callback: None, validate_callback: None }
        }

        fn set_create_callback(
            &mut self,
            create_callback: impl Fn(&TriggerProto) -> Option<Result<Trigger, TriggerFactoryError>>
                + 'static,
        ) {
            self.create_callback = Some(Box::new(create_callback)
                as Box<dyn Fn(&TriggerProto) -> Option<Result<Trigger, TriggerFactoryError>>>);
        }

        fn set_validate_callback(
            &mut self,
            validate_callback: impl Fn(&TriggerProto) -> Option<Result<(), TriggerFactoryError>>
                + 'static,
        ) {
            self.validate_callback = Some(Box::new(validate_callback)
                as Box<dyn Fn(&TriggerProto) -> Option<Result<(), TriggerFactoryError>>>);
        }
    }

    impl TriggerFactory for MockTriggerFactory {
        fn create(
            &self,
            trigger_proto: &TriggerProto,
            _context: &TriggerFactoryContext,
        ) -> Result<Trigger, TriggerFactoryError> {
            if let Some(callback) = &self.create_callback {
                if let Some(res) = callback(trigger_proto) {
                    return res;
                }
            }
            Ok(Trigger::new(
                trigger_proto.name.clone(),
                TriggerType::Periodic { interval_ms: u64::MAX },
            ))
        }

        fn validate(
            &self,
            trigger_proto: &TriggerProto,
            _context: &TriggerFactoryContext,
        ) -> Result<(), TriggerFactoryError> {
            if let Some(callback) = &self.validate_callback {
                if let Some(res) = callback(trigger_proto) {
                    return res;
                }
            }
            Ok(())
        }
    }

    impl MockedMetricsConfigOrchestratorImpl {
        fn new() -> Self {
            MetricsConfigOrchestratorImpl {
                configs: Mutex::new(HashMap::new()),
                node_factory: MockNodeFactory::new(),
                publisher_factory: MockPublisherFactory::new(),
                trigger_factory: MockTriggerFactory::new(),
            }
        }
    }

    fn create_valid_config_with_publisher() -> MetricsConfigProto {
        let mut publisher = PublisherProto::new();
        publisher.name = SOME_PUBLISHER_NAME.to_owned();
        publisher.set_service_publisher(ServicePublisherProto::new());

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.publishers.push(publisher);
        config
    }

    fn create_valid_config_with_trigger() -> MetricsConfigProto {
        let mut trigger = TriggerProto::new();
        trigger.name = SOME_TRIGGER_NAME.to_owned();
        trigger.set_periodic_trigger(PeriodicTriggerProto::new());

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.triggers.push(trigger);
        config
    }

    fn create_config_with_duplicate_publisher_name() -> MetricsConfigProto {
        let mut publisher1 = PublisherProto::new();
        publisher1.name = SOME_PUBLISHER_NAME.to_owned();
        publisher1.set_signal_publisher(SignalPublisherProto::new());

        let mut publisher2 = PublisherProto::new();
        publisher2.name = publisher1.name.clone();
        publisher2.set_service_publisher(ServicePublisherProto::new());

        let mut config = MetricsConfigProto::new();
        config.publishers.push(publisher1);
        config.publishers.push(publisher2);
        config
    }

    fn create_config_with_duplicate_trigger_name() -> MetricsConfigProto {
        let mut trigger1 = TriggerProto::new();
        trigger1.name = SOME_TRIGGER_NAME.to_owned();
        trigger1.set_periodic_trigger(PeriodicTriggerProto::new());

        let mut trigger2 = TriggerProto::new();
        trigger2.name = trigger1.name.clone();
        trigger2.set_periodic_trigger(PeriodicTriggerProto::new());

        let mut config = MetricsConfigProto::new();
        config.triggers.push(trigger1);
        config.triggers.push(trigger2);
        config
    }

    fn create_config_with_duplicate_publisher_trigger_name() -> MetricsConfigProto {
        let mut publisher = PublisherProto::new();
        publisher.name = SOME_PUBLISHER_NAME.to_owned();
        publisher.set_service_publisher(ServicePublisherProto::new());

        let mut trigger = TriggerProto::new();
        trigger.name = publisher.name.clone();
        trigger.set_periodic_trigger(PeriodicTriggerProto::new());

        let mut config = MetricsConfigProto::new();
        config.publishers.push(publisher);
        config.triggers.push(trigger);
        config
    }

    fn create_config_with_publisher_trigger_cycle() -> MetricsConfigProto {
        let mut trigger1 = TriggerProto::new();
        trigger1.name = DATA_TRIGGER_NAME.to_owned();
        let mut trigger2 = TriggerProto::new();
        trigger2.name = CONDITIONAL_TRIGGER_NAME.to_owned();
        let mut publisher = PublisherProto::new();
        publisher.name = AGGREGATION_PUBLISHER_NAME.to_owned();

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
        config
    }

    fn create_config_with_invalid_aggregation_publisher_trigger() -> MetricsConfigProto {
        let mut publisher = PublisherProto::new();
        publisher.name = AGGREGATION_PUBLISHER_NAME.to_owned();

        let mut aggregation_publisher = AggregationPublisherProto::new();
        aggregation_publisher.trigger_names.push(INVALID_TRIGGER_NAME.to_owned());
        publisher.set_aggregation_publisher(aggregation_publisher);

        let mut config = MetricsConfigProto::new();
        config.publishers.push(publisher);
        config
    }

    fn create_config_with_invalid_data_trigger_publisher() -> MetricsConfigProto {
        let mut trigger = TriggerProto::new();
        trigger.name = DATA_TRIGGER_NAME.to_owned();

        let mut data_trigger = DataTriggerProto::new();
        data_trigger.publisher_name = INVALID_PUBLISHER_NAME.to_owned();
        trigger.set_data_trigger(data_trigger);

        let mut config = MetricsConfigProto::new();
        config.triggers.push(trigger);
        config
    }

    fn create_config_with_invalid_conditional_trigger_parent_trigger() -> MetricsConfigProto {
        let mut trigger = TriggerProto::new();
        trigger.name = CONDITIONAL_TRIGGER_NAME.to_owned();

        let mut conditional_trigger = ConditionalTriggerProto::new();
        conditional_trigger.trigger_names.push(INVALID_TRIGGER_NAME.to_owned());
        trigger.set_conditional_trigger(conditional_trigger);

        let mut config = MetricsConfigProto::new();
        config.triggers.push(trigger);
        config
    }

    fn create_configs_with_duplicate_uuid() -> (MetricsConfigProto, MetricsConfigProto) {
        let mut config1 = MetricsConfigProto::new();
        config1.uuid = UUID.to_owned();
        let mut config2 = MetricsConfigProto::new();
        config2.uuid = config1.uuid.clone();
        (config1, config2)
    }

    fn create_valid_config_with_dependencies() -> MetricsConfigProto {
        let mut trigger1 = TriggerProto::new();
        trigger1.name = DATA_TRIGGER_NAME.to_owned();
        let mut trigger2 = TriggerProto::new();
        trigger2.name = CONDITIONAL_TRIGGER_NAME.to_owned();
        let mut publisher1 = PublisherProto::new();
        publisher1.name = SERVICE_PUBLISHER_NAME.to_owned();
        let mut publisher2 = PublisherProto::new();
        publisher2.name = AGGREGATION_PUBLISHER_NAME.to_owned();

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
        config.uuid = UUID.to_owned();
        config.publishers.push(publisher1);
        config.publishers.push(publisher2);
        config.triggers.push(trigger1);
        config.triggers.push(trigger2);
        config.expression_nodes.push(NodeProto::new());
        config
    }

    #[test]
    fn initialize_config_duplicate_publisher_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_publisher_name();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicatePublisherDefinition { publisher_name } if publisher_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_duplicate_trigger_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_trigger_name();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_duplicate_publisher_trigger_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_publisher_trigger_name();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name } if trigger_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_publisher_trigger_cycle_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_publisher_trigger_cycle();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::CycledPublishersTriggers),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_aggregation_publisher_invalid_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_aggregation_publisher_trigger();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name } if dependency_name == INVALID_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_data_trigger_invalid_publisher_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_data_trigger_publisher();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name } if dependency_name == INVALID_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_conditional_trigger_invalid_parent_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_conditional_trigger_parent_trigger();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name } if dependency_name == INVALID_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_duplicate_uuid_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let (config1, config2) = create_configs_with_duplicate_uuid();
        orchestrator.initialize_config(&config1).unwrap();

        let err = orchestrator.initialize_config(&config2).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid } if uuid == UUID),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 1);
    }

    #[test]
    fn initialize_config_invalid_expression_nodes_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.node_factory.fail_create_nodes();

        let mut config = MetricsConfigProto::new();
        config.expression_nodes.push(NodeProto::new());

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidExpressionNodes { .. }),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_publisher_factory_error_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator
            .publisher_factory
            .set_create_callback(|_| Some(Err(PublisherFactoryError::UnknownPublisherType)));
        let config = create_valid_config_with_publisher();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidPublisherDefinition { factory_error: PublisherFactoryError::UnknownPublisherType, publisher_name } if publisher_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_trigger_factory_error_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator
            .trigger_factory
            .set_create_callback(|_| Some(Err(TriggerFactoryError::UnknownTriggerType)));
        let config = create_valid_config_with_trigger();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidTriggerDefinition { factory_error: TriggerFactoryError::UnknownTriggerType, trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn initialize_config_with_dependencies_success() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let factory_calls_clone1 = factory_calls.clone();
        let factory_calls_clone2 = factory_calls.clone();
        orchestrator.publisher_factory.set_create_callback(move |publisher_proto| {
            factory_calls_clone1.lock().unwrap().push(publisher_proto.name.clone());
            None
        });
        orchestrator.trigger_factory.set_create_callback(move |trigger_proto| {
            factory_calls_clone2.lock().unwrap().push(trigger_proto.name.clone());
            None
        });
        let config = create_valid_config_with_dependencies();

        orchestrator.initialize_config(&config).unwrap();

        let expected_calls = vec![
            SERVICE_PUBLISHER_NAME,
            DATA_TRIGGER_NAME,
            CONDITIONAL_TRIGGER_NAME,
            AGGREGATION_PUBLISHER_NAME,
        ];
        assert_eq!(*factory_calls.lock().unwrap(), expected_calls);
        assert_eq!(*orchestrator.node_factory.num_nodes.lock().unwrap(), 1);
        let contexts = orchestrator.configs.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        let context = &contexts[&config.uuid];
        assert_eq!(context.publishers.len(), 2);
        assert_eq!(context.triggers.len(), 2);
    }

    #[test]
    fn finish_config_invalid_config_uuid_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let err = orchestrator.finish_config(UUID).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidConfigUuid { uuid } if uuid == UUID),
            "Unexpected error: {:?}",
            err
        )
    }

    #[test]
    fn finish_config_success() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut metrics_config = MetricsConfigProto::new();
        metrics_config.uuid = UUID.to_owned();

        orchestrator.initialize_config(&metrics_config).unwrap();

        assert!(orchestrator.finish_config(UUID).is_ok());
    }

    #[test]
    fn validate_config_duplicate_publisher_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_publisher_name();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicatePublisherDefinition { publisher_name } if publisher_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_duplicate_trigger_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_trigger_name();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_duplicate_publisher_trigger_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_publisher_trigger_name();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name } if trigger_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_publisher_trigger_cycle_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_publisher_trigger_cycle();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::CycledPublishersTriggers),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_aggregation_publisher_invalid_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_aggregation_publisher_trigger();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name } if dependency_name == INVALID_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_data_trigger_invalid_publisher_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_data_trigger_publisher();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name } if dependency_name == INVALID_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_conditional_trigger_invalid_parent_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_conditional_trigger_parent_trigger();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name } if dependency_name == INVALID_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_duplicate_uuid_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let (config1, config2) = create_configs_with_duplicate_uuid();
        orchestrator.initialize_config(&config1).unwrap();

        let err = orchestrator.validate_config(&config2).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid } if uuid == UUID),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 1);
    }

    #[test]
    fn validate_config_invalid_expression_nodes_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.node_factory.fail_create_nodes();

        let mut config = MetricsConfigProto::new();
        config.expression_nodes.push(NodeProto::new());

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidExpressionNodes { .. }),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_publisher_factory_error_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator
            .publisher_factory
            .set_validate_callback(|_| Some(Err(PublisherFactoryError::UnknownPublisherType)));
        let config = create_valid_config_with_publisher();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidPublisherDefinition { factory_error: PublisherFactoryError::UnknownPublisherType, publisher_name } if publisher_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_trigger_factory_error_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator
            .trigger_factory
            .set_validate_callback(|_| Some(Err(TriggerFactoryError::UnknownTriggerType)));
        let config = create_valid_config_with_trigger();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidTriggerDefinition { factory_error: TriggerFactoryError::UnknownTriggerType, trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_with_dependencies_success() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let factory_calls_clone1 = factory_calls.clone();
        let factory_calls_clone2 = factory_calls.clone();
        orchestrator.publisher_factory.set_validate_callback(move |publisher_proto| {
            factory_calls_clone1.lock().unwrap().push(publisher_proto.name.clone());
            None
        });
        orchestrator.trigger_factory.set_validate_callback(move |trigger_proto| {
            factory_calls_clone2.lock().unwrap().push(trigger_proto.name.clone());
            None
        });
        let config = create_valid_config_with_dependencies();

        orchestrator.validate_config(&config).unwrap();

        let expected_calls = vec![
            SERVICE_PUBLISHER_NAME,
            DATA_TRIGGER_NAME,
            CONDITIONAL_TRIGGER_NAME,
            AGGREGATION_PUBLISHER_NAME,
        ];
        assert_eq!(*factory_calls.lock().unwrap(), expected_calls);
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }
}
