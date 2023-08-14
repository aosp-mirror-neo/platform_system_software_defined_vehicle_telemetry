// Copyright 2023 Google LLC. All Rights Reserved.

mod publisher;
mod publisher_factory;
mod trigger;
mod trigger_factory;

use log::{error, warn};
use protobuf::reflect::FileDescriptor;
use sdv_ast::{Node, NodeFactory, NodeFactoryImpl};
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto, trigger::Trigger_type as TriggerTypeProto,
    MetricsConfig as MetricsConfigProto, Publisher as PublisherProto, Trigger as TriggerProto,
};
use std::{
    collections::HashMap,
    marker::Sync,
    sync::{Arc, Mutex},
};
use thiserror::Error;

use crate::collection_extension::HashMapExtension;
use publisher::Publisher;
use publisher_factory::{
    PublisherFactory, PublisherFactoryContext, PublisherFactoryError, PublisherFactoryImpl,
};
use trigger::{Trigger, TriggerImpl, TriggerSubscriber};
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
    #[error("Invalid definition for publisher {publisher_name}: {factory_error}")]
    InvalidPublisherDefinition { publisher_name: String, factory_error: PublisherFactoryError },
    #[error("Invalid definition for trigger {trigger_name}: {factory_error}")]
    InvalidTriggerDefinition { trigger_name: String, factory_error: TriggerFactoryError },
    #[error("Invalid descriptor protos: {error}")]
    InvalidDescriptorProtos {
        #[from]
        error: protobuf::Error,
    },
    #[error("Metric Config with UUID {uuid} doesn't exist")]
    InvalidConfigUuid { uuid: String },
    #[error("Invalid expression nodes. Reason: {error_detail}")]
    InvalidExpressionNodes { error_detail: String },
    #[error("Unable to activate start trigger: {error}")]
    UnableToActivateStartTrigger { error: String },
}

/// Struct for default implementation of MetricsConfigOrchestrator.
pub(super) struct MetricsConfigOrchestratorImpl<
    NdFactory: NodeFactory = NodeFactoryImpl,
    PubFactory: PublisherFactory<Trig> = PublisherFactoryImpl,
    Trig: Trigger = TriggerImpl,
    TrigFactory: TriggerFactory<Trig> = TriggerFactoryImpl,
> {
    configs: Mutex<HashMap<String, Arc<MetricsConfig<Trig>>>>,
    node_factory: NdFactory,
    publisher_factory: PubFactory,
    trigger_factory: TrigFactory,
}

#[allow(dead_code)]
pub(super) struct MetricsConfig<Trig: Trigger = TriggerImpl> {
    uuid: String,
    publishers: HashMap<String, Arc<Publisher>>,
    start_trigger: Option<Arc<Trig>>,
    end_trigger: Option<Arc<Trig>>,
    finish_trigger: Option<Arc<Trig>>,
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

impl<NdFactory, PubFactory, Trig, TrigFactory>
    MetricsConfigOrchestratorImpl<NdFactory, PubFactory, Trig, TrigFactory>
where
    NdFactory: NodeFactory,
    PubFactory: PublisherFactory<Trig>,
    Trig: Trigger + Send + Sync + 'static,
    TrigFactory: TriggerFactory<Trig>,
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
            &mut HashMap<String, Arc<Publisher>>,
            &HashMap<String, Arc<Trig>>,
        ) -> Result<(), MetricsConfigOrchestratorError>,
        trigger_func: impl Fn(
            &TriggerProto,
            &HashMap<String, Arc<Publisher>>,
            &mut HashMap<String, Arc<Trig>>,
        ) -> Result<(), MetricsConfigOrchestratorError>,
    ) -> Result<
        (HashMap<String, Arc<Publisher>>, HashMap<String, Arc<Trig>>),
        MetricsConfigOrchestratorError,
    > {
        enum DependencyContainer<'a> {
            Publisher { publisher_proto: &'a PublisherProto, dependencies: Vec<&'a String> },
            Trigger { trigger_proto: &'a TriggerProto, dependencies: Vec<&'a String> },
        }

        // Build dependency map for topological sorting.
        let dependency_map = config_proto.publishers.iter().try_fold(
            HashMap::new(),
            |mut acc, publisher_proto| match acc.try_insert_value(
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
            ) {
                Ok(_) => Ok(acc),
                Err(()) => Err(MetricsConfigOrchestratorError::DuplicatePublisherDefinition {
                    publisher_name: publisher_proto.name.clone(),
                }),
            },
        )?;

        let dependency_map =
            config_proto.triggers.iter().try_fold(dependency_map, |mut acc, trigger_proto| {
                match acc.try_insert_value(
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
                ) {
                    Ok(_) => Ok(acc),
                    Err(()) => Err(MetricsConfigOrchestratorError::DuplicateTriggerDefinition {
                        trigger_name: trigger_proto.name.clone(),
                    }),
                }
            })?;

        // Retrieve topologically sorted vector of keys.
        let sorted_vector = dependency_map
            .to_topological_sorted_vec(|dependency| match dependency {
                DependencyContainer::Trigger { dependencies, .. } => dependencies,
                DependencyContainer::Publisher { dependencies, .. } => dependencies,
            })
            .ok_or(MetricsConfigOrchestratorError::CycledPublishersTriggers)?;

        // Create publishers and triggers in the topologically sorted order to make sure
        // the dependencies are created before those that reference them.
        sorted_vector.iter().rev().try_fold(
            (HashMap::new(), HashMap::new()),
            |(mut publishers, mut triggers), container_name| {
                match dependency_map.get(*container_name) {
                    Some(DependencyContainer::Publisher { publisher_proto, .. }) => {
                        publisher_func(publisher_proto, &mut publishers, &triggers)
                    }
                    Some(DependencyContainer::Trigger { trigger_proto, .. }) => {
                        trigger_func(trigger_proto, &publishers, &mut triggers)
                    }
                    None => Err(MetricsConfigOrchestratorError::InvalidDependency {
                        dependency_name: container_name.to_string(),
                    }),
                }?;
                Ok((publishers, triggers))
            },
        )
    }

    fn maybe_get_trigger<'a>(
        triggers: &'a HashMap<String, Arc<Trig>>,
        trigger_name: &str,
    ) -> Result<Option<&'a Arc<Trig>>, MetricsConfigOrchestratorError> {
        if trigger_name.is_empty() {
            return Ok(None);
        }
        Ok(Some(triggers.get(trigger_name).ok_or(
            MetricsConfigOrchestratorError::InvalidDependency {
                dependency_name: trigger_name.to_owned(),
            },
        )?))
    }

    fn on_start_trigger_fired(config: &Arc<MetricsConfig<Trig>>) {
        if let Some(start_trigger) = &config.start_trigger {
            if let Err(err) = start_trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart) {
                error!(
                    "Error when trying to deactivate start trigger of Metrics Configuration {}: {}.\
                     Metrics Configuration data collection will not start.",
                    config.uuid, err
                );
                return;
            }
        }

        if let Some(end_trigger) = &config.end_trigger {
            let config_clone = config.clone();
            if let Err(err) = end_trigger
                .subscribe(&TriggerSubscriber::MetricsConfigEnd, move || {
                    Self::on_end_trigger_fired(&config_clone)
                })
            {
                error!(
                    "Error when trying to activate end trigger of Metrics Configuration {}: {}.\
                     Metrics Configuration data collection will never end.",
                    config.uuid, err
                );
            }
        }

        if let Some(finish_trigger) = &config.finish_trigger {
            let config_clone = config.clone();
            if let Err(err) = finish_trigger
                .subscribe(&TriggerSubscriber::MetricsConfigFinish, move || {
                    Self::on_finish_trigger_fired(&config_clone)
                })
            {
                error!(
                    "Error when trying to activate end trigger of Metrics Configuration {}: {}.\
                     Metrics Configuration data collection will never finish.",
                    config.uuid, err
                );
            }
        }
    }

    fn on_end_trigger_fired(_config: &Arc<MetricsConfig<Trig>>) {
        // TODO(b/289386420): implement end trigger handling logic.
        warn!("End trigger handling logic is not implemented. Data collection won't end.");
    }

    fn on_finish_trigger_fired(_config: &Arc<MetricsConfig<Trig>>) {
        // TODO(b/294616843): implement finish trigger handling logic.
        warn!("Finish trigger handling logic is not implemented. Data collection won't finish.");
    }
}

impl<
        NdFactory: NodeFactory + Sync,
        PubFactory: PublisherFactory<Trig> + Sync,
        Trig: Trigger + Sync + Send + 'static,
        TrigFactory: TriggerFactory<Trig> + Sync,
    > MetricsConfigOrchestrator
    for MetricsConfigOrchestratorImpl<NdFactory, PubFactory, Trig, TrigFactory>
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

        let (publishers, triggers) = Self::try_fold_topoligically_sorted_publishers_triggers(
            config_proto,
            // Create a rust Publisher object using already created Trigger objects to resolve dependencies.
            |publisher_proto, publishers, triggers| {
                let context = PublisherFactoryContext {
                    expression_nodes: &expression_nodes,
                    file_descriptors: &file_descriptors,
                    triggers,
                };
                self.publisher_factory.create(publisher_proto, &context).map_or_else(
                    |factory_error| {
                        Err(MetricsConfigOrchestratorError::InvalidPublisherDefinition {
                            publisher_name: publisher_proto.name.clone(),
                            factory_error,
                        })
                    },
                    |publisher| {
                        publishers.insert(publisher_proto.name.clone(), Arc::new(publisher));
                        Ok(())
                    },
                )
            },
            // Create a rust Trigger object using already created Publisher and Trigger objects to resolve dependencies.
            |trigger_proto, publishers, triggers| {
                let context = TriggerFactoryContext {
                    expression_nodes: &expression_nodes,
                    publishers,
                    triggers,
                };
                self.trigger_factory.create(trigger_proto, &context).map_or_else(
                    |factory_error| {
                        Err(MetricsConfigOrchestratorError::InvalidTriggerDefinition {
                            trigger_name: trigger_proto.name.clone(),
                            factory_error,
                        })
                    },
                    |trigger| {
                        triggers.insert(trigger_proto.name.clone(), Arc::new(trigger));
                        Ok(())
                    },
                )
            },
        )?;

        // Create MetricsConfig.
        let start_trigger =
            Self::maybe_get_trigger(&triggers, &config_proto.start_trigger_name)?.cloned();
        let end_trigger =
            Self::maybe_get_trigger(&triggers, &config_proto.end_trigger_name)?.cloned();
        let finish_trigger =
            Self::maybe_get_trigger(&triggers, &config_proto.finish_trigger_name)?.cloned();
        let config = Arc::new(MetricsConfig {
            uuid: config_proto.uuid.clone(),
            publishers,
            start_trigger,
            end_trigger,
            finish_trigger,
        });

        // Store MetricsConfig with encapsulated objects.
        let mut configs = self.configs.lock().unwrap();
        let config = match configs.try_insert_value(config_proto.uuid.clone(), config) {
            Ok(val) => val,
            Err(()) => {
                return Err(MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid {
                    uuid: config_proto.uuid.clone(),
                })
            }
        };

        // Activate start trigger, if set.
        if let Some(start_trigger) = &config.start_trigger {
            let config_clone = config.clone();
            start_trigger
                .subscribe(&TriggerSubscriber::MetricsConfigStart, move || {
                    Self::on_start_trigger_fired(&config_clone)
                })
                .map_err(|error| MetricsConfigOrchestratorError::UnableToActivateStartTrigger {
                    error,
                })?;
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
            |publisher_proto, _, triggers| {
                let context = PublisherFactoryContext {
                    expression_nodes: &expression_nodes,
                    file_descriptors: &file_descriptors,
                    triggers,
                };
                self.publisher_factory.validate(publisher_proto, &context).map_err(|e| {
                    MetricsConfigOrchestratorError::InvalidPublisherDefinition {
                        publisher_name: publisher_proto.name.clone(),
                        factory_error: e,
                    }
                })?;
                Ok(())
            },
            |trigger_proto, publishers, triggers| {
                let context = TriggerFactoryContext {
                    expression_nodes: &expression_nodes,
                    publishers,
                    triggers,
                };
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
    use crate::metrics_config_orchestrator::trigger_factory::TriggerFactory;
    use crate::MetricsConfigOrchestratorImpl;

    type MockedMetricsConfigOrchestratorImpl = MetricsConfigOrchestratorImpl<
        MockNodeFactory,
        MockPublisherFactory,
        MockTrigger,
        MockTriggerFactory,
    >;

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
        create_callback:
            Option<Box<dyn Fn(&PublisherProto) -> Option<PublisherFactoryError> + Sync>>,
        validate_callback:
            Option<Box<dyn Fn(&PublisherProto) -> Option<PublisherFactoryError> + Sync>>,
    }

    impl MockPublisherFactory {
        fn new() -> Self {
            MockPublisherFactory { create_callback: None, validate_callback: None }
        }

        fn set_create_callback(
            &mut self,
            callback: impl Fn(&PublisherProto) -> Option<PublisherFactoryError> + Sync + 'static,
        ) {
            self.create_callback = Some(Box::new(callback));
        }

        fn set_validate_callback(
            &mut self,
            callback: impl Fn(&PublisherProto) -> Option<PublisherFactoryError> + Sync + 'static,
        ) {
            self.validate_callback = Some(Box::new(callback));
        }
    }

    impl PublisherFactory<MockTrigger> for MockPublisherFactory {
        fn create(
            &self,
            publisher_proto: &PublisherProto,
            _: &PublisherFactoryContext<MockTrigger>,
        ) -> Result<Publisher, PublisherFactoryError> {
            if let Some(callback) = &self.create_callback {
                if let Some(res) = callback(publisher_proto) {
                    return Err(res);
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
            _: &PublisherFactoryContext<MockTrigger>,
        ) -> Result<(), PublisherFactoryError> {
            if let Some(callback) = &self.validate_callback {
                if let Some(res) = callback(publisher_proto) {
                    return Err(res);
                }
            }
            Ok(())
        }
    }

    struct MockTrigger {
        name: String,

        subscribe_callback:
            Option<Arc<Box<dyn Fn(&str, Box<dyn Fn()>) -> Option<String> + Send + Sync>>>,
        unsubscribe_callback: Option<Arc<Box<dyn Fn(&str) -> Option<String> + Send + Sync>>>,
    }

    impl MockTrigger {
        fn new(name: &str) -> Self {
            MockTrigger {
                name: name.to_owned(),
                subscribe_callback: None,
                unsubscribe_callback: None,
            }
        }
    }

    impl Trigger for MockTrigger {
        fn subscribe(
            &self,
            _: &TriggerSubscriber,
            on_fired_callback: impl Fn() + Send + Sync + 'static,
        ) -> Result<(), String> {
            if let Some(callback) = &self.subscribe_callback {
                if let Some(error) = callback(&self.name, Box::new(on_fired_callback)) {
                    return Err(error);
                }
            }

            Ok(())
        }

        fn unsubscribe(&self, _: &TriggerSubscriber) -> Result<(), String> {
            if let Some(callback) = &self.unsubscribe_callback {
                if let Some(error) = callback(&self.name) {
                    return Err(error);
                }
            }

            Ok(())
        }
    }

    struct MockTriggerFactory {
        create_callback: Option<Box<dyn Fn(&TriggerProto) -> Option<TriggerFactoryError> + Sync>>,
        validate_callback: Option<Box<dyn Fn(&TriggerProto) -> Option<TriggerFactoryError> + Sync>>,
        subscribe_callback:
            Option<Arc<Box<dyn Fn(&str, Box<dyn Fn()>) -> Option<String> + Send + Sync>>>,
        unsubscribe_callback: Option<Arc<Box<dyn Fn(&str) -> Option<String> + Send + Sync>>>,
    }

    impl MockTriggerFactory {
        fn new() -> Self {
            MockTriggerFactory {
                create_callback: None,
                validate_callback: None,
                subscribe_callback: None,
                unsubscribe_callback: None,
            }
        }

        fn set_create_callback(
            &mut self,
            callback: impl Fn(&TriggerProto) -> Option<TriggerFactoryError> + Sync + 'static,
        ) {
            self.create_callback = Some(Box::new(callback));
        }

        fn set_validate_callback(
            &mut self,
            callback: impl Fn(&TriggerProto) -> Option<TriggerFactoryError> + Sync + 'static,
        ) {
            self.validate_callback = Some(Box::new(callback));
        }

        fn set_subscribe_callback(
            &mut self,
            callback: impl Fn(&str, Box<dyn Fn()>) -> Option<String> + Send + Sync + 'static,
        ) {
            self.subscribe_callback = Some(Arc::new(Box::new(callback)));
        }

        fn set_unsubscribe_callback(
            &mut self,
            callback: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
        ) {
            self.unsubscribe_callback = Some(Arc::new(Box::new(callback)));
        }
    }

    impl TriggerFactory<MockTrigger> for MockTriggerFactory {
        fn create(
            &self,
            trigger_proto: &TriggerProto,
            _: &TriggerFactoryContext<MockTrigger>,
        ) -> Result<MockTrigger, TriggerFactoryError> {
            if let Some(callback) = &self.create_callback {
                if let Some(res) = callback(trigger_proto) {
                    return Err(res);
                }
            }

            let mut trigger = MockTrigger::new(&trigger_proto.name);
            trigger.subscribe_callback = self.subscribe_callback.clone();
            trigger.unsubscribe_callback = self.unsubscribe_callback.clone();
            Ok(trigger)
        }

        fn validate(
            &self,
            trigger_proto: &TriggerProto,
            _: &TriggerFactoryContext<MockTrigger>,
        ) -> Result<(), TriggerFactoryError> {
            if let Some(callback) = &self.validate_callback {
                if let Some(res) = callback(trigger_proto) {
                    return Err(res);
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
            .set_create_callback(|_| Some(PublisherFactoryError::UnknownPublisherType));
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
            .set_create_callback(|_| Some(TriggerFactoryError::UnknownTriggerType));
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
    }

    #[test]
    fn initialize_config_invalid_start_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = SOME_TRIGGER_NAME.to_owned();

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name } if dependency_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn initialize_config_start_trigger_doesnt_activate_fails() {
        const ERROR_MESSAGE: &str = "error_message";

        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.trigger_factory.set_subscribe_callback(|_, _| Some(ERROR_MESSAGE.to_owned()));

        let mut start_trigger = TriggerProto::new();
        start_trigger.name = SOME_TRIGGER_NAME.to_owned();
        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = start_trigger.name.clone();
        config.triggers.push(start_trigger);

        let err = orchestrator.initialize_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::UnableToActivateStartTrigger { error } if error == ERROR_MESSAGE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn initialize_config_with_lifetime_triggers_success() {
        const ACTIVATE: &str = "activate";
        const DEACTIVATE: &str = "deactivate";
        const START_TRIGGER_NAME: &str = "start_trigger";
        const END_TRIGGER_NAME: &str = "end_trigger";
        const FINISH_TRIGGER_NAME: &str = "finish_trigger";

        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let factory_calls_clone1 = factory_calls.clone();
        let factory_calls_clone2 = factory_calls.clone();
        orchestrator.trigger_factory.set_subscribe_callback(move |name, fire_trigger_func| {
            factory_calls_clone1.lock().unwrap().push((ACTIVATE.to_owned(), name.to_owned()));
            fire_trigger_func();
            None
        });
        orchestrator.trigger_factory.set_unsubscribe_callback(move |name| {
            factory_calls_clone2.lock().unwrap().push((DEACTIVATE.to_owned(), name.to_owned()));
            None
        });

        let mut start_trigger = TriggerProto::new();
        start_trigger.name = START_TRIGGER_NAME.to_owned();
        let mut end_trigger = TriggerProto::new();
        end_trigger.name = END_TRIGGER_NAME.to_owned();
        let mut finish_trigger = TriggerProto::new();
        finish_trigger.name = FINISH_TRIGGER_NAME.to_owned();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = start_trigger.name.clone();
        config.end_trigger_name = end_trigger.name.clone();
        config.finish_trigger_name = finish_trigger.name.clone();
        config.triggers.push(start_trigger);
        config.triggers.push(end_trigger);
        config.triggers.push(finish_trigger);

        orchestrator.initialize_config(&config).unwrap();

        let expected_calls = vec![
            (ACTIVATE.to_owned(), START_TRIGGER_NAME.to_owned()),
            (DEACTIVATE.to_owned(), START_TRIGGER_NAME.to_owned()),
            (ACTIVATE.to_owned(), END_TRIGGER_NAME.to_owned()),
            (ACTIVATE.to_owned(), FINISH_TRIGGER_NAME.to_owned()),
        ];
        assert_eq!(*factory_calls.lock().unwrap(), expected_calls);
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
            .set_validate_callback(|_| Some(PublisherFactoryError::UnknownPublisherType));
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
            .set_validate_callback(|_| Some(TriggerFactoryError::UnknownTriggerType));
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
