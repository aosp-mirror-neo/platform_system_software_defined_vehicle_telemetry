// Copyright 2023 Google LLC. All Rights Reserved.

mod descriptor_provider;
mod message_builder;
mod message_builder_factory;
mod metrics_report_generator;
mod metrics_report_generator_factory;
mod publisher;
mod publisher_factory;
mod service_adapter;
mod subscription_manager;
mod trigger;
mod trigger_factory;

// External dependencies.
use async_trait::async_trait;
use log::error;
use protobuf::{reflect::FileDescriptor, MessageDyn};
use sdv_telemetry_utils::{
    collection_extension::HashMapExtension, descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS,
};
use sdvtelemetry_proto_rust::metrics_configuration::{
    publisher::Publisher_type as PublisherTypeProto, trigger::Trigger_type as TriggerTypeProto,
    MetricsConfig as MetricsConfigProto, MetricsReport, Publisher as PublisherProto,
    Trigger as TriggerProto,
};
use std::collections::{HashMap, HashSet};
use std::marker::{PhantomData, Sync};
use std::sync::{Arc, Mutex, OnceLock};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

// Crate dependencies.
use crate::expr::ast::{MessageProvider, Node, NodeFactory, NodeFactoryContext, NodeFactoryImpl};
use crate::expr::node_errors::{MessageProviderError, NodeError};
use crate::expr::node_value_type::NodeValueType;
use crate::service_publisher_manager::ServicePublisherManager;

// Child module dependencies.
use descriptor_provider::{DescriptorProviderConstructorError, DescriptorProviderImpl};
use message_builder_factory::MessageBuilderFactoryImpl;
use metrics_report_generator::{MetricsReportGenerator, MetricsReportGeneratorImpl};
use metrics_report_generator_factory::{
    MetricsReportGeneratorFactory, MetricsReportGeneratorFactoryCreateContext,
    MetricsReportGeneratorFactoryError, MetricsReportGeneratorFactoryImpl,
    MetricsReportGeneratorFactoryValidateContext,
};
use publisher::{Publisher, PublisherActivateError, PublisherImpl};
use publisher_factory::{
    PublisherFactory, PublisherFactoryCreateContext, PublisherFactoryError, PublisherFactoryImpl,
    PublisherFactoryValidateContext,
};
use service_adapter::ServiceAdapterImpl;
use trigger::{Trigger, TriggerImpl, TriggerSubscriber};
use trigger_factory::{
    TriggerFactory, TriggerFactoryCreateContext, TriggerFactoryError, TriggerFactoryImpl,
    TriggerFactoryValidateContext,
};

const PUBLISHER: &str = "publisher";
const TRIGGER: &str = "trigger";

/// Interface of a service that orchestrates lifecycle of Metrics Configurations.
#[async_trait]
pub trait MetricsConfigOrchestrator {
    /// Initialize lifecycle of the specified Metrics Configuration.
    async fn initialize_config(
        &self,
        config: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError>;

    /// Finish lifecycle of Metrics Configuration with the specified uuid.
    async fn finish_config(&self, config_uuid: &str) -> Result<(), MetricsConfigOrchestratorError>;

    /// Verify that Metrics Configuration is valid.
    fn validate_config(
        &self,
        config: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsConfigOrchestratorError {
    #[error("Cycle detected in the publisher and trigger dependency graph")]
    CycledPublishersTriggers,
    #[error("Duplicate definition of publisher {publisher_name}")]
    DuplicatePublisherDefinition { publisher_name: String },
    #[error("Duplicate definition of trigger {trigger_name}")]
    DuplicateTriggerDefinition { trigger_name: String },
    #[error("Duplicate metrics config identifier: {uuid}")]
    DuplicateMetricsConfigUuid { uuid: String },
    #[error("Duplicate definition of metrics report config {name}")]
    DuplicateMetricsReportConfigName { name: String },
    #[error("Metric Config with UUID {uuid} doesn't exist")]
    InvalidConfigUuid { uuid: String },
    #[error("Invalid expression nodes. Reason: {e}")]
    InvalidExpressionNodes { e: NodeError },
    #[error("Invalid dependency: {dependency_name} doesn't exist")]
    InvalidDependency { dependency_type: String, dependency_name: String },
    #[error("Invalid descriptor protos: {error}")]
    InvalidDescriptorProtos {
        #[from]
        error: protobuf::Error,
    },
    #[error("Invalid definition for metrics report config {report_config_name}: {factory_error}")]
    InvalidMetricsReportConfig {
        report_config_name: String,
        factory_error: MetricsReportGeneratorFactoryError,
    },
    #[error("Unable to create DescriptorProvider because of an invalid publisher: {error}")]
    InvalidPublisherAndDescriptorCombination {
        #[from]
        error: DescriptorProviderConstructorError,
    },
    #[error("Invalid definition for publisher {publisher_name}: {factory_error}")]
    InvalidPublisherDefinition { publisher_name: String, factory_error: PublisherFactoryError },
    #[error("Invalid definition for trigger {trigger_name}: {factory_error}")]
    InvalidTriggerDefinition { trigger_name: String, factory_error: TriggerFactoryError },
    #[error("Unable to activate publisher {publisher_name}: {error}")]
    UnableToActivatePublisher { publisher_name: String, error: PublisherActivateError },
}

/// Struct for default implementation of MetricsConfigOrchestrator.
// TODO(b/304793694): refactor tests and remove generic parameters.
pub(super) struct MetricsConfigOrchestratorImpl<
    TMetricsReportGenerator = MetricsReportGeneratorImpl,
    TMetricsReportGeneratorFactory = MetricsReportGeneratorFactoryImpl,
    TNodeFactory = NodeFactoryImpl,
    TPublisher = PublisherImpl,
    TPublisherFactory = PublisherFactoryImpl,
    TTrigger = TriggerImpl,
    TTriggerFactory = TriggerFactoryImpl,
> where
    TMetricsReportGenerator: MetricsReportGenerator,
    TMetricsReportGeneratorFactory:
        MetricsReportGeneratorFactory<TMetricsReportGenerator, TTrigger> + Send + Sync,
    TNodeFactory: NodeFactory + Send + Sync,
    TPublisher: Publisher,
    TPublisherFactory: PublisherFactory<TPublisher, TTrigger> + Send + Sync,
    TTrigger: Trigger,
    TTriggerFactory: TriggerFactory<TPublisher, TTrigger> + Send + Sync,
{
    configs:
        Mutex<HashMap<String, Arc<MetricsConfig<TMetricsReportGenerator, TPublisher, TTrigger>>>>,
    node_factory: TNodeFactory,
    metrics_report_generator_factory: TMetricsReportGeneratorFactory,
    publisher_factory: TPublisherFactory,
    trigger_factory: TTriggerFactory,

    /// Used to fetch data from service publishers
    service_publisher_manager: Arc<dyn ServicePublisherManager>,

    /// The function that is called when finish trigger of a Metrics Config is fired.
    finish_trigger_fired_callback: Arc<dyn Fn(String) + Send + Sync>,

    /// The function that is called when a new Metrics Report is ready.
    report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,

    /// Phantom field to be able to define bounds for TMetricsReportGenerator.
    phantom_generator: PhantomData<TMetricsReportGenerator>,
}

// TODO(b/304793694): refactor tests and remove generic parameters.
pub(super) struct MetricsConfig<
    TMetricsReportGenerator: MetricsReportGenerator = MetricsReportGeneratorImpl,
    TPublisher: Publisher = PublisherImpl,
    TTrigger: Trigger = TriggerImpl,
> {
    state: AsyncMutex<MetricsConfigState>,
    metrics_report_generators: Vec<Arc<TMetricsReportGenerator>>,
    ordered_publisher_names: Vec<String>,
    publishers: HashMap<String, Arc<TPublisher>>,
    start_trigger: Option<Arc<TTrigger>>,
    end_trigger: Option<Arc<TTrigger>>,
    finish_trigger: Option<Arc<TTrigger>>,
}

/// Lifecycle state of the metrics config.
#[derive(Debug)]
pub(super) enum MetricsConfigState {
    /// Config is active and waiting for start trigger to fire.
    /// This is the initial state.
    NotStarted,
    /// Performing data collection and listening to end trigger.
    Running,
    /// Data collection finished and won't restart.
    /// This is the terminal state.
    Finished,
}

// TODO(b/304793694): refactor tests and remove generic parameters.
struct MessageProviderImpl<TMetricsReportGenerator, TPublisher, TTrigger>
where
    TMetricsReportGenerator: MetricsReportGenerator,
    TPublisher: Publisher,
    TTrigger: Trigger,
{
    metrics_config: OnceLock<Arc<MetricsConfig<TMetricsReportGenerator, TPublisher, TTrigger>>>,
}

// TODO(b/304793694): refactor tests and remove generic parameters.
impl<TMetricsReportGenerator, TPublisher, TTrigger> MessageProvider
    for MessageProviderImpl<TMetricsReportGenerator, TPublisher, TTrigger>
where
    TMetricsReportGenerator: MetricsReportGenerator,
    TPublisher: Publisher,
    TTrigger: Trigger,
{
    fn get_message(
        &self,
        publisher_name: &str,
    ) -> Result<Option<Box<dyn MessageDyn>>, MessageProviderError> {
        self.metrics_config.get().unwrap().publishers.get(publisher_name).map_or_else(
            || Err(MessageProviderError::InvalidPublisherName),
            |publisher| Ok(publisher.get_latest_message()),
        )
    }
}

// TODO(b/304793694): refactor tests and remove generic parameters.
#[async_trait]
impl<
        TMetricsReportGenerator,
        TMetricsReportGeneratorFactory,
        TNodeFactory,
        TPublisher,
        TPublisherFactory,
        TTrigger,
        TTriggerFactory,
    > MetricsConfigOrchestrator
    for MetricsConfigOrchestratorImpl<
        TMetricsReportGenerator,
        TMetricsReportGeneratorFactory,
        TNodeFactory,
        TPublisher,
        TPublisherFactory,
        TTrigger,
        TTriggerFactory,
    >
where
    TMetricsReportGenerator: MetricsReportGenerator + 'static,
    TMetricsReportGeneratorFactory:
        MetricsReportGeneratorFactory<TMetricsReportGenerator, TTrigger> + Send + Sync + 'static,
    TNodeFactory: NodeFactory + Send + Sync + 'static,
    TPublisher: Publisher + 'static,
    TPublisherFactory: PublisherFactory<TPublisher, TTrigger> + Send + Sync + 'static,
    TTrigger: Trigger + 'static,
    TTriggerFactory: TriggerFactory<TPublisher, TTrigger> + Send + Sync + 'static,
{
    async fn initialize_config(
        &self,
        config_proto: &MetricsConfigProto,
    ) -> Result<(), MetricsConfigOrchestratorError> {
        // Build the provided proto file descriptors
        let cloned_descriptor_protos = config_proto.descriptor_protos.to_vec();
        let file_descriptors = FileDescriptor::new_dynamic_fds(
            cloned_descriptor_protos,
            &WELL_KNOWN_FILE_DESCRIPTORS,
        )?;

        // Create expression nodes.
        let message_provider = Arc::new(MessageProviderImpl { metrics_config: OnceLock::new() });
        let node_factory_context = NodeFactoryContext {
            message_provider: Some(message_provider.clone()),
            descriptor_provider: Box::new(DescriptorProviderImpl::new(
                &file_descriptors,
                &config_proto.publishers,
                &self.service_publisher_manager,
            )?),
        };
        let (expression_nodes, node_types) =
            self.create_expression_nodes(config_proto, &node_factory_context)?;

        // Iterate over topologically sorted vector of publishers and triggers and create encapsulated objects.
        // Sorted order ensures that all dependencies of the entity are met by the time iterator reaches it.
        // Store topologically sorted list of publisher names in order to activate them in the topological order.
        let mut ordered_publisher_names = vec![];
        let (publishers, triggers) = Self::try_fold_topoligically_sorted_publishers_triggers(
            config_proto,
            // Create a rust Publisher object using already created Trigger objects to resolve dependencies.
            |publisher_proto, triggers| {
                let context = PublisherFactoryCreateContext {
                    file_descriptors: &file_descriptors,
                    nodes: &expression_nodes,
                    node_types: &node_types,
                    triggers,
                };
                ordered_publisher_names.push(publisher_proto.name.clone());
                self.publisher_factory.create(publisher_proto, &context).map_err(|factory_error| {
                    MetricsConfigOrchestratorError::InvalidPublisherDefinition {
                        publisher_name: publisher_proto.name.clone(),
                        factory_error,
                    }
                })
            },
            // Create a rust Trigger object using already created Publisher and Trigger objects to resolve dependencies.
            |trigger_proto, publishers, triggers| {
                let context = TriggerFactoryCreateContext {
                    expression_nodes: &expression_nodes,
                    node_types: &node_types,
                    publishers,
                    triggers,
                };
                self.trigger_factory.create(trigger_proto, &context).map_err(|factory_error| {
                    MetricsConfigOrchestratorError::InvalidTriggerDefinition {
                        trigger_name: trigger_proto.name.clone(),
                        factory_error,
                    }
                })
            },
        )?;

        // Create metrics report generators.
        let context = MetricsReportGeneratorFactoryCreateContext {
            config_proto,
            triggers: &triggers,
            report_ready_callback: &self.report_ready_callback,
            file_descriptors: &file_descriptors,
            nodes: &expression_nodes,
            node_types: &node_types,
        };
        let metrics_report_configs = config_proto.metrics_report_configs.iter().try_fold(
            HashMap::new(),
            |mut acc, proto| {
                let report_generator =
                    self.metrics_report_generator_factory.create(proto, &context).map_or_else(
                        |err| {
                            Err(MetricsConfigOrchestratorError::InvalidMetricsReportConfig {
                                report_config_name: proto.name.clone(),
                                factory_error: err,
                            })
                        },
                        Ok,
                    )?;
                match acc.try_insert_value(&proto.name, report_generator) {
                    Ok(_) => Ok(acc),
                    Err(_) => {
                        Err(MetricsConfigOrchestratorError::DuplicateMetricsReportConfigName {
                            name: proto.name.clone(),
                        })
                    }
                }
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
            state: MetricsConfigState::NotStarted.into(),
            metrics_report_generators: metrics_report_configs.into_values().collect(),
            ordered_publisher_names,
            publishers,
            start_trigger,
            end_trigger,
            finish_trigger,
        });

        // Store MetricsConfig with encapsulated objects.
        {
            let mut configs = self.configs.lock().unwrap();
            match configs.try_insert_value(config_proto.uuid.clone(), config.clone()) {
                Ok(val) => val,
                Err(_) => {
                    return Err(MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid {
                        uuid: config_proto.uuid.clone(),
                    })
                }
            };
        };

        // Set real MetricsConfig in MessageProvider so that Publishers are discoverable by Nodes.
        message_provider
            .metrics_config
            .set(config.clone())
            .unwrap_or_else(|_| panic!("Provider shouldn't have metrics config set"));

        // Activate publishers one by one in topologically sorted order.
        for publisher_name in &config.ordered_publisher_names {
            config.publishers[publisher_name].clone().activate().await.map_err(|error| {
                MetricsConfigOrchestratorError::UnableToActivatePublisher {
                    publisher_name: publisher_name.to_owned(),
                    error,
                }
            })?;
        }

        // Activate start trigger, if set.
        Self::maybe_subscribe_to_start_trigger(&config).await;

        // Subscribe to finish trigger if it's set.
        if let Some(finish_trigger) = &config.finish_trigger {
            let mut maybe_parameters =
                Some((self.finish_trigger_fired_callback.clone(), config_proto.uuid.clone()));
            finish_trigger
                .clone()
                .subscribe(
                    &TriggerSubscriber::MetricsConfigFinish,
                    Box::new(move || {
                        if let Some((callback, uuid)) = maybe_parameters.take() {
                            callback(uuid);
                        }
                    }),
                )
                .await;
        }

        Ok(())
    }

    async fn finish_config(&self, config_uuid: &str) -> Result<(), MetricsConfigOrchestratorError> {
        let config = self.configs.lock().unwrap().remove(config_uuid).ok_or_else(|| {
            MetricsConfigOrchestratorError::InvalidConfigUuid { uuid: config_uuid.to_owned() }
        })?;

        // Lock state mutex and hold it mutex until transition is complete.
        let mut config_state = config.state.lock().await;

        // Unsubscribe from finish trigger if it's set.
        if let Some(finish_trigger) = &config.finish_trigger {
            finish_trigger.unsubscribe(&TriggerSubscriber::MetricsConfigFinish).await;
        }

        // Perform finishing logic based on the current state.
        match &*config_state {
            MetricsConfigState::NotStarted => {
                // Unsubscribe from start trigger if it's set.
                if let Some(start_trigger) = &config.start_trigger {
                    start_trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await;
                }
            }
            MetricsConfigState::Running => {
                // Unsubscribe from end trigger if it's set.
                if let Some(end_trigger) = &config.end_trigger {
                    end_trigger.unsubscribe(&TriggerSubscriber::MetricsConfigEnd).await;
                }

                // Deactivate metrics report generators.
                for generator in &config.metrics_report_generators {
                    generator.deactivate().await;
                }
            }
            state => {
                panic!(
                    "Attempted to finish Metrics Configuration with unexpected state: {:?}",
                    state
                );
            }
        };

        // Deactivate publishers in reverse topologically sorted order.
        for publisher_name in config.ordered_publisher_names.iter().rev() {
            if let Err(err) = config.publishers[publisher_name].deactivate().await {
                error!("Error when trying to deactivate publisher {}: {}.", publisher_name, err);
            }
        }

        // Change state to Finished and release the mutex.
        *config_state = MetricsConfigState::Finished;

        Ok(())
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

        // Build the provided proto file descriptors
        let cloned_descriptor_protos = config_proto.descriptor_protos.to_vec();
        let file_descriptors = FileDescriptor::new_dynamic_fds(
            cloned_descriptor_protos,
            &WELL_KNOWN_FILE_DESCRIPTORS,
        )?;

        // Validate expression nodes to ensure they are set up correctly.
        let node_factory_context = NodeFactoryContext {
            message_provider: None,
            descriptor_provider: Box::new(DescriptorProviderImpl::new(
                &file_descriptors,
                &config_proto.publishers,
                &self.service_publisher_manager,
            )?),
        };
        let node_types = self
            .node_factory
            .validate(&config_proto.expression_nodes, &node_factory_context)
            .map_err(|e| MetricsConfigOrchestratorError::InvalidExpressionNodes { e })?;

        // Topologically sort publishers and triggers to ensure dependencies exist and there are no cycles.
        let (_publishers, triggers) = Self::try_fold_topoligically_sorted_publishers_triggers(
            config_proto,
            |publisher_proto, _triggers| {
                let context = PublisherFactoryValidateContext {
                    node_types: &node_types,
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
            |trigger_proto, _publishers, _triggers| {
                let context = TriggerFactoryValidateContext { node_types: &node_types };
                self.trigger_factory.validate(trigger_proto, &context).map_err(|e| {
                    MetricsConfigOrchestratorError::InvalidTriggerDefinition {
                        trigger_name: trigger_proto.name.clone(),
                        factory_error: e,
                    }
                })?;
                Ok(())
            },
        )?;

        // Validate metrics report configs.
        let context = MetricsReportGeneratorFactoryValidateContext {
            trigger_protos: &config_proto.triggers,
            file_descriptors: &file_descriptors,
            node_types: &node_types,
        };
        config_proto.metrics_report_configs.iter().try_fold(HashSet::new(), |mut acc, proto| {
            if !acc.insert(&proto.name) {
                return Err(MetricsConfigOrchestratorError::DuplicateMetricsReportConfigName {
                    name: proto.name.clone(),
                });
            }
            self.metrics_report_generator_factory.validate(proto, &context).map_err(|err| {
                MetricsConfigOrchestratorError::InvalidMetricsReportConfig {
                    report_config_name: proto.name.clone(),
                    factory_error: err,
                }
            })?;
            Ok(acc)
        })?;

        // Validate lifecycle triggers.
        Self::maybe_get_trigger(&triggers, &config_proto.start_trigger_name)?;
        Self::maybe_get_trigger(&triggers, &config_proto.end_trigger_name)?;
        Self::maybe_get_trigger(&triggers, &config_proto.finish_trigger_name)?;

        Ok(())
    }
}

impl MetricsConfigOrchestratorImpl {
    pub(super) fn new(
        service_publisher_manager: Arc<dyn ServicePublisherManager>,
        finish_trigger_fired_callback: impl Fn(String) + Send + Sync + 'static,
        report_ready_callback: impl Fn(MetricsReport) + Send + Sync + 'static,
    ) -> Self {
        let message_builder_factory = Arc::new(MessageBuilderFactoryImpl::new());
        let service_adapter = Arc::new(ServiceAdapterImpl::new(service_publisher_manager.clone()));
        MetricsConfigOrchestratorImpl {
            configs: Mutex::new(HashMap::new()),
            service_publisher_manager,
            metrics_report_generator_factory: MetricsReportGeneratorFactoryImpl::new(
                message_builder_factory.clone(),
            ),
            node_factory: NodeFactoryImpl::new(),
            publisher_factory: PublisherFactoryImpl::new(message_builder_factory, service_adapter),
            trigger_factory: TriggerFactoryImpl::new(),
            finish_trigger_fired_callback: Arc::new(finish_trigger_fired_callback),
            report_ready_callback: Arc::new(report_ready_callback),
            phantom_generator: PhantomData,
        }
    }
}

// TODO(b/304793694): refactor tests and remove generic parameters.
impl<
        TMetricsReportGenerator,
        TMetricsReportGeneratorFactory,
        TNodeFactory,
        TPublisher,
        TPublisherFactory,
        TTrigger,
        TTriggerFactory,
    >
    MetricsConfigOrchestratorImpl<
        TMetricsReportGenerator,
        TMetricsReportGeneratorFactory,
        TNodeFactory,
        TPublisher,
        TPublisherFactory,
        TTrigger,
        TTriggerFactory,
    >
where
    TMetricsReportGenerator: MetricsReportGenerator + 'static,
    TMetricsReportGeneratorFactory:
        MetricsReportGeneratorFactory<TMetricsReportGenerator, TTrigger> + Send + Sync + 'static,
    TNodeFactory: NodeFactory + Send + Sync + 'static,
    TPublisher: Publisher + 'static,
    TPublisherFactory: PublisherFactory<TPublisher, TTrigger> + Send + Sync + 'static,
    TTrigger: Trigger + 'static,
    TTriggerFactory: TriggerFactory<TPublisher, TTrigger> + Send + Sync + 'static,
{
    // Private functions.

    fn create_expression_nodes(
        &self,
        config_proto: &MetricsConfigProto,
        context: &NodeFactoryContext,
    ) -> Result<(Vec<Arc<Node>>, Vec<NodeValueType>), MetricsConfigOrchestratorError> {
        self.node_factory
            .create_nodes(&config_proto.expression_nodes, context)
            .map_err(|e| MetricsConfigOrchestratorError::InvalidExpressionNodes { e })
    }

    fn maybe_get_trigger<'a, TTrig>(
        triggers: &'a HashMap<String, TTrig>,
        trigger_name: &str,
    ) -> Result<Option<&'a TTrig>, MetricsConfigOrchestratorError> {
        if trigger_name.is_empty() {
            return Ok(None);
        }
        Ok(Some(triggers.get(trigger_name).ok_or(
            MetricsConfigOrchestratorError::InvalidDependency {
                dependency_type: TRIGGER.to_owned(),
                dependency_name: trigger_name.to_owned(),
            },
        )?))
    }

    async fn maybe_subscribe_to_start_trigger(
        config: &Arc<MetricsConfig<TMetricsReportGenerator, TPublisher, TTrigger>>,
    ) {
        if let Some(start_trigger) = &config.start_trigger {
            let mut config_clone = Some(config.clone());
            start_trigger
                .clone()
                .subscribe(
                    &TriggerSubscriber::MetricsConfigStart,
                    Box::new(move || {
                        if let Some(config) = config_clone.take() {
                            Self::on_start_trigger_fired(config);
                        }
                    }),
                )
                .await;
        }
    }

    /// Topologically sort publishers and triggers declared in the protobuf message
    /// and try to fold the sorted list using provided functions.
    fn try_fold_topoligically_sorted_publishers_triggers<TTrig, TPub>(
        config_proto: &MetricsConfigProto,
        mut publisher_func: impl FnMut(
            &PublisherProto,
            &HashMap<String, TTrig>,
        ) -> Result<TPub, MetricsConfigOrchestratorError>,
        mut trigger_func: impl FnMut(
            &TriggerProto,
            &HashMap<String, TPub>,
            &HashMap<String, TTrig>,
        ) -> Result<TTrig, MetricsConfigOrchestratorError>,
    ) -> Result<(HashMap<String, TPub>, HashMap<String, TTrig>), MetricsConfigOrchestratorError>
    {
        enum DependencyContainer<'a> {
            Publisher { publisher_proto: &'a PublisherProto },
            Trigger { trigger_proto: &'a TriggerProto },
        }

        // Build dependency map for topological sorting.
        let dependency_map = config_proto.publishers.iter().try_fold(
            HashMap::new(),
            |mut acc, publisher_proto| match acc.try_insert_value(
                (PUBLISHER, publisher_proto.name.as_str()),
                (
                    DependencyContainer::Publisher { publisher_proto },
                    if let Some(PublisherTypeProto::AggregationPublisher(aggregation_publisher)) =
                        &publisher_proto.publisher_type
                    {
                        aggregation_publisher
                            .trigger_names
                            .iter()
                            .map(|name| (TRIGGER, name.as_str()))
                            .collect()
                    } else {
                        vec![]
                    },
                ),
            ) {
                Ok(_) => Ok(acc),
                Err(_) => Err(MetricsConfigOrchestratorError::DuplicatePublisherDefinition {
                    publisher_name: publisher_proto.name.clone(),
                }),
            },
        )?;

        let dependency_map =
            config_proto.triggers.iter().try_fold(dependency_map, |mut acc, trigger_proto| {
                let publisher_key = (PUBLISHER, trigger_proto.name.as_str());
                if acc.contains_key(&publisher_key) {
                    return Err(MetricsConfigOrchestratorError::DuplicateTriggerDefinition {
                        trigger_name: trigger_proto.name.clone(),
                    });
                }
                match acc.try_insert_value(
                    (TRIGGER, trigger_proto.name.as_str()),
                    (
                        DependencyContainer::Trigger { trigger_proto },
                        match &trigger_proto.trigger_type {
                            Some(TriggerTypeProto::DataTrigger(data_trigger)) => {
                                vec![(PUBLISHER, data_trigger.publisher_name.as_str())]
                            }
                            Some(TriggerTypeProto::ConditionalTrigger(conditional_trigger)) => {
                                conditional_trigger
                                    .trigger_names
                                    .iter()
                                    .map(|name| (TRIGGER, name.as_str()))
                                    .collect()
                            }
                            _ => vec![],
                        },
                    ),
                ) {
                    Ok(_) => Ok(acc),
                    Err(_) => Err(MetricsConfigOrchestratorError::DuplicateTriggerDefinition {
                        trigger_name: trigger_proto.name.clone(),
                    }),
                }
            })?;

        // Retrieve topologically sorted vector of keys.
        let sorted_vector = dependency_map
            .to_topological_sorted_vec(|(_, dependencies)| dependencies)
            .ok_or(MetricsConfigOrchestratorError::CycledPublishersTriggers)?;

        // Create publishers and triggers in the topologically sorted order to make sure
        // the dependencies are created before those that reference them.
        sorted_vector.iter().rev().try_fold(
            (HashMap::new(), HashMap::new()),
            |(mut publishers, mut triggers), container_key| {
                match dependency_map.get(container_key) {
                    Some((DependencyContainer::Publisher { publisher_proto, .. }, _)) => {
                        let publisher = publisher_func(publisher_proto, &triggers)?;
                        publishers.insert(publisher_proto.name.clone(), publisher);
                    }
                    Some((DependencyContainer::Trigger { trigger_proto, .. }, _)) => {
                        let trigger = trigger_func(trigger_proto, &publishers, &mut triggers)?;
                        triggers.insert(trigger_proto.name.clone(), trigger);
                    }
                    None => {
                        let (container_type, name) = container_key;
                        return Err(MetricsConfigOrchestratorError::InvalidDependency {
                            dependency_type: container_type.to_string(),
                            dependency_name: name.to_string(),
                        });
                    }
                };
                Ok((publishers, triggers))
            },
        )
    }

    // Lifecycle event handlers.

    fn on_start_trigger_fired(
        config: Arc<MetricsConfig<TMetricsReportGenerator, TPublisher, TTrigger>>,
    ) {
        tokio::spawn(async move {
            // Lock state mutex, verify state and hold the mutex until transition is complete.
            let mut config_state = config.state.lock().await;
            match &*config_state {
                MetricsConfigState::NotStarted => {} // Proceed with transition
                MetricsConfigState::Finished => return, // No need to handle trigger fire when finished.
                state => panic!(
                    "Start trigger fired handler called for config in unexpected state: {:?}",
                    state
                ),
            }

            // Unsubscribe from start trigger.
            config
                .start_trigger
                .as_ref()
                .expect("Start trigger is expected to exist in the start trigger fired handler")
                .unsubscribe(&TriggerSubscriber::MetricsConfigStart)
                .await;

            // Activate metrics report generators.
            for generator in &config.metrics_report_generators {
                generator.activate().await;
            }

            // Subscribe to end trigger if it's set.
            if let Some(end_trigger) = &config.end_trigger {
                let mut config_clone = Some(config.clone());
                end_trigger
                    .clone()
                    .subscribe(
                        &TriggerSubscriber::MetricsConfigEnd,
                        Box::new(move || {
                            if let Some(config) = config_clone.take() {
                                Self::on_end_trigger_fired(config);
                            }
                        }),
                    )
                    .await;
            }

            // Change state to Running and release the mutex.
            *config_state = MetricsConfigState::Running;
        });
    }

    fn on_end_trigger_fired(
        config: Arc<MetricsConfig<TMetricsReportGenerator, TPublisher, TTrigger>>,
    ) {
        tokio::spawn(async move {
            // Lock state mutex, verify state and hold the mutex until transition is complete.
            let mut config_state = config.state.lock().await;
            match &*config_state {
                MetricsConfigState::Running => {}       // Proceed with transition
                MetricsConfigState::Finished => return, // No need to handle trigger fire when finished.
                state => panic!(
                    "End trigger fired handler called for config in unexpected state: {:?}",
                    state
                ),
            }

            // Unsubscribe from end trigger.
            config
                .end_trigger
                .as_ref()
                .expect("End trigger is expected to exist in the end trigger fired handler")
                .unsubscribe(&TriggerSubscriber::MetricsConfigEnd)
                .await;

            // Deactivate metrics report generators.
            for generator in &config.metrics_report_generators {
                generator.deactivate().await;
            }

            // Subscribe to start trigger if it's set.
            Self::maybe_subscribe_to_start_trigger(&config).await;

            // Change state to NotStarted and release the mutex.
            *config_state = MetricsConfigState::NotStarted;
        });
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use protobuf::descriptor::{
        field_descriptor_proto, DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };
    use protobuf::well_known_types::{empty::Empty, wrappers::Int32Value};
    use protobuf::{well_known_types, MessageDyn, MessageFull};
    use sdvtelemetry_proto_rust::expression::Node as NodeProto;
    use sdvtelemetry_proto_rust::metrics_configuration::{
        AggregationPublisher as AggregationPublisherProto,
        ConditionalTrigger as ConditionalTriggerProto, DataTrigger as DataTriggerProto,
        MetricsConfig as MetricsConfigProto, MetricsReportConfig,
        PeriodicTrigger as PeriodicTriggerProto, ProtoMessageBuilder, Publisher as PublisherProto,
        ServicePublisher as ServicePublisherProto, SignalPublisher as SignalPublisherProto,
        Trigger as TriggerProto,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    use super::*;
    use crate::expr::node_value::NodeValue;
    use crate::expr::node_value_type::NodeValueType;
    use crate::metrics_config_orchestrator::message_builder_factory::mock::get_test_file_descriptors;
    use crate::metrics_config_orchestrator::publisher::mock::MockPublisher;
    use crate::metrics_config_orchestrator::publisher_factory::{
        PublisherFactory, PublisherFactoryError,
    };
    use crate::metrics_config_orchestrator::service_adapter::ServiceAdapterSubscribeError;
    use crate::metrics_config_orchestrator::trigger::mock::MockTrigger;
    use crate::metrics_config_orchestrator::trigger_factory::TriggerFactory;
    use crate::service_publisher_manager::mock::MockServicePublisherManager;
    use crate::{MetricsConfigOrchestratorImpl, ServicePublisherManagerSubscribeError};

    type MockedMetricsConfigOrchestratorImpl<'a> = MetricsConfigOrchestratorImpl<
        MockMetricsReportGenerator,
        MockMetricsReportGeneratorFactory,
        MockNodeFactory,
        MockPublisher,
        MockPublisherFactory,
        MockTrigger,
        MockTriggerFactory,
    >;

    const AGGREGATION_PUBLISHER_NAME: &str = "AggregationPublisher1";
    const CONDITIONAL_TRIGGER_NAME: &str = "ConditionalTrigger1";
    const DATA_TRIGGER_NAME: &str = "DataTrigger1";
    const END_TRIGGER_NAME: &str = "end_trigger";
    const FINISH_TRIGGER_NAME: &str = "finish_trigger";
    const INVALID_PUBLISHER_NAME: &str = "Publisher~0";
    const INVALID_TRIGGER_NAME: &str = "Trigger~0";
    const METRICS_REPORT_CONFIG_NAME: &str = "MetricsReportConfig1";
    const REPORT_GENERATOR_NAME: &str = "report_generator";
    const SERVICE_PUBLISHER_NAME: &str = "ServicePublisher1";
    const SOME_PUBLISHER_NAME: &str = "Publisher1";
    const SOME_TRIGGER_NAME: &str = "Trigger1";
    const START_TRIGGER_NAME: &str = "start_trigger";
    const TEST_MESSAGE_TYPE_NAME: &str = ".android.sdv.telemetry.test.TestMessage";
    const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

    struct MockMetricsReportGenerator {
        name: String,
        activate_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,
        deactivate_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,
    }

    impl MockMetricsReportGenerator {
        fn new(name: String) -> Self {
            MockMetricsReportGenerator { name, activate_callback: None, deactivate_callback: None }
        }

        fn set_activate_callback(&mut self, callback: impl Fn(&str) + Send + Sync + 'static) {
            self.activate_callback.replace(Box::new(callback));
        }

        fn set_deactivate_callback(&mut self, callback: impl Fn(&str) + Send + Sync + 'static) {
            self.deactivate_callback.replace(Box::new(callback));
        }
    }

    #[async_trait]
    impl MetricsReportGenerator for MockMetricsReportGenerator {
        fn get_name(&self) -> &str {
            &self.name
        }

        async fn activate(self: &Arc<Self>) {
            if let Some(callback) = &self.activate_callback {
                callback(&self.name);
            }
        }

        async fn deactivate(&self) {
            if let Some(callback) = &self.deactivate_callback {
                callback(&self.name);
            }
        }
    }

    struct MockMetricsReportGeneratorFactory {
        report_generators: Mutex<HashMap<String, Arc<MockMetricsReportGenerator>>>,
        /// When set, the factory will return this error once.
        error: Mutex<Option<MetricsReportGeneratorFactoryError>>,
    }

    impl MockMetricsReportGeneratorFactory {
        fn new() -> Self {
            MockMetricsReportGeneratorFactory {
                report_generators: HashMap::new().into(),
                error: None.into(),
            }
        }

        fn add_report_generators(&self, report_generators: Vec<Arc<MockMetricsReportGenerator>>) {
            self.report_generators.lock().unwrap().extend(
                report_generators.into_iter().map(|generator| (generator.name.clone(), generator)),
            );
        }
    }

    impl MetricsReportGeneratorFactory<MockMetricsReportGenerator, MockTrigger>
        for MockMetricsReportGeneratorFactory
    {
        fn create(
            &self,
            config: &MetricsReportConfig,
            _context: &MetricsReportGeneratorFactoryCreateContext<MockTrigger>,
        ) -> Result<Arc<MockMetricsReportGenerator>, MetricsReportGeneratorFactoryError> {
            if let Some(error) = self.error.lock().unwrap().take() {
                return Err(error);
            }

            let report_generator = if let Some(report_generator) =
                self.report_generators.lock().unwrap().remove(&config.name)
            {
                report_generator
            } else {
                Arc::new(MockMetricsReportGenerator::new(config.name.clone()))
            };

            Ok(report_generator)
        }

        fn validate(
            &self,
            _config: &MetricsReportConfig,
            _context: &MetricsReportGeneratorFactoryValidateContext,
        ) -> Result<(), MetricsReportGeneratorFactoryError> {
            self.error.lock().unwrap().take().map_or_else(|| Ok(()), Err)
        }
    }

    struct MockNodeFactory {
        fail_create_nodes: bool,
        fail_validate: bool,
        num_nodes: Mutex<usize>,
        message_providers: Mutex<Vec<Arc<dyn MessageProvider>>>,
    }

    impl MockNodeFactory {
        fn new() -> Self {
            MockNodeFactory {
                fail_create_nodes: false,
                fail_validate: false,
                num_nodes: Mutex::new(0),
                message_providers: Mutex::new(vec![]),
            }
        }

        /// Cause `validate` method to return an error.
        fn fail_validate(&mut self) {
            self.fail_validate = true;
        }

        /// Cause `create_nodes` method to return an error.
        fn fail_create_nodes(&mut self) {
            self.fail_create_nodes = true;
        }
    }

    impl NodeFactory for MockNodeFactory {
        fn validate(
            &self,
            proto_nodes: &[NodeProto],
            _context: &NodeFactoryContext,
        ) -> Result<Vec<NodeValueType>, NodeError> {
            if self.fail_validate {
                return Err(NodeError::InternalError { msg: "Simulated failure".to_string() });
            }
            Ok(vec![NodeValueType::Int; proto_nodes.len()])
        }

        fn create_nodes(
            &self,
            proto_nodes: &[NodeProto],
            context: &NodeFactoryContext,
        ) -> Result<(Vec<Arc<Node>>, Vec<NodeValueType>), NodeError> {
            if self.fail_create_nodes {
                return Err(NodeError::InternalError { msg: "Simulated failure".to_string() });
            }

            *self.num_nodes.lock().unwrap() = proto_nodes.len();
            self.message_providers
                .lock()
                .unwrap()
                .push(context.message_provider.as_ref().unwrap().clone());

            let size = proto_nodes.len();
            let empty_node = Arc::new(Node::ConstantLeaf { value: NodeValue::None });
            let node_type = NodeValueType::None;
            Ok((vec![empty_node; size], vec![node_type; size]))
        }
    }

    struct MockPublisherFactory {
        publishers: Mutex<HashMap<String, Arc<MockPublisher>>>,
        create_callback:
            Option<Box<dyn Fn(&PublisherProto) -> Option<PublisherFactoryError> + Send + Sync>>,
        validate_callback:
            Option<Box<dyn Fn(&PublisherProto) -> Option<PublisherFactoryError> + Send + Sync>>,
    }

    impl MockPublisherFactory {
        fn new() -> Self {
            MockPublisherFactory {
                publishers: HashMap::new().into(),
                create_callback: None,
                validate_callback: None,
            }
        }

        fn add_publishers(&mut self, publishers: Vec<Arc<MockPublisher>>) {
            self.publishers.lock().unwrap().extend(
                publishers
                    .into_iter()
                    .map(|publisher| (publisher.get_name().to_owned(), publisher)),
            );
        }

        fn set_create_callback(
            &mut self,
            callback: impl Fn(&PublisherProto) -> Option<PublisherFactoryError> + Send + Sync + 'static,
        ) {
            self.create_callback.replace(Box::new(callback));
        }

        fn set_validate_callback(
            &mut self,
            callback: impl Fn(&PublisherProto) -> Option<PublisherFactoryError> + Send + Sync + 'static,
        ) {
            self.validate_callback.replace(Box::new(callback));
        }
    }

    impl PublisherFactory<MockPublisher, MockTrigger> for MockPublisherFactory {
        fn create(
            &self,
            publisher_proto: &PublisherProto,
            _context: &PublisherFactoryCreateContext<MockTrigger>,
        ) -> Result<Arc<MockPublisher>, PublisherFactoryError> {
            if let Some(callback) = &self.create_callback {
                if let Some(res) = callback(publisher_proto) {
                    return Err(res);
                }
            }

            let publisher = if let Some(publisher) =
                self.publishers.lock().unwrap().remove(publisher_proto.name.as_str())
            {
                publisher
            } else {
                Arc::new(MockPublisher::new(publisher_proto.name.clone()))
            };

            Ok(publisher)
        }

        fn validate(
            &self,
            publisher_proto: &PublisherProto,
            _context: &PublisherFactoryValidateContext,
        ) -> Result<(), PublisherFactoryError> {
            if let Some(callback) = &self.validate_callback {
                if let Some(res) = callback(publisher_proto) {
                    return Err(res);
                }
            }

            Ok(())
        }
    }

    struct MockTriggerFactory {
        triggers: Mutex<HashMap<String, Arc<MockTrigger>>>,
        create_callback:
            Option<Box<dyn Fn(&TriggerProto) -> Option<TriggerFactoryError> + Send + Sync>>,
        validate_callback:
            Option<Box<dyn Fn(&TriggerProto) -> Option<TriggerFactoryError> + Send + Sync>>,
    }

    impl MockTriggerFactory {
        fn new() -> Self {
            MockTriggerFactory {
                triggers: HashMap::new().into(),
                create_callback: None,
                validate_callback: None,
            }
        }

        fn add_triggers(&mut self, triggers: Vec<Arc<MockTrigger>>) {
            self.triggers.lock().unwrap().extend(
                triggers.into_iter().map(|trigger| (trigger.get_name().to_owned(), trigger)),
            );
        }

        fn set_create_callback(
            &mut self,
            callback: impl Fn(&TriggerProto) -> Option<TriggerFactoryError> + Send + Sync + 'static,
        ) {
            self.create_callback.replace(Box::new(callback));
        }

        fn set_validate_callback(
            &mut self,
            callback: impl Fn(&TriggerProto) -> Option<TriggerFactoryError> + Send + Sync + 'static,
        ) {
            self.validate_callback.replace(Box::new(callback));
        }
    }

    impl TriggerFactory<MockPublisher, MockTrigger> for MockTriggerFactory {
        fn create(
            &self,
            trigger_proto: &TriggerProto,
            _context: &TriggerFactoryCreateContext<MockPublisher, MockTrigger>,
        ) -> Result<Arc<MockTrigger>, TriggerFactoryError> {
            if let Some(callback) = &self.create_callback {
                if let Some(res) = callback(trigger_proto) {
                    return Err(res);
                }
            }

            let trigger = if let Some(trigger) =
                self.triggers.lock().unwrap().remove(trigger_proto.name.as_str())
            {
                trigger
            } else {
                Arc::new(MockTrigger::new(trigger_proto.name.clone()))
            };

            Ok(trigger)
        }

        fn validate(
            &self,
            trigger_proto: &TriggerProto,
            _context: &TriggerFactoryValidateContext,
        ) -> Result<(), TriggerFactoryError> {
            if let Some(callback) = &self.validate_callback {
                if let Some(res) = callback(trigger_proto) {
                    return Err(res);
                }
            }

            Ok(())
        }
    }

    impl MockedMetricsConfigOrchestratorImpl<'_> {
        fn new() -> Self {
            MetricsConfigOrchestratorImpl {
                configs: Mutex::new(HashMap::new()),
                service_publisher_manager: Arc::new(
                    MockServicePublisherManager::new_with_descriptor(Empty::descriptor()),
                ),
                metrics_report_generator_factory: MockMetricsReportGeneratorFactory::new(),
                node_factory: MockNodeFactory::new(),
                publisher_factory: MockPublisherFactory::new(),
                trigger_factory: MockTriggerFactory::new(),
                finish_trigger_fired_callback: Arc::new(|_| {}),
                report_ready_callback: Arc::new(|_| {}),
                phantom_generator: PhantomData,
            }
        }
    }

    fn create_test_file_descriptor_protos() -> Vec<FileDescriptorProto> {
        let fds = get_test_file_descriptors();
        fds.iter().map(|fd| fd.proto()).cloned().collect()
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

    fn create_valid_config_with_metrics_report_config() -> MetricsConfigProto {
        let mut report_config = MetricsReportConfig::new();
        report_config.name = METRICS_REPORT_CONFIG_NAME.to_owned();

        let mut config = MetricsConfigProto::new();
        config.metrics_report_configs.push(report_config);
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
        // The cycle is:
        // DataTrigger <- ConditionalTrigger <- AggregationPublisher
        //       L_______________________________________^
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
        let mut proto_message_builder = ProtoMessageBuilder::new();
        proto_message_builder.message_type = TEST_MESSAGE_TYPE_NAME.to_string();
        aggregation_publisher.message_builder = protobuf::MessageField::some(proto_message_builder);
        publisher.set_aggregation_publisher(aggregation_publisher);

        let mut config = MetricsConfigProto::new();
        config.descriptor_protos = create_test_file_descriptor_protos();
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
        let mut proto_message_builder = ProtoMessageBuilder::new();
        proto_message_builder.message_type = TEST_MESSAGE_TYPE_NAME.to_string();
        aggregation_publisher.message_builder = protobuf::MessageField::some(proto_message_builder);
        publisher.set_aggregation_publisher(aggregation_publisher);

        let mut config = MetricsConfigProto::new();
        config.descriptor_protos = create_test_file_descriptor_protos();
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

    fn create_config_with_duplicate_metrics_report_config_name() -> MetricsConfigProto {
        let mut metrics_report1 = MetricsReportConfig::new();
        metrics_report1.name = METRICS_REPORT_CONFIG_NAME.to_owned();

        let mut metrics_report2 = MetricsReportConfig::new();
        metrics_report2.name = METRICS_REPORT_CONFIG_NAME.to_owned();

        let mut config = MetricsConfigProto::new();
        config.metrics_report_configs.push(metrics_report1);
        config.metrics_report_configs.push(metrics_report2);
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
        let mut proto_message_builder = ProtoMessageBuilder::new();
        proto_message_builder.message_type = TEST_MESSAGE_TYPE_NAME.to_string();
        aggregation_publisher.message_builder = protobuf::MessageField::some(proto_message_builder);
        publisher2.set_aggregation_publisher(aggregation_publisher);

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.descriptor_protos = create_test_file_descriptor_protos();
        config.publishers.push(publisher1);
        config.publishers.push(publisher2);
        config.triggers.push(trigger1);
        config.triggers.push(trigger2);
        config.expression_nodes.push(NodeProto::new());
        config
    }

    fn create_valid_config_with_known_protobuf_type() -> MetricsConfigProto {
        let mut descriptor_proto = DescriptorProto::new();
        descriptor_proto.set_name("WellKnownTypeMessage".to_owned());

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto
            .set_type_name(".".to_owned() + well_known_types::any::Any::descriptor().full_name());
        field_descriptor_proto.set_name("any".to_owned());
        field_descriptor_proto.set_number(1);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto
            .set_type_name(".".to_owned() + well_known_types::api::Api::descriptor().full_name());
        field_descriptor_proto.set_name("api".to_owned());
        field_descriptor_proto.set_number(2);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned() + well_known_types::duration::Duration::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("duration".to_owned());
        field_descriptor_proto.set_number(3);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned() + well_known_types::empty::Empty::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("empty".to_owned());
        field_descriptor_proto.set_number(4);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned() + well_known_types::field_mask::FieldMask::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("field_mask".to_owned());
        field_descriptor_proto.set_number(5);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned()
                + well_known_types::source_context::SourceContext::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("source_context".to_owned());
        field_descriptor_proto.set_number(6);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned() + well_known_types::struct_::Struct::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("struct".to_owned());
        field_descriptor_proto.set_number(7);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned() + well_known_types::timestamp::Timestamp::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("timestamp".to_owned());
        field_descriptor_proto.set_number(8);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned() + well_known_types::type_::Type::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("type".to_owned());
        field_descriptor_proto.set_number(9);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut field_descriptor_proto = FieldDescriptorProto::new();
        field_descriptor_proto.set_type(field_descriptor_proto::Type::TYPE_MESSAGE);
        field_descriptor_proto.set_type_name(
            ".".to_owned() + well_known_types::wrappers::Int32Value::descriptor().full_name(),
        );
        field_descriptor_proto.set_name("int32_value".to_owned());
        field_descriptor_proto.set_number(10);
        descriptor_proto.field.push(field_descriptor_proto);

        let mut file_descriptor_proto = FileDescriptorProto::new();
        file_descriptor_proto
            .dependency
            .push(well_known_types::any::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::api::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::duration::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::empty::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::field_mask::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::source_context::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::struct_::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::timestamp::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::type_::file_descriptor().name().to_owned());
        file_descriptor_proto
            .dependency
            .push(well_known_types::wrappers::file_descriptor().name().to_owned());
        file_descriptor_proto.set_name("well_known_type_message.proto".to_owned());
        file_descriptor_proto.message_type.push(descriptor_proto);

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.descriptor_protos = vec![file_descriptor_proto];
        config.metrics_report_configs.push(MetricsReportConfig::new());
        config
    }

    #[tokio::test]
    async fn initialize_config_duplicate_publisher_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_publisher_name();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicatePublisherDefinition { publisher_name } if publisher_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_duplicate_trigger_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_trigger_name();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_duplicate_publisher_trigger_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_publisher_trigger_name();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateTriggerDefinition { trigger_name } if trigger_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_publisher_trigger_cycle_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_publisher_trigger_cycle();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::CycledPublishersTriggers),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_aggregation_publisher_invalid_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_aggregation_publisher_trigger();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == INVALID_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_data_trigger_invalid_publisher_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_data_trigger_publisher();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == INVALID_PUBLISHER_NAME && dependency_type == PUBLISHER),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_conditional_trigger_invalid_parent_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_invalid_conditional_trigger_parent_trigger();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == INVALID_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_duplicate_metrics_report_config_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_metrics_report_config_name();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateMetricsReportConfigName { name } if name == METRICS_REPORT_CONFIG_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_duplicate_uuid_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let (config1, config2) = create_configs_with_duplicate_uuid();
        orchestrator.initialize_config(&config1).await.unwrap();

        let err = orchestrator.initialize_config(&config2).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid } if uuid == UUID),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn initialize_config_invalid_expression_nodes_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.node_factory.fail_create_nodes();

        let mut config = MetricsConfigProto::new();
        config.expression_nodes.push(NodeProto::new());

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidExpressionNodes { .. }),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_publisher_factory_error_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator
            .publisher_factory
            .set_create_callback(|_| Some(PublisherFactoryError::UnknownPublisherType));
        let config = create_valid_config_with_publisher();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidPublisherDefinition { factory_error: PublisherFactoryError::UnknownPublisherType, publisher_name } if publisher_name == SOME_PUBLISHER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_trigger_factory_error_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator
            .trigger_factory
            .set_create_callback(|_| Some(TriggerFactoryError::UnknownTriggerType));

        let config = create_valid_config_with_trigger();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidTriggerDefinition { factory_error: TriggerFactoryError::UnknownTriggerType, trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn initialize_config_metrics_report_config_factory_error_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.metrics_report_generator_factory.error.lock().unwrap().replace(
            MetricsReportGeneratorFactoryError::InvalidTriggerName {
                trigger_name: SOME_TRIGGER_NAME.to_owned(),
            },
        );
        let config = create_valid_config_with_metrics_report_config();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        let MetricsConfigOrchestratorError::InvalidMetricsReportConfig {
            report_config_name,
            factory_error,
        } = err
        else {
            panic!("Unexpected error: {:?}", err);
        };
        assert_eq!(report_config_name, METRICS_REPORT_CONFIG_NAME);
        assert!(
            matches!(&factory_error, MetricsReportGeneratorFactoryError::InvalidTriggerName { trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            factory_error
        );
    }

    #[tokio::test]
    async fn initialize_config_with_dependencies_success() {
        let mut value1 = Box::new(Int32Value::new());
        value1.value = 101;
        let mut value2 = Box::new(Int32Value::new());
        value2.value = 10101;

        let mut service_publisher_mock = MockPublisher::new(SERVICE_PUBLISHER_NAME.to_owned());
        service_publisher_mock.latest_message.replace(value1.clone());

        let mut aggregation_publisher_mock =
            MockPublisher::new(AGGREGATION_PUBLISHER_NAME.to_owned());
        aggregation_publisher_mock.latest_message.replace(value2.clone());

        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let factory_calls_clone1 = factory_calls.clone();
        let factory_calls_clone2 = factory_calls.clone();
        orchestrator.publisher_factory.add_publishers(vec![
            Arc::new(service_publisher_mock),
            Arc::new(aggregation_publisher_mock),
        ]);
        orchestrator.publisher_factory.set_create_callback(move |publisher_proto| {
            factory_calls_clone1.lock().unwrap().push(publisher_proto.name.clone());
            None
        });
        orchestrator.trigger_factory.set_create_callback(move |trigger_proto| {
            factory_calls_clone2.lock().unwrap().push(trigger_proto.name.clone());
            None
        });

        let config = create_valid_config_with_dependencies();

        orchestrator.initialize_config(&config).await.unwrap();

        let expected_calls = vec![
            SERVICE_PUBLISHER_NAME,
            DATA_TRIGGER_NAME,
            CONDITIONAL_TRIGGER_NAME,
            AGGREGATION_PUBLISHER_NAME,
        ];
        assert_eq!(*factory_calls.lock().unwrap(), expected_calls);
        assert_eq!(*orchestrator.node_factory.num_nodes.lock().unwrap(), 1);
        {
            let message_providers = orchestrator.node_factory.message_providers.lock().unwrap();
            assert_eq!(message_providers.len(), 1);
            assert_eq!(
                &message_providers[0].get_message(SERVICE_PUBLISHER_NAME).unwrap().unwrap(),
                &(value1 as Box<dyn MessageDyn>)
            );
            assert_eq!(
                &message_providers[0].get_message(AGGREGATION_PUBLISHER_NAME).unwrap().unwrap(),
                &(value2 as Box<dyn MessageDyn>)
            );
        }
        let config = {
            let configs = orchestrator.configs.lock().unwrap();
            assert_eq!(configs.len(), 1);
            configs[&config.uuid].clone()
        };
        assert_eq!(config.publishers.len(), 2);
        let config_state = config.state.lock().await;
        assert!(
            matches!(&*config_state, MetricsConfigState::NotStarted),
            "Unexpected config state: {:?}",
            config_state
        );
    }

    #[tokio::test]
    async fn initialize_config_invalid_start_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = SOME_TRIGGER_NAME.to_owned();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == SOME_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn initialize_config_invalid_end_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.end_trigger_name = SOME_TRIGGER_NAME.to_owned();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == SOME_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn initialize_config_invalid_finish_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.finish_trigger_name = SOME_TRIGGER_NAME.to_owned();

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == SOME_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn initialize_config_publisher_doesnt_activate_fails() {
        // Setup mock factory.
        let mut publisher_mock = MockPublisher::new(SOME_PUBLISHER_NAME.to_owned());
        publisher_mock.set_activate_callback(move |_| {
            Some(PublisherActivateError::ServiceAdapterError {
                error: ServiceAdapterSubscribeError::SubscriptionFailed {
                    error: ServicePublisherManagerSubscribeError::ServiceNotFound {
                        service_name: String::default(),
                    },
                },
            })
        });

        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.publisher_factory.add_publishers(vec![publisher_mock.into()]);

        // Create MetricsConfig proto.
        let mut publisher = PublisherProto::new();
        publisher.name = SOME_PUBLISHER_NAME.to_owned();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.publishers.push(publisher);

        let err = orchestrator.initialize_config(&config).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::UnableToActivatePublisher { publisher_name, error } if publisher_name == SOME_PUBLISHER_NAME && matches!(error, PublisherActivateError::ServiceAdapterError { .. })),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn initialize_config_with_lifecycle_triggers_success() {
        const ACTIVATE: &str = "activate";
        const DEACTIVATE: &str = "deactivate";
        const SUBSCRIBE: &str = "subscribe";
        const UNSUBSCRIBE: &str = "unsubscribe";

        // Setup callbacks that log actions and current MetricsConfig state.
        let factory_calls = Arc::new(Mutex::new(Vec::new()));

        let factory_calls_clone = factory_calls.clone();
        let subscribe_callback = move |name: &str| {
            factory_calls_clone.lock().unwrap().push((SUBSCRIBE.to_owned(), name.to_owned()))
        };
        let factory_calls_clone = factory_calls.clone();
        let unsubscribe_callback = move |name: &str| {
            factory_calls_clone.lock().unwrap().push((UNSUBSCRIBE.to_owned(), name.to_owned()))
        };

        // Create start, end and finish mock triggers that use callbacks.
        let mut start_trigger_mock = MockTrigger::new_for_subscriber(
            START_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigStart,
        );
        start_trigger_mock.set_subscribe_callback(subscribe_callback.clone());
        start_trigger_mock.set_unsubscribe_callback(unsubscribe_callback.clone());

        let mut end_trigger_mock = MockTrigger::new_for_subscriber(
            END_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigEnd,
        );
        end_trigger_mock.set_subscribe_callback(subscribe_callback.clone());
        end_trigger_mock.set_unsubscribe_callback(unsubscribe_callback);

        let mut finish_trigger_mock = MockTrigger::new_for_subscriber(
            FINISH_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigFinish,
        );
        finish_trigger_mock.set_subscribe_callback(subscribe_callback);

        // Create and setup mock publisher.
        let mut publisher_mock = MockPublisher::new(SOME_PUBLISHER_NAME.to_owned());

        let factory_calls_clone = factory_calls.clone();
        publisher_mock.set_activate_callback(move |name| {
            factory_calls_clone.lock().unwrap().push((ACTIVATE.to_owned(), name.to_owned()));
            None
        });

        // Create and setup mock report generator.
        let mut report_generator_mock =
            MockMetricsReportGenerator::new(REPORT_GENERATOR_NAME.to_owned());

        let factory_calls_clone = factory_calls.clone();
        report_generator_mock.set_activate_callback(move |name| {
            factory_calls_clone.lock().unwrap().push((ACTIVATE.to_owned(), name.to_owned()))
        });
        let factory_calls_clone = factory_calls.clone();
        report_generator_mock.set_deactivate_callback(move |name| {
            factory_calls_clone.lock().unwrap().push((DEACTIVATE.to_owned(), name.to_owned()))
        });

        // Create and setup orchestrator.
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let start_trigger_mock = Arc::new(start_trigger_mock);
        let end_trigger_mock = Arc::new(end_trigger_mock);
        orchestrator
            .metrics_report_generator_factory
            .add_report_generators(vec![report_generator_mock.into()]);
        orchestrator.publisher_factory.add_publishers(vec![publisher_mock.into()]);
        orchestrator.trigger_factory.add_triggers(vec![
            start_trigger_mock.clone(),
            end_trigger_mock.clone(),
            finish_trigger_mock.into(),
        ]);

        // Create metrics config.
        let mut start_trigger = TriggerProto::new();
        start_trigger.name = START_TRIGGER_NAME.to_owned();
        let mut end_trigger = TriggerProto::new();
        end_trigger.name = END_TRIGGER_NAME.to_owned();
        let mut finish_trigger = TriggerProto::new();
        finish_trigger.name = FINISH_TRIGGER_NAME.to_owned();
        let mut publisher = PublisherProto::new();
        publisher.name = SOME_PUBLISHER_NAME.to_owned();
        let mut metrics_report_config = MetricsReportConfig::new();
        metrics_report_config.name = REPORT_GENERATOR_NAME.to_owned();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = start_trigger.name.clone();
        config.end_trigger_name = end_trigger.name.clone();
        config.finish_trigger_name = finish_trigger.name.clone();
        config.metrics_report_configs.push(metrics_report_config);
        config.triggers.extend([start_trigger, end_trigger, finish_trigger]);
        config.publishers.push(publisher);

        // Initialize config.
        orchestrator.initialize_config(&config).await.unwrap();

        // Fire twice to verify that the second fire event is ignored.
        start_trigger_mock.fire().await;
        start_trigger_mock.fire().await;

        // Yield to make sure trigger handler task is processed.
        tokio::task::yield_now().await;

        // Fire twice to verify that the second fire event is ignored.
        end_trigger_mock.fire().await;
        end_trigger_mock.fire().await;

        // Yield to make sure trigger handler task is processed.
        tokio::task::yield_now().await;

        let expected_calls = vec![
            (ACTIVATE.to_owned(), SOME_PUBLISHER_NAME.to_owned()),
            (SUBSCRIBE.to_owned(), START_TRIGGER_NAME.to_owned()),
            (SUBSCRIBE.to_owned(), FINISH_TRIGGER_NAME.to_owned()),
            (UNSUBSCRIBE.to_owned(), START_TRIGGER_NAME.to_owned()),
            (ACTIVATE.to_owned(), REPORT_GENERATOR_NAME.to_owned()),
            (SUBSCRIBE.to_owned(), END_TRIGGER_NAME.to_owned()),
            (UNSUBSCRIBE.to_owned(), END_TRIGGER_NAME.to_owned()),
            (DEACTIVATE.to_owned(), REPORT_GENERATOR_NAME.to_owned()),
            (SUBSCRIBE.to_owned(), START_TRIGGER_NAME.to_owned()),
        ];
        assert_eq!(*factory_calls.lock().unwrap(), expected_calls);
        let config = { orchestrator.configs.lock().unwrap()[UUID].clone() };
        let config_state = config.state.lock().await;
        assert!(
            matches!(&*config_state, MetricsConfigState::NotStarted),
            "Unexpected config state: {:?}",
            config_state
        );
    }

    #[tokio::test]
    async fn initialize_config_activates_publishers_topologically() {
        // The names of the publishers set up in a way that alphabetic sorting
        // will never sort them topologically.
        const AGGREGATION_PUBLISHER_NAME_2: &str = "000_publisher";
        const AGGREGATION_DATA_TRIGGER_NAME: &str = "aggregation_data_trigger";
        const SERVICE_DATA_TRIGGER_NAME: &str = "service_data_trigger_name";

        // Setup mocks and call tracker.
        let activate_calls = Arc::new(Mutex::new(vec![]));

        let mut aggregation_publisher1_mock =
            MockPublisher::new(AGGREGATION_PUBLISHER_NAME.to_owned());
        let activate_calls_clone = activate_calls.clone();
        aggregation_publisher1_mock.set_activate_callback(move |name| {
            activate_calls_clone.lock().unwrap().push(name.to_owned());
            None
        });

        let mut service_publisher_mock = MockPublisher::new(SERVICE_PUBLISHER_NAME.to_owned());
        let activate_calls_clone = activate_calls.clone();
        service_publisher_mock.set_activate_callback(move |name| {
            activate_calls_clone.lock().unwrap().push(name.to_owned());
            None
        });

        let mut aggregation_publisher2_mock =
            MockPublisher::new(AGGREGATION_PUBLISHER_NAME_2.to_owned());
        let activate_calls_clone = activate_calls.clone();
        aggregation_publisher2_mock.set_activate_callback(move |name| {
            activate_calls_clone.lock().unwrap().push(name.to_owned());
            None
        });

        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.publisher_factory.add_publishers(vec![
            Arc::new(aggregation_publisher1_mock),
            Arc::new(service_publisher_mock),
            Arc::new(aggregation_publisher2_mock),
        ]);

        // Setup config protobuf messages with the following dependencies:
        // Aggregation publisher 2 -> Aggregation data trigger -> Aggregation publisher 1 ->
        // Service data trigger -> Service publisher.
        let mut service_publisher = PublisherProto::new();
        service_publisher.name = SERVICE_PUBLISHER_NAME.to_owned();
        service_publisher.mut_service_publisher();

        let mut service_data_trigger = TriggerProto::new();
        service_data_trigger.name = SERVICE_DATA_TRIGGER_NAME.to_owned();
        service_data_trigger.mut_data_trigger().publisher_name = service_publisher.name.clone();

        let mut aggregation_publisher1 = PublisherProto::new();
        aggregation_publisher1.name = AGGREGATION_PUBLISHER_NAME.to_owned();
        let aggregation_publisher = aggregation_publisher1.mut_aggregation_publisher();
        aggregation_publisher.message_builder.mut_or_insert_default().message_type =
            TEST_MESSAGE_TYPE_NAME.to_string();
        aggregation_publisher.trigger_names.push(service_data_trigger.name.clone());

        let mut aggregation_data_trigger = TriggerProto::new();
        aggregation_data_trigger.name = AGGREGATION_DATA_TRIGGER_NAME.to_owned();
        aggregation_data_trigger.mut_data_trigger().publisher_name =
            aggregation_publisher1.name.clone();

        let mut aggregation_publisher2 = PublisherProto::new();
        aggregation_publisher2.name = AGGREGATION_PUBLISHER_NAME_2.to_owned();
        let aggregation_publisher = aggregation_publisher2.mut_aggregation_publisher();
        aggregation_publisher.message_builder.mut_or_insert_default().message_type =
            TEST_MESSAGE_TYPE_NAME.to_string();
        aggregation_publisher.trigger_names.push(aggregation_data_trigger.name.clone());

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.descriptor_protos = create_test_file_descriptor_protos();
        config.publishers.push(aggregation_publisher2);
        config.publishers.push(aggregation_publisher1);
        config.publishers.push(service_publisher);
        config.triggers.push(aggregation_data_trigger);
        config.triggers.push(service_data_trigger);

        // Initialize config.
        orchestrator.initialize_config(&config).await.unwrap();

        // Verify that service publisher was activated first.
        let expected_activations = vec![
            SERVICE_PUBLISHER_NAME.to_owned(),
            AGGREGATION_PUBLISHER_NAME.to_owned(),
            AGGREGATION_PUBLISHER_NAME_2.to_owned(),
        ];
        assert_eq!(*activate_calls.lock().unwrap(), expected_activations);
    }

    #[tokio::test]
    async fn initialize_config_with_known_protobuf_type_success() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_valid_config_with_known_protobuf_type();

        orchestrator.initialize_config(&config).await.unwrap();
    }

    #[tokio::test]
    async fn initialize_config_callback_triggered_when_finish_trigger_fires() {
        // Setup orchestrator.
        let finish_trigger_mock = Arc::new(MockTrigger::new_for_subscriber(
            FINISH_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigFinish,
        ));
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let (tx, rx) = oneshot::channel();
        let maybe_tx = Mutex::new(Some(tx));
        orchestrator.finish_trigger_fired_callback =
            Arc::new(move |uuid| maybe_tx.lock().unwrap().take().unwrap().send(uuid).unwrap());
        orchestrator.trigger_factory.add_triggers(vec![finish_trigger_mock.clone()]);

        // Setup Metrics Config.
        let mut finish_trigger_proto = TriggerProto::new();
        finish_trigger_proto.name = FINISH_TRIGGER_NAME.to_owned();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.finish_trigger_name = finish_trigger_proto.name.clone();
        config.triggers.push(finish_trigger_proto);

        // Initialize config.
        orchestrator.initialize_config(&config).await.unwrap();

        // Fire finish trigger. This should finish the config.
        finish_trigger_mock.fire().await;

        // Verify uuid that was received by the callback.
        assert_eq!(&rx.await.unwrap(), UUID);
    }

    #[tokio::test]
    async fn finish_config_invalid_config_uuid_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let err = orchestrator.finish_config(UUID).await.err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidConfigUuid { uuid } if uuid == UUID),
            "Unexpected error: {:?}",
            err
        )
    }

    #[tokio::test]
    async fn finish_config_not_started_unsubscribes() {
        // Setup trigger mocks.
        let name_container = Arc::new(Mutex::new(vec![]));
        let mut start_trigger_mock = MockTrigger::new_for_subscriber(
            START_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigStart,
        );
        let name_container_clone = name_container.clone();
        start_trigger_mock.set_unsubscribe_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned())
        });

        let mut end_trigger_mock = MockTrigger::new_for_subscriber(
            END_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigEnd,
        );
        let name_container_clone = name_container.clone();
        end_trigger_mock.set_unsubscribe_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned())
        });

        let mut finish_trigger_mock = MockTrigger::new_for_subscriber(
            FINISH_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigFinish,
        );
        let name_container_clone = name_container.clone();
        finish_trigger_mock.set_unsubscribe_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned())
        });

        // Setup mock publishers.
        let mut service_publisher_mock = MockPublisher::new(SERVICE_PUBLISHER_NAME.to_owned());
        let name_container_clone = name_container.clone();
        service_publisher_mock.set_deactivate_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned());
            None
        });

        let mut aggregation_publisher_mock =
            MockPublisher::new(AGGREGATION_PUBLISHER_NAME.to_owned());
        let name_container_clone = name_container.clone();
        aggregation_publisher_mock.set_deactivate_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned());
            None
        });

        // Setup orchestrator.
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let start_trigger_mock = Arc::new(start_trigger_mock);
        let end_trigger_mock = Arc::new(end_trigger_mock);
        let finish_trigger_mock = Arc::new(finish_trigger_mock);
        orchestrator.publisher_factory.add_publishers(vec![
            Arc::new(service_publisher_mock),
            Arc::new(aggregation_publisher_mock),
        ]);
        orchestrator.trigger_factory.add_triggers(vec![
            start_trigger_mock.clone(),
            end_trigger_mock.clone(),
            finish_trigger_mock.clone(),
        ]);

        // Setup Metrics Config.
        let mut service_publisher_proto = PublisherProto::new();
        service_publisher_proto.name = SERVICE_PUBLISHER_NAME.to_owned();
        let mut aggregation_publisher_proto = PublisherProto::new();
        aggregation_publisher_proto.name = AGGREGATION_PUBLISHER_NAME.to_owned();
        let mut start_trigger_proto = TriggerProto::new();
        start_trigger_proto.name = START_TRIGGER_NAME.to_owned();
        let mut end_trigger_proto = TriggerProto::new();
        end_trigger_proto.name = END_TRIGGER_NAME.to_owned();
        let mut finish_trigger_proto = TriggerProto::new();
        finish_trigger_proto.name = FINISH_TRIGGER_NAME.to_owned();

        let aggregation_publisher = aggregation_publisher_proto.mut_aggregation_publisher();
        aggregation_publisher.trigger_names.push(start_trigger_proto.name.clone());
        let mut proto_message_builder = ProtoMessageBuilder::new();
        proto_message_builder.message_type = TEST_MESSAGE_TYPE_NAME.to_string();
        aggregation_publisher.message_builder = protobuf::MessageField::some(proto_message_builder);

        let data_trigger = start_trigger_proto.mut_data_trigger();
        data_trigger.publisher_name = service_publisher_proto.name.clone();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = start_trigger_proto.name.clone();
        config.end_trigger_name = end_trigger_proto.name.clone();
        config.finish_trigger_name = finish_trigger_proto.name.clone();
        config.descriptor_protos = create_test_file_descriptor_protos();
        config.publishers.extend([service_publisher_proto, aggregation_publisher_proto]);
        config.triggers.extend([start_trigger_proto, end_trigger_proto, finish_trigger_proto]);

        // Initialize config.
        orchestrator.initialize_config(&config).await.unwrap();

        // Store metrics config Rust object.
        let metrics_config = orchestrator.configs.lock().unwrap()[UUID].clone();

        // Finish config.
        orchestrator.finish_config(UUID).await.unwrap();

        // Fire start trigger. This should be ignored.
        start_trigger_mock.fire().await;

        // Verify config state.
        {
            let config_state = metrics_config.state.lock().await;
            assert!(
                matches!(*config_state, MetricsConfigState::Finished),
                "Unexpected state: {:?}",
                *config_state
            );
        }

        // Verify that the config was removed from the map.
        assert!(
            !orchestrator.configs.lock().unwrap().contains_key(UUID),
            "Config was not removed from the map"
        );

        // Verify the order of calls.
        assert_eq!(
            *name_container.lock().unwrap(),
            [
                FINISH_TRIGGER_NAME,
                START_TRIGGER_NAME,
                AGGREGATION_PUBLISHER_NAME,
                SERVICE_PUBLISHER_NAME
            ]
        );
    }

    #[tokio::test]
    async fn finish_config_running_unsubscribes_and_deactivates() {
        // Setup trigger mocks.
        let name_container = Arc::new(Mutex::new(vec![]));

        let mut end_trigger_mock = MockTrigger::new_for_subscriber(
            END_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigEnd,
        );
        let name_container_clone = name_container.clone();
        end_trigger_mock.set_unsubscribe_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned())
        });

        let mut finish_trigger_mock = MockTrigger::new_for_subscriber(
            FINISH_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigFinish,
        );
        let name_container_clone = name_container.clone();
        finish_trigger_mock.set_unsubscribe_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned())
        });

        // Setup mock publishers.
        let mut service_publisher_mock = MockPublisher::new(SERVICE_PUBLISHER_NAME.to_owned());
        let name_container_clone = name_container.clone();
        service_publisher_mock.set_deactivate_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned());
            None
        });

        let mut aggregation_publisher_mock =
            MockPublisher::new(AGGREGATION_PUBLISHER_NAME.to_owned());
        let name_container_clone = name_container.clone();
        aggregation_publisher_mock.set_deactivate_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned());
            None
        });

        // Setup mock report generator.
        let mut report_generator_mock =
            MockMetricsReportGenerator::new(REPORT_GENERATOR_NAME.to_owned());
        let name_container_clone = name_container.clone();
        report_generator_mock.set_deactivate_callback(move |name| {
            name_container_clone.lock().unwrap().push(name.to_owned())
        });

        // Setup orchestrator.
        let start_trigger_mock = Arc::new(MockTrigger::new_for_subscriber(
            START_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::MetricsConfigStart,
        ));
        let end_trigger_mock = Arc::new(end_trigger_mock);
        let finish_trigger_mock = Arc::new(finish_trigger_mock);
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.publisher_factory.add_publishers(vec![
            Arc::new(service_publisher_mock),
            Arc::new(aggregation_publisher_mock),
        ]);
        orchestrator.trigger_factory.add_triggers(vec![
            start_trigger_mock.clone(),
            end_trigger_mock.clone(),
            finish_trigger_mock.clone(),
        ]);
        orchestrator
            .metrics_report_generator_factory
            .add_report_generators(vec![report_generator_mock.into()]);

        // Setup Metrics Config.
        let mut service_publisher_proto = PublisherProto::new();
        service_publisher_proto.name = SERVICE_PUBLISHER_NAME.to_owned();
        let mut aggregation_publisher_proto = PublisherProto::new();
        aggregation_publisher_proto.name = AGGREGATION_PUBLISHER_NAME.to_owned();
        let mut start_trigger_proto = TriggerProto::new();
        start_trigger_proto.name = START_TRIGGER_NAME.to_owned();
        let mut end_trigger_proto = TriggerProto::new();
        end_trigger_proto.name = END_TRIGGER_NAME.to_owned();
        let mut finish_trigger_proto = TriggerProto::new();
        finish_trigger_proto.name = FINISH_TRIGGER_NAME.to_owned();
        let mut metrics_report_config = MetricsReportConfig::new();
        metrics_report_config.name = REPORT_GENERATOR_NAME.to_owned();

        let aggregation_publisher = aggregation_publisher_proto.mut_aggregation_publisher();
        aggregation_publisher.trigger_names.push(start_trigger_proto.name.clone());
        let mut proto_message_builder = ProtoMessageBuilder::new();
        proto_message_builder.message_type = TEST_MESSAGE_TYPE_NAME.to_string();
        aggregation_publisher.message_builder = protobuf::MessageField::some(proto_message_builder);

        let data_trigger = start_trigger_proto.mut_data_trigger();
        data_trigger.publisher_name = service_publisher_proto.name.clone();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = start_trigger_proto.name.clone();
        config.end_trigger_name = end_trigger_proto.name.clone();
        config.finish_trigger_name = finish_trigger_proto.name.clone();
        config.descriptor_protos = create_test_file_descriptor_protos();
        config.metrics_report_configs.push(metrics_report_config);
        config.publishers.extend([service_publisher_proto, aggregation_publisher_proto]);
        config.triggers.extend([start_trigger_proto, end_trigger_proto, finish_trigger_proto]);

        // Initialize config.
        orchestrator.initialize_config(&config).await.unwrap();

        // Store metrics config Rust object.
        let metrics_config = orchestrator.configs.lock().unwrap()[UUID].clone();

        // Fire start trigger. This should start data collection.
        start_trigger_mock.fire().await;

        // Yield to make sure trigger handler task is processed.
        tokio::task::yield_now().await;

        // Finish config.
        orchestrator.finish_config(UUID).await.unwrap();

        // Fire end and finish triggers. This should be ignored.
        end_trigger_mock.fire().await;
        finish_trigger_mock.fire().await;

        // Verify config state.
        {
            let config_state = metrics_config.state.lock().await;
            assert!(
                matches!(*config_state, MetricsConfigState::Finished),
                "Unexpected state: {:?}",
                *config_state
            );
        }

        // Verify that the config was removed from the map.
        assert!(
            !orchestrator.configs.lock().unwrap().contains_key(UUID),
            "Config was not removed from the map"
        );

        // Verify that there was exactly one unsubscribe call.
        assert_eq!(
            *name_container.lock().unwrap(),
            [
                FINISH_TRIGGER_NAME,
                END_TRIGGER_NAME,
                REPORT_GENERATOR_NAME,
                AGGREGATION_PUBLISHER_NAME,
                SERVICE_PUBLISHER_NAME
            ]
        );
    }

    #[test]
    fn validate_config_with_invalid_expression_nodes_fails() {
        let mut orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.node_factory.fail_validate();
        let config = create_valid_config_with_dependencies();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidExpressionNodes { .. }),
            "Unexpected error: {:?}",
            err
        );
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
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == INVALID_TRIGGER_NAME && dependency_type == TRIGGER),
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
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == INVALID_PUBLISHER_NAME && dependency_type == PUBLISHER),
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
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == INVALID_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[test]
    fn validate_config_duplicate_metrics_report_config_name_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_config_with_duplicate_metrics_report_config_name();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::DuplicateMetricsReportConfigName { name } if name == METRICS_REPORT_CONFIG_NAME),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(orchestrator.configs.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn validate_config_duplicate_uuid_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let (config1, config2) = create_configs_with_duplicate_uuid();
        orchestrator.initialize_config(&config1).await.unwrap();

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
        orchestrator.node_factory.fail_validate();

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
    fn validate_config_metrics_report_config_factory_error_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        orchestrator.metrics_report_generator_factory.error.lock().unwrap().replace(
            MetricsReportGeneratorFactoryError::InvalidTriggerName {
                trigger_name: SOME_TRIGGER_NAME.to_owned(),
            },
        );
        let config = create_valid_config_with_metrics_report_config();

        let err = orchestrator.validate_config(&config).err().unwrap();

        let MetricsConfigOrchestratorError::InvalidMetricsReportConfig {
            report_config_name,
            factory_error,
        } = err
        else {
            panic!("Unexpected error: {:?}", err);
        };
        assert_eq!(report_config_name, METRICS_REPORT_CONFIG_NAME);
        assert!(
            matches!(&factory_error, MetricsReportGeneratorFactoryError::InvalidTriggerName { trigger_name } if trigger_name == SOME_TRIGGER_NAME),
            "Unexpected error: {:?}",
            factory_error
        );
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

    #[test]
    fn validate_config_with_known_protobuf_type_success() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();
        let config = create_valid_config_with_known_protobuf_type();

        orchestrator.validate_config(&config).unwrap();
    }

    #[test]
    fn validate_config_invalid_start_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.start_trigger_name = SOME_TRIGGER_NAME.to_owned();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == SOME_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_config_invalid_end_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.end_trigger_name = SOME_TRIGGER_NAME.to_owned();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == SOME_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_config_invalid_finish_trigger_fails() {
        let orchestrator = MockedMetricsConfigOrchestratorImpl::new();

        let mut config = MetricsConfigProto::new();
        config.uuid = UUID.to_owned();
        config.finish_trigger_name = SOME_TRIGGER_NAME.to_owned();

        let err = orchestrator.validate_config(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsConfigOrchestratorError::InvalidDependency { dependency_name, dependency_type } if dependency_name == SOME_TRIGGER_NAME && dependency_type == TRIGGER),
            "Unexpected error: {:?}",
            err
        );
    }
}
