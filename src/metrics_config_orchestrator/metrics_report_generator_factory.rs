// Copyright 2023 Google LLC

use protobuf::reflect::FileDescriptor;
use sdv_telemetry_proto_rust::metrics_configuration::{
    MetricsConfig as MetricsConfigProto, MetricsReport, MetricsReportConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::{node::Node, node_value_type::NodeValueType};
use crate::metrics_config_orchestrator::message_builder_factory::{
    MessageBuilderFactory, MessageBuilderFactoryCreateContext, MessageBuilderFactoryError,
    MessageBuilderFactoryValidateContext,
};
use crate::metrics_config_orchestrator::{
    MetricsReportGenerator, MetricsReportGeneratorImpl, Trigger,
};

#[cfg_attr(test, sdv_telemetry_utils_macros::with_super_trait(downcast_rs::Downcast))]
pub(crate) trait MetricsReportGeneratorFactory: Send + Sync {
    /// Returns a new metrics report generator based on the metrics report config
    /// protobuf message.
    fn create(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryCreateContext,
    ) -> Result<Arc<dyn MetricsReportGenerator>, MetricsReportGeneratorFactoryError>;

    /// Verifies that the metrics report config protobuf message is valid and can be used
    /// to create a metrics report generator.
    fn validate(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryValidateContext,
    ) -> Result<(), MetricsReportGeneratorFactoryError>;
}

#[cfg(test)]
downcast_rs::impl_downcast!(MetricsReportGeneratorFactory);

pub(crate) struct MetricsReportGeneratorFactoryCreateContext<'a> {
    pub config_proto: &'a MetricsConfigProto,
    pub report_ready_callback: &'a Arc<dyn Fn(MetricsReport) + Send + Sync>,
    pub triggers: &'a HashMap<String, Arc<dyn Trigger>>,
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
    pub node_types: &'a [Option<NodeValueType>],
}

pub(crate) struct MetricsReportGeneratorFactoryValidateContext<'a> {
    pub trigger_protos: &'a HashMap<String, ()>,
    pub file_descriptors: &'a [FileDescriptor],
    pub node_types: &'a [Option<NodeValueType>],
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsReportGeneratorFactoryError {
    #[error("MetricsReportConfig doesn't have a name")]
    NameNotSet,
    #[error("Trigger name {trigger_name} is invalid")]
    InvalidTriggerName { trigger_name: String },
    #[error("Trigger names in MetricsReportConfig are required for complete reports.")]
    TriggerNameRequired,
    #[error("MetricsReportConfig contains invalid ProtoMessageBuilder: {e}")]
    InvalidProtoMessageBuilder {
        #[from]
        e: MessageBuilderFactoryError,
    },
    #[error("MetricsReportConfig must specify the ProtoMessageBuilder")]
    ProtoMessageBuilderNotSet,
}

pub(crate) struct MetricsReportGeneratorFactoryImpl {
    pub(super) message_builder_factory: Arc<dyn MessageBuilderFactory>,
}

impl MetricsReportGeneratorFactoryImpl {
    pub fn new(message_builder_factory: Arc<dyn MessageBuilderFactory>) -> Self {
        Self { message_builder_factory }
    }
}

impl MetricsReportGeneratorFactory for MetricsReportGeneratorFactoryImpl {
    fn create(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryCreateContext,
    ) -> Result<Arc<dyn MetricsReportGenerator>, MetricsReportGeneratorFactoryError> {
        // Verify that the name is set.
        Self::verify_name_set(metrics_report_config)?;

        // Verify that trigger names are only left empty for incomplete reports.
        Self::verify_trigger_names_set_for_complete_reports(metrics_report_config)?;

        // Create message builder.
        let message_builder = self.message_builder_factory.create(
            format!("Metrics Report {}", metrics_report_config.name),
            metrics_report_config
                .message_builder
                .as_ref()
                .ok_or(MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet)?,
            &MessageBuilderFactoryCreateContext::new(
                context.file_descriptors,
                context.nodes,
                context.node_types,
            ),
        )?;

        // Retrieve associated triggers.
        let triggers = metrics_report_config
            .trigger_names
            .iter()
            .map(|name| {
                context
                    .triggers
                    .get(name)
                    .map(|trigger| Arc::clone(trigger) as Arc<dyn Trigger>)
                    .ok_or_else(|| MetricsReportGeneratorFactoryError::InvalidTriggerName {
                        trigger_name: name.clone(),
                    })
            })
            .collect::<Result<_, _>>()?;

        // Create and return a new report generator.
        Ok(Arc::new(MetricsReportGeneratorImpl::new(
            metrics_report_config,
            context.config_proto.uuid.clone(),
            context.config_proto.version,
            Arc::clone(context.report_ready_callback),
            triggers,
            message_builder.into(),
        )))
    }

    fn validate(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryValidateContext,
    ) -> Result<(), MetricsReportGeneratorFactoryError> {
        // Verify that the name is set.
        Self::verify_name_set(metrics_report_config)?;

        // Verify that trigger names are only left empty for incomplete reports.
        Self::verify_trigger_names_set_for_complete_reports(metrics_report_config)?;

        // Validate message builder protobuf message.
        self.message_builder_factory.validate(
            metrics_report_config
                .message_builder
                .as_ref()
                .ok_or(MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet)?,
            &MessageBuilderFactoryValidateContext::new(
                context.file_descriptors,
                context.node_types,
            ),
        )?;

        // Verify that trigger names for this report config exist.
        metrics_report_config.trigger_names.iter().try_for_each(|name| {
            if context.trigger_protos.contains_key(name) {
                Ok(())
            } else {
                Err(MetricsReportGeneratorFactoryError::InvalidTriggerName {
                    trigger_name: name.clone(),
                })
            }
        })
    }
}

impl MetricsReportGeneratorFactoryImpl {
    fn verify_name_set(
        metrics_report_config: &MetricsReportConfig,
    ) -> Result<(), MetricsReportGeneratorFactoryError> {
        if metrics_report_config.name.is_empty() {
            return Err(MetricsReportGeneratorFactoryError::NameNotSet);
        }
        Ok(())
    }

    fn verify_trigger_names_set_for_complete_reports(
        metrics_report_config: &MetricsReportConfig,
    ) -> Result<(), MetricsReportGeneratorFactoryError> {
        if !metrics_report_config.report_incomplete
            && metrics_report_config.trigger_names.is_empty()
        {
            return Err(MetricsReportGeneratorFactoryError::TriggerNameRequired);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use sdv_telemetry_proto_rust::metrics_configuration::ProtoMessageBuilder;
    use sdv_telemetry_utils::collection_extension::VecExtension;
    use std::time::Duration;

    use crate::metrics_config_orchestrator::message_builder_factory::mock::{
        get_test_file_descriptors, get_test_node_types, get_test_nodes, MockMessageBuilderFactory,
    };
    use crate::metrics_config_orchestrator::trigger::periodic::PeriodicTrigger;

    const METRICS_REPORT_CONFIG_NAME: &str = "mrc";
    const TRIGGER_NAME: &str = "trigger";
    const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const VERSION: u32 = 42;

    impl MetricsReportGeneratorFactoryImpl {
        // The following functions create a context with passed data and use it to call `create`
        // or `validate`. This is needed because context structs contain borrows and it's impossible
        // to have a function that creates and returns it - it can only be used in the function
        // where it's created.
        fn create_using_empty_context(
            &self,
            config: &MetricsReportConfig,
        ) -> Result<Arc<MetricsReportGeneratorImpl>, MetricsReportGeneratorFactoryError> {
            self.create_using_context(config, None, None, None)
        }

        fn create_using_context(
            &self,
            report_config: &MetricsReportConfig,
            metrics_config_proto: Option<MetricsConfigProto>,
            report_ready_callback: Option<Arc<dyn Fn(MetricsReport) + Send + Sync>>,
            triggers: Option<Vec<Arc<dyn Trigger>>>,
        ) -> Result<Arc<MetricsReportGeneratorImpl>, MetricsReportGeneratorFactoryError> {
            let metrics_config_proto = metrics_config_proto.unwrap_or_default();
            let report_ready_callback = report_ready_callback.unwrap_or_else(|| Arc::new(|_| {}));
            let triggers = triggers.map_or(HashMap::new(), |triggers| {
                triggers.into_hash_map(|trigger| trigger.get_name().to_owned())
            });
            let context = MetricsReportGeneratorFactoryCreateContext {
                config_proto: &metrics_config_proto,
                report_ready_callback: &report_ready_callback,
                triggers: &triggers,
                file_descriptors: get_test_file_descriptors(),
                nodes: get_test_nodes(),
                node_types: get_test_node_types(),
            };
            self.create(report_config, &context).map(|generator| {
                generator.downcast_arc::<MetricsReportGeneratorImpl>().ok().unwrap()
            })
        }

        fn validate_using_empty_context(
            &self,
            config: &MetricsReportConfig,
        ) -> Result<(), MetricsReportGeneratorFactoryError> {
            self.validate_using_context(config, None)
        }

        fn validate_using_context(
            &self,
            config: &MetricsReportConfig,
            trigger_names: Option<Vec<String>>,
        ) -> Result<(), MetricsReportGeneratorFactoryError> {
            let triggers = trigger_names
                .map(|vec| vec.into_iter().map(|name| (name, ())).collect())
                .unwrap_or_default();
            let context = MetricsReportGeneratorFactoryValidateContext {
                trigger_protos: &triggers,
                file_descriptors: get_test_file_descriptors(),
                node_types: get_test_node_types(),
            };
            self.validate(config, &context)
        }
    }

    fn create_factory() -> MetricsReportGeneratorFactoryImpl {
        MetricsReportGeneratorFactoryImpl::new(Arc::new(MockMessageBuilderFactory::new()))
    }

    fn create_config_with_invalid_name() -> MetricsReportConfig {
        MetricsReportConfig::new()
    }

    fn create_config_without_message_builder() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        METRICS_REPORT_CONFIG_NAME.clone_into(&mut config.name);
        config.trigger_names.push(TRIGGER_NAME.to_owned());
        config
    }

    fn create_config_with_invalid_trigger_name() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        METRICS_REPORT_CONFIG_NAME.clone_into(&mut config.name);
        config.trigger_names.push(TRIGGER_NAME.to_owned());
        config.message_builder = protobuf::MessageField::some(ProtoMessageBuilder::new());
        config
    }

    fn create_config_with_empty_trigger_names(report_incomplete: bool) -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        METRICS_REPORT_CONFIG_NAME.clone_into(&mut config.name);
        config.trigger_names = vec![];
        config.report_incomplete = report_incomplete;
        config.message_builder = protobuf::MessageField::some(ProtoMessageBuilder::new());
        config
    }

    fn create_valid_config() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        METRICS_REPORT_CONFIG_NAME.clone_into(&mut config.name);
        config.report_incomplete = true;
        config.trigger_names.push(TRIGGER_NAME.to_owned());
        config.message_builder = protobuf::MessageField::some(ProtoMessageBuilder::new());
        config
    }

    #[test]
    fn create_invalid_name_fails() {
        let factory = create_factory();
        let config = create_config_with_invalid_name();

        let err = factory.create_using_empty_context(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::NameNotSet),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_no_message_builder_fails() {
        let factory = create_factory();
        let config = create_config_without_message_builder();

        let err = factory.create_using_empty_context(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_invalid_trigger_name_fails() {
        let factory = create_factory();
        let config = create_config_with_invalid_trigger_name();

        let err = factory.create_using_empty_context(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::InvalidTriggerName { trigger_name } if trigger_name == TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_proto_message_builder_factory_returns_error_fails() {
        let mut message_builder_factory = MockMessageBuilderFactory::new();
        message_builder_factory.fail_create();
        let factory = MetricsReportGeneratorFactoryImpl::new(Arc::new(message_builder_factory));
        let config = create_valid_config();
        let trigger = PeriodicTrigger::new(TRIGGER_NAME.to_owned(), Duration::ZERO);

        let err = factory
            .create_using_context(
                &config,
                None,
                None,
                Some(vec![Arc::clone(&trigger) as Arc<dyn Trigger>]),
            )
            .err()
            .unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::InvalidProtoMessageBuilder { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_success() {
        let factory = create_factory();
        let report_config = create_valid_config();
        let metrics_config =
            MetricsConfigProto { uuid: UUID.to_owned(), version: VERSION, ..Default::default() };
        let report_ready_callback = Arc::new(|_| {}) as Arc<dyn Fn(MetricsReport) + Send + Sync>;
        let trigger = PeriodicTrigger::new(TRIGGER_NAME.to_owned(), Duration::ZERO);

        let res = factory
            .create_using_context(
                &report_config,
                Some(metrics_config),
                Some(Arc::clone(&report_ready_callback)),
                Some(vec![Arc::clone(&trigger) as Arc<dyn Trigger>]),
            )
            .unwrap();

        assert_eq!(res.name, METRICS_REPORT_CONFIG_NAME);
        assert_eq!(res.config_uuid, UUID);
        assert_eq!(res.config_version, VERSION);
        assert!(res.report_incomplete);
        assert_eq!(res.triggers.len(), 1);
        assert!(Arc::ptr_eq(&res.report_ready_callback, &report_ready_callback));
        assert!(Arc::ptr_eq(&res.triggers[0], &(trigger as Arc<dyn Trigger>)));
    }

    #[test]
    fn validate_invalid_name_fails() {
        let factory = create_factory();
        let config = create_config_with_invalid_name();

        let err = factory.validate_using_empty_context(&config).unwrap_err();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::NameNotSet),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_no_message_builder_fails() {
        let factory = create_factory();
        let config = create_config_without_message_builder();

        let err = factory.validate_using_empty_context(&config).unwrap_err();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_invalid_trigger_name_fails() {
        let factory = create_factory();
        let config = create_config_with_invalid_trigger_name();

        let err = factory.validate_using_empty_context(&config).unwrap_err();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::InvalidTriggerName { trigger_name } if trigger_name == TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_proto_message_builder_factory_returns_error_fails() {
        let mut message_builder_factory = MockMessageBuilderFactory::new();
        message_builder_factory.fail_validate();
        let factory = MetricsReportGeneratorFactoryImpl::new(Arc::new(message_builder_factory));
        let config = create_valid_config();

        let err = factory
            .validate_using_context(&config, Some(vec![TRIGGER_NAME.to_owned()]))
            .err()
            .unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::InvalidProtoMessageBuilder { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_empty_trigger_names_with_complete_report_fails() {
        let factory = create_factory();
        let config = create_config_with_empty_trigger_names(/*incomplete_report=*/ false);
        let err = factory.validate_using_empty_context(&config).unwrap_err();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::TriggerNameRequired),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_empty_trigger_names_with_incomplete_report_succeeds() {
        let factory = create_factory();
        let config = create_config_with_empty_trigger_names(/*incomplete_report=*/ true);
        factory.validate_using_empty_context(&config).unwrap();
    }

    #[test]
    fn validate_success() {
        let factory = create_factory();
        let config = create_valid_config();

        factory.validate_using_context(&config, Some(vec![TRIGGER_NAME.to_owned()])).unwrap();
    }
}
