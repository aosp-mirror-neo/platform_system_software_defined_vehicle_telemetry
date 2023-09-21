// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::FileDescriptor;
use sdv_ast::Node;
use sdvtelemetry_proto_rust::metrics_configuration::{
    MetricsConfig as MetricsConfigProto, MetricsReport, MetricsReportConfig,
    Trigger as TriggerProto,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use thiserror::Error;

use crate::metrics_config_orchestrator::{
    message_builder_factory::{
        MessageBuilderFactory, MessageBuilderFactoryContext, MessageBuilderFactoryError,
    },
    MetricsReportGenerator, MetricsReportGeneratorImpl, Trigger, TriggerImpl,
};

pub(crate) trait MetricsReportGeneratorFactory<
    TMetricsReportGenerator: MetricsReportGenerator = MetricsReportGeneratorImpl,
    TTrigger: Trigger = TriggerImpl,
>
{
    /// Returns a new metrics report generator based on the metrics report config
    /// protobuf message.
    fn create(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryCreateContext<TTrigger>,
    ) -> Result<Arc<TMetricsReportGenerator>, MetricsReportGeneratorFactoryError>;

    /// Verifies that the metrics report config protobuf message is valid and can be used
    /// to create a metrics report generator.
    fn validate(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryValidateContext,
    ) -> Result<(), MetricsReportGeneratorFactoryError>;
}

pub(crate) struct MetricsReportGeneratorFactoryCreateContext<'a, TTrigger: Trigger = TriggerImpl> {
    pub config_proto: &'a MetricsConfigProto,
    pub report_ready_callback: &'a Arc<dyn Fn(MetricsReport) + Send + Sync>,
    pub triggers: &'a HashMap<String, Arc<TTrigger>>,
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
}

pub(crate) struct MetricsReportGeneratorFactoryValidateContext<'a> {
    pub trigger_protos: &'a Vec<TriggerProto>,
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsReportGeneratorFactoryError {
    #[error("Trigger name {trigger_name} is invalid")]
    InvalidTriggerName { trigger_name: String },
    #[error("MetricsReportConfig contains invalid ProtoMessageBuilder: {e}")]
    InvalidProtoMessageBuilder {
        #[from]
        e: MessageBuilderFactoryError,
    },
    #[error("MetricsReportConfig must specify the ProtoMessageBuilder")]
    ProtoMessageBuilderNotSet,
}

pub(crate) struct MetricsReportGeneratorFactoryImpl {
    pub(super) message_builder_factory: Box<dyn MessageBuilderFactory>,
}

impl MetricsReportGeneratorFactoryImpl {
    pub fn new(message_builder_factory: Box<dyn MessageBuilderFactory>) -> Self {
        MetricsReportGeneratorFactoryImpl { message_builder_factory }
    }
}

impl MetricsReportGeneratorFactory for MetricsReportGeneratorFactoryImpl {
    fn create(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryCreateContext,
    ) -> Result<Arc<MetricsReportGeneratorImpl>, MetricsReportGeneratorFactoryError> {
        let message_builder = self.message_builder_factory.create(
            metrics_report_config
                .message_builder
                .as_ref()
                .ok_or(MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet)?,
            &MessageBuilderFactoryContext::new(context.file_descriptors, context.nodes),
        )?;
        let triggers = metrics_report_config
            .trigger_names
            .iter()
            .map(|name| {
                context
                    .triggers
                    .get(name)
                    .map(|trigger| trigger.clone() as Arc<dyn Trigger + Send + Sync>)
                    .ok_or_else(|| MetricsReportGeneratorFactoryError::InvalidTriggerName {
                        trigger_name: name.clone(),
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Arc::new(MetricsReportGeneratorImpl::new(
            metrics_report_config.name.clone(),
            context.config_proto.uuid.clone(),
            context.config_proto.version,
            metrics_report_config.report_incomplete,
            context.report_ready_callback.clone(),
            triggers,
            message_builder,
        )))
    }

    fn validate(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryValidateContext,
    ) -> Result<(), MetricsReportGeneratorFactoryError> {
        // validate protobuf message builder is valid
        self.message_builder_factory.validate(
            metrics_report_config
                .message_builder
                .as_ref()
                .ok_or(MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet)?,
            &MessageBuilderFactoryContext::new(context.file_descriptors, context.nodes),
        )?;

        // validate trigger names for this report config exist
        let names_set =
            context.trigger_protos.iter().map(|proto| &proto.name).collect::<HashSet<_>>();
        metrics_report_config.trigger_names.iter().try_for_each(|name| {
            if names_set.contains(&name) {
                Ok(())
            } else {
                Err(MetricsReportGeneratorFactoryError::InvalidTriggerName {
                    trigger_name: name.clone(),
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use sdvtelemetry_proto_rust::metrics_configuration::ProtoMessageBuilder;

    use super::*;
    use crate::collection_extension::VecExtension;
    use crate::metrics_config_orchestrator::message_builder_factory::mock::{
        get_test_file_descriptors, get_test_nodes, MockMessageBuilderFactory,
    };
    use crate::metrics_config_orchestrator::trigger::TriggerType;
    use crate::test_utils::ptr_eq;

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
            triggers: Option<Vec<Arc<TriggerImpl>>>,
        ) -> Result<Arc<MetricsReportGeneratorImpl>, MetricsReportGeneratorFactoryError> {
            let metrics_config_proto = metrics_config_proto.unwrap_or_else(MetricsConfigProto::new);
            let report_ready_callback = report_ready_callback.unwrap_or_else(|| Arc::new(|_| {}));
            let triggers = triggers
                .map_or(HashMap::new(), |val| val.into_hash_map(|trigger| trigger.name.clone()));
            let context = MetricsReportGeneratorFactoryCreateContext {
                config_proto: &metrics_config_proto,
                report_ready_callback: &report_ready_callback,
                triggers: &triggers,
                file_descriptors: get_test_file_descriptors(),
                nodes: get_test_nodes(),
            };
            self.create(report_config, &context)
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
                .map(|vec| {
                    vec.into_iter()
                        .map(|name| {
                            let mut trigger = TriggerProto::new();
                            trigger.name = name;
                            trigger
                        })
                        .collect()
                })
                .unwrap_or_default();
            let context = MetricsReportGeneratorFactoryValidateContext {
                trigger_protos: &triggers,
                file_descriptors: get_test_file_descriptors(),
                nodes: get_test_nodes(),
            };
            self.validate(config, &context)
        }
    }

    fn create_factory() -> MetricsReportGeneratorFactoryImpl {
        MetricsReportGeneratorFactoryImpl::new(Box::new(MockMessageBuilderFactory::new()))
    }

    fn create_config_with_invalid_trigger_name() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        config.trigger_names.push(TRIGGER_NAME.to_owned());
        config.message_builder = protobuf::MessageField::some(ProtoMessageBuilder::new());
        config
    }

    fn create_valid_config() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        config.name = METRICS_REPORT_CONFIG_NAME.to_owned();
        config.report_incomplete = true;
        config.trigger_names.push(TRIGGER_NAME.to_owned());
        config.message_builder = protobuf::MessageField::some(ProtoMessageBuilder::new());
        config
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
    fn create_config_has_no_proto_message_builder_fails() {
        let factory = create_factory();
        let config = MetricsReportConfig::new();

        let err = factory.create_using_empty_context(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_proto_message_builder_factory_returns_error_fails() {
        let mut message_builder_factory = MockMessageBuilderFactory::new();
        message_builder_factory.fail_create();
        let factory = MetricsReportGeneratorFactoryImpl::new(Box::new(message_builder_factory));
        let config = create_valid_config();
        let trigger = Arc::new(TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: 0 },
        ));

        let err = factory
            .create_using_context(&config, None, None, Some(vec![trigger.clone()]))
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
        let mut metrics_config = MetricsConfigProto::new();
        metrics_config.uuid = UUID.to_owned();
        metrics_config.version = VERSION;
        let report_ready_callback = Arc::new(|_| {}) as Arc<dyn Fn(MetricsReport) + Send + Sync>;
        let trigger = Arc::new(TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: 0 },
        ));

        let res = factory
            .create_using_context(
                &report_config,
                Some(metrics_config),
                Some(report_ready_callback.clone()),
                Some(vec![trigger.clone()]),
            )
            .unwrap();

        assert_eq!(res.name, METRICS_REPORT_CONFIG_NAME);
        assert_eq!(res.config_uuid, UUID);
        assert_eq!(res.config_version, VERSION);
        assert!(res.report_incomplete);
        assert_eq!(res.triggers.len(), 1);
        assert!(ptr_eq(&res.report_ready_callback, &report_ready_callback));
        assert!(ptr_eq(&res.triggers[0], &(trigger as Arc<dyn Trigger + Send + Sync>)));
    }

    #[test]
    fn validate_invalid_trigger_name_fails() {
        let factory = create_factory();
        let config = create_config_with_invalid_trigger_name();

        let err = factory.validate_using_empty_context(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::InvalidTriggerName { trigger_name } if trigger_name == TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_config_has_no_proto_message_builder_fails() {
        let factory = create_factory();
        let config = MetricsReportConfig::new();

        let err = factory.validate_using_empty_context(&config).err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorFactoryError::ProtoMessageBuilderNotSet),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_proto_message_builder_factory_returns_error_fails() {
        let mut message_builder_factory = MockMessageBuilderFactory::new();
        message_builder_factory.fail_validate();
        let factory = MetricsReportGeneratorFactoryImpl::new(Box::new(message_builder_factory));
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
    fn validate_success() {
        let factory = create_factory();
        let config = create_valid_config();

        factory.validate_using_context(&config, Some(vec![TRIGGER_NAME.to_owned()])).unwrap();
    }
}
