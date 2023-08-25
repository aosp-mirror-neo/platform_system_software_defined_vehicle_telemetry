// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::metrics_configuration::{
    MetricsReportConfig, Trigger as TriggerProto,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use thiserror::Error;

use crate::metrics_config_orchestrator::{MetricsReportGenerator, Trigger, TriggerImpl};

pub(crate) trait MetricsReportGeneratorFactory<TTrigger: Trigger = TriggerImpl> {
    /// Returns a new metrics report generator based on the metrics report config
    /// protobuf message.
    fn create(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryCreateContext<TTrigger>,
    ) -> Result<MetricsReportGenerator<TTrigger>, MetricsReportGeneratorFactoryError>;

    /// Verifies that the metrics report config protobuf message is valid and can be used
    /// to create a metrics report generator.
    fn validate(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryValidateContext,
    ) -> Result<(), MetricsReportGeneratorFactoryError>;
}

pub(crate) struct MetricsReportGeneratorFactoryCreateContext<'a, TTrigger: Trigger = TriggerImpl> {
    pub triggers: &'a HashMap<String, Arc<TTrigger>>,
}

pub(crate) struct MetricsReportGeneratorFactoryValidateContext<'a> {
    pub trigger_protos: &'a Vec<TriggerProto>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsReportGeneratorFactoryError {
    #[error("Trigger name {trigger_name} is invalid")]
    InvalidTriggerName { trigger_name: String },
}

pub(crate) struct MetricsReportGeneratorFactoryImpl {}

impl MetricsReportGeneratorFactory for MetricsReportGeneratorFactoryImpl {
    fn create(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryCreateContext,
    ) -> Result<MetricsReportGenerator, MetricsReportGeneratorFactoryError> {
        let triggers = metrics_report_config
            .trigger_names
            .iter()
            .map(|name| {
                context.triggers.get(name).cloned().ok_or_else(|| {
                    MetricsReportGeneratorFactoryError::InvalidTriggerName {
                        trigger_name: name.clone(),
                    }
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(MetricsReportGenerator {
            report_incomplete: metrics_report_config.report_incomplete,
            triggers,
        })
    }

    fn validate(
        &self,
        metrics_report_config: &MetricsReportConfig,
        context: &MetricsReportGeneratorFactoryValidateContext,
    ) -> Result<(), MetricsReportGeneratorFactoryError> {
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
    use super::*;
    use crate::collection_extension::VecExtension;
    use crate::metrics_config_orchestrator::trigger::TriggerType;

    const TRIGGER_NAME: &str = "trigger";

    impl MetricsReportGeneratorFactoryImpl {
        // The following functions create a context with passed data and use it to call `create`
        // or `validate`. This is needed because context structs contain borrows and it's impossible
        // to have a function that creates and returns it - it can only be used in the function
        // where it's created.
        fn create_using_empty_context(
            &self,
            config: &MetricsReportConfig,
        ) -> Result<MetricsReportGenerator, MetricsReportGeneratorFactoryError> {
            self.create_using_context(config, None)
        }

        fn create_using_context(
            &self,
            config: &MetricsReportConfig,
            triggers: Option<Vec<Arc<TriggerImpl>>>,
        ) -> Result<MetricsReportGenerator, MetricsReportGeneratorFactoryError> {
            let triggers = triggers
                .map_or(HashMap::new(), |val| val.into_hash_map(|trigger| trigger.name.clone()));
            let context = MetricsReportGeneratorFactoryCreateContext { triggers: &triggers };
            self.create(config, &context)
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
            let context =
                MetricsReportGeneratorFactoryValidateContext { trigger_protos: &triggers };
            self.validate(config, &context)
        }
    }

    fn create_factory() -> MetricsReportGeneratorFactoryImpl {
        MetricsReportGeneratorFactoryImpl {}
    }

    fn create_config_with_invalid_trigger_name() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        config.trigger_names.push(TRIGGER_NAME.to_owned());
        config
    }

    fn create_valid_config() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        config.report_incomplete = true;
        config.trigger_names.push(TRIGGER_NAME.to_owned());
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
    fn create_success() {
        let factory = create_factory();
        let config = create_valid_config();
        let trigger = Arc::new(TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: 0 },
        ));

        let res = factory.create_using_context(&config, Some(vec![trigger.clone()])).unwrap();

        assert!(res.report_incomplete);
        assert_eq!(res.triggers.len(), 1);
        assert!(Arc::ptr_eq(&res.triggers[0], &trigger));
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
    fn validate_success() {
        let factory = create_factory();
        let config = create_valid_config();

        factory.validate_using_context(&config, Some(vec![TRIGGER_NAME.to_owned()])).unwrap();
    }
}
