// Copyright 2023 Google LLC

use protobuf::MessageDyn;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::sync::{Arc, OnceLock};

use crate::expr::node_error::MessageProviderError;
use crate::expr::node_factory::MessageProvider;
use crate::metrics_config_orchestrator::MetricsConfig;

pub(super) struct MessageProviderImpl {
    metrics_config: OnceLock<Arc<MetricsConfig>>,
}

impl MessageProvider for MessageProviderImpl {
    fn get_message(
        &self,
        publisher_name: &str,
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, MessageProviderError> {
        self.metrics_config
            .get()
            .expect("Metrics Config should be set")
            .publishers
            .get(publisher_name)
            .map_or_else(
                move || Err(MessageProviderError::InvalidPublisherName).into(),
                move |publisher| Arc::clone(publisher).get_latest_message().err_into(),
            )
    }
}

impl MessageProviderImpl {
    pub(super) fn new() -> Self {
        Self { metrics_config: OnceLock::new() }
    }

    pub(super) fn set_metrics_config(
        &self,
        metrics_config: &Arc<MetricsConfig>,
    ) -> Result<(), Arc<MetricsConfig>> {
        self.metrics_config.set(Arc::clone(metrics_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageDyn;
    use std::collections::HashMap;

    use crate::metrics_config_orchestrator::publisher::mock::MockPublisher;
    use crate::metrics_config_orchestrator::MetricsConfigState;
    use crate::service_publisher_accessor::GetLatestMessageError;

    const ERROR_MESSAGE: &str = "error message";
    const PUBLISHER_NAME: &str = "publisher";

    fn create_message_provider(metrics_config: MetricsConfig) -> MessageProviderImpl {
        MessageProviderImpl { metrics_config: Arc::new(metrics_config).into() }
    }

    fn create_metrics_config() -> MetricsConfig {
        MetricsConfig {
            uuid: String::default(),
            state: MetricsConfigState::NotStarted.into(),
            metrics_report_generators: vec![],
            ordered_publisher_names: vec![],
            publishers: HashMap::new(),
            start_trigger: None,
            end_trigger: None,
            finish_trigger: None,
        }
    }

    #[test]
    fn get_message_publisher_returns_message() {
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() }) as Arc<dyn MessageDyn>;
        let mut publisher = MockPublisher::new(PUBLISHER_NAME.to_owned());
        publisher.latest_message.replace(Arc::clone(&msg));
        let mut metrics_config = create_metrics_config();
        metrics_config.publishers.insert(PUBLISHER_NAME.to_owned(), Arc::new(publisher));
        let message_provider = create_message_provider(metrics_config);

        let res = message_provider.get_message(PUBLISHER_NAME).unwrap_sync().unwrap().unwrap();

        assert!(Arc::ptr_eq(&res, &msg), "Expected {res:?} to equal {msg:?}");
    }

    #[test]
    fn get_message_publisher_returns_none() {
        let publisher = MockPublisher::new(PUBLISHER_NAME.to_owned());
        let mut metrics_config = create_metrics_config();
        metrics_config.publishers.insert(PUBLISHER_NAME.to_owned(), Arc::new(publisher));
        let message_provider = create_message_provider(metrics_config);

        let res = message_provider.get_message(PUBLISHER_NAME).unwrap_sync().unwrap();

        assert!(res.is_none());
    }

    #[test]
    fn get_message_publisher_not_found_returns_error() {
        let message_provider = create_message_provider(create_metrics_config());
        let err = message_provider.get_message(PUBLISHER_NAME).unwrap_sync().err().unwrap();

        assert!(
            matches!(&err, MessageProviderError::InvalidPublisherName),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn get_message_propagates_publisher_error() {
        let publisher = MockPublisher::new(PUBLISHER_NAME.to_owned());
        publisher.set_latest_message_error(GetLatestMessageError::MessageParsingError(anyhow!(
            ERROR_MESSAGE
        )));
        let mut metrics_config = create_metrics_config();
        metrics_config.publishers.insert(PUBLISHER_NAME.to_owned(), Arc::new(publisher));
        let message_provider = create_message_provider(metrics_config);

        let err = message_provider.get_message(PUBLISHER_NAME).unwrap_sync().err().unwrap();

        assert!(
            matches!(&err, MessageProviderError::MessageRetrievalError { e } if e.to_string() == format!("failed to parse message: {ERROR_MESSAGE}")),
            "Unexpected error: {err:?}"
        );
    }
}
