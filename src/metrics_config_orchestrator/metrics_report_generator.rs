// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use log::error;
use protobuf::well_known_types::{any::Any, timestamp::Timestamp};
use sdvtelemetry_proto_rust::metrics_configuration::{metrics_report::ReportReason, MetricsReport};
use std::{
    marker::{Send, Sync},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tokio::{sync::Mutex as AsyncMutex, task};

use crate::metrics_config_orchestrator::{
    message_builder::{MessageBuilder, MessageBuilderError},
    trigger::{TriggerSubscriber, TriggerUnsubscribeError},
    Trigger,
};

#[async_trait]
pub(crate) trait MetricsReportGenerator: Send + Sync {
    fn get_name(&self) -> &str;

    async fn activate(self: &Arc<Self>) -> Result<(), MetricsReportGeneratorError>;
    async fn deactivate(&self) -> Result<(), MetricsReportGeneratorError>;
}

#[derive(Error, Debug)]
pub(crate) enum MetricsReportGeneratorError {
    #[error("Unable to activate metrics report generator: it's already active")]
    AlreadyActive,
    #[error("Unable to deactivate metrics report generator: it's already inactive")]
    AlreadyInactive,
    #[error("Unable to deactivate metrics report generator due to unsubscription errors: {unsubscribe_errors:?}")]
    UnableToUnsubscribe { unsubscribe_errors: Vec<TriggerUnsubscribeError> },
    #[error("Unable to build report due to error in MessageBuilder: {e}")]
    FailedToBuildReport { e: MessageBuilderError },
}

#[allow(dead_code)]
pub(crate) struct MetricsReportGeneratorImpl {
    pub(super) name: String,

    pub(super) config_uuid: String,
    pub(super) config_version: u32,
    pub(super) report_incomplete: bool,
    pub(super) report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,
    pub(super) triggers: Vec<Arc<dyn Trigger>>,

    is_active: AsyncMutex<bool>,
    report_number: AtomicU32,
    subscriber: TriggerSubscriber,
    message_builder: Box<dyn MessageBuilder>,
}

#[async_trait]
impl MetricsReportGenerator for MetricsReportGeneratorImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn activate(self: &Arc<Self>) -> Result<(), MetricsReportGeneratorError> {
        let mut is_active = self.is_active.lock().await;

        if *is_active {
            return Err(MetricsReportGeneratorError::AlreadyActive);
        }

        // Subscribing to triggers one by one. In theory, it should be possible to also spawn
        // subscription tasks in parallel as an optimization.
        for trigger in &self.triggers {
            let self_clone = self.clone();
            if let Err(err) = trigger
                .clone()
                .subscribe(&self.subscriber, Box::new(move || self_clone.on_trigger_fired()))
                .await
            {
                error!(
                    "Metrics Report Generator {} failed to subscribe to trigger {} with error {}.",
                    self.name,
                    trigger.get_name(),
                    err
                );
            }
        }

        *is_active = true;
        Ok(())
    }

    async fn deactivate(&self) -> Result<(), MetricsReportGeneratorError> {
        let mut is_active = self.is_active.lock().await;

        if !*is_active {
            return Err(MetricsReportGeneratorError::AlreadyInactive);
        }

        let mut unsubscribe_errors = vec![];

        // Unsubscribing from triggers one by one. In theory, it should be possible to also
        // spawn activation tasks in parallel as an optimization.
        for trigger in &self.triggers {
            if let Err(err) = trigger.unsubscribe(&self.subscriber).await {
                unsubscribe_errors.push(err);
            }
        }

        if !unsubscribe_errors.is_empty() {
            return Err(MetricsReportGeneratorError::UnableToUnsubscribe { unsubscribe_errors });
        }

        *is_active = false;
        Ok(())
    }
}

impl MetricsReportGeneratorImpl {
    pub(super) fn new(
        name: String,
        config_uuid: String,
        config_version: u32,
        report_incomplete: bool,
        report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,
        triggers: Vec<Arc<dyn Trigger>>,
        message_builder: Box<dyn MessageBuilder>,
    ) -> Self {
        let subscriber = TriggerSubscriber::MetricsReportGenerator { name: name.clone() };
        MetricsReportGeneratorImpl {
            name,
            config_uuid,
            config_version,
            report_incomplete,
            report_ready_callback,
            triggers,
            is_active: false.into(),
            report_number: 1.into(),
            subscriber,
            message_builder,
        }
    }

    fn on_trigger_fired(self: &Arc<Self>) {
        // Send report in a non-blocking task since it's handled outside of orchestrator
        // by a function that is not allowed to mutate itself.
        let self_clone = self.clone();
        task::spawn(async move {
            match self_clone.message_builder.build() {
                Ok(report_data) => {
                    let mut report = MetricsReport::new();
                    report.report_number = self_clone.report_number.fetch_add(1, Ordering::SeqCst);
                    report.report_timestamp = Some(Timestamp::now()).into();
                    report.report_reason = ReportReason::REGULAR.into();
                    report.metrics_config_uuid = self_clone.config_uuid.clone();
                    report.metrics_config_version = self_clone.config_version;
                    report.report_data = Some(Any::pack_dyn(report_data.as_ref()).unwrap()).into();

                    (self_clone.report_ready_callback)(report);
                }
                Err(e) => {
                    error!("{:?}", MetricsReportGeneratorError::FailedToBuildReport { e });
                }
            }
        });
    }
}

#[cfg(test)]
mod test {
    use lazy_static::lazy_static;
    use std::{thread, time::Duration};
    use tokio::sync::watch;

    use super::*;
    use crate::metrics_config_orchestrator::{
        message_builder::mock::MockMessageBuilder, trigger::mock::MockTrigger,
    };
    use sdv_telem_test_proto::test_message::TestMessage;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const CONFIG_VERSION: u32 = 42;
    const REPORT_CONFIG_NAME: &str = "name";

    lazy_static! {
        static ref NOOP_REPORT_READY_CALLBACK: Arc<dyn Fn(MetricsReport) + Send + Sync> =
            Arc::new(|_| {});
        static ref MOCK_MESSAGE_BUILDER: MockMessageBuilder = MockMessageBuilder::new();
    }

    fn create_generator() -> MetricsReportGeneratorImpl {
        MetricsReportGeneratorImpl::new(
            REPORT_CONFIG_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            CONFIG_VERSION,
            true,
            NOOP_REPORT_READY_CALLBACK.clone(),
            vec![],
            Box::new(MOCK_MESSAGE_BUILDER.clone()),
        )
    }

    fn create_trigger() -> MockTrigger {
        MockTrigger::new_for_subscriber(
            String::default(),
            TriggerSubscriber::MetricsReportGenerator { name: REPORT_CONFIG_NAME.to_owned() },
        )
    }

    #[tokio::test]
    async fn activate_already_active_fails() {
        let generator = Arc::new(create_generator());

        generator.activate().await.unwrap();
        let err = generator.activate().await.err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorError::AlreadyActive),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn activate_subscribes_to_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = num_fired.clone();
        trigger.subscribe_callback.replace(Box::new(move |_| {
            num_fired_clone.fetch_add(1, Ordering::SeqCst);
            None
        }));

        let mut generator = create_generator();
        generator.triggers.push(Arc::new(trigger));

        Arc::new(generator).activate().await.unwrap();

        assert_eq!(num_fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn on_trigger_fired_creates_reports() {
        let trigger = Arc::new(create_trigger());

        let mut generator = create_generator();
        let (tx, mut rx) = watch::channel(None);
        generator.report_ready_callback = Arc::new(move |report| tx.send(Some(report)).unwrap());
        generator.triggers.push(trigger.clone());
        Arc::new(generator).activate().await.unwrap();

        // Fire twice to make sure report number is incremented.
        trigger.fire().await;
        rx.changed().await.unwrap();

        trigger.fire().await;
        rx.changed().await.unwrap();

        let report_container = rx.borrow();
        let report = report_container.as_ref().unwrap();
        assert_eq!(report.report_number, 2);
        // Less than 1 hour old.
        assert!(
            Timestamp::now().seconds - report.report_timestamp.as_ref().unwrap().seconds < 60 * 60
        );
        assert!(matches!(report.report_reason.unwrap(), ReportReason::REGULAR));
        assert_eq!(report.metrics_config_uuid, CONFIG_UUID);
        assert_eq!(report.metrics_config_version, CONFIG_VERSION);
        // as built by MockMessageBuilder
        assert!(report.report_data.is::<TestMessage>());
    }

    #[tokio::test]
    async fn report_ready_callback_doesnt_block_trigger_fire() {
        let trigger = Arc::new(create_trigger());

        let mut generator = create_generator();
        generator.report_ready_callback = Arc::new(|_| thread::sleep(Duration::MAX));
        generator.triggers.push(trigger.clone());
        Arc::new(generator).activate().await.unwrap();

        trigger.fire().await;
    }

    #[tokio::test]
    async fn deactivate_already_inactive_fails() {
        let generator = Arc::new(create_generator());

        let err = generator.deactivate().await.err().unwrap();

        assert!(
            matches!(&err, MetricsReportGeneratorError::AlreadyInactive),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn deactivate_unsubscribes_from_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = num_fired.clone();
        trigger.set_unsubscribe_callback(move |_| {
            num_fired_clone.fetch_add(1, Ordering::SeqCst);
            None
        });

        let mut generator = create_generator();
        generator.triggers.push(Arc::new(trigger));
        let generator = Arc::new(generator);

        generator.activate().await.unwrap();
        generator.deactivate().await.unwrap();

        assert_eq!(num_fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deactivate_unsubscribe_returns_error_fails() {
        const SUBSCRIBER_NAME: &str = "subscriber";

        let mut trigger = create_trigger();
        trigger.set_unsubscribe_callback(move |_| {
            Some(TriggerUnsubscribeError::NotSubscribed {
                subscriber_name: SUBSCRIBER_NAME.to_owned(),
            })
        });

        let mut generator = create_generator();
        generator.triggers.push(Arc::new(trigger));
        let generator = Arc::new(generator);

        generator.activate().await.unwrap();
        let err = generator.deactivate().await.err().unwrap();

        if let MetricsReportGeneratorError::UnableToUnsubscribe { unsubscribe_errors } = &err {
            assert_eq!(unsubscribe_errors.len(), 1);
            assert!(
                matches!(&unsubscribe_errors[0], TriggerUnsubscribeError::NotSubscribed { subscriber_name } if subscriber_name == SUBSCRIBER_NAME),
                "Unexpected inner error: {:?}",
                unsubscribe_errors[0]
            );
        } else {
            panic!("Unexpected error: {:?}", err);
        }
    }
}
