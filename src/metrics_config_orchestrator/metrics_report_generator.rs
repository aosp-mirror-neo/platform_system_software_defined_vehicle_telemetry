// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use log::{debug, error};
use protobuf::well_known_types::{any::Any, timestamp::Timestamp};
use sdvtelemetry_proto_rust::metrics_configuration::{
    metrics_report::ReportReason, MetricsReport, MetricsReportConfig,
};
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tokio::{sync::Mutex as AsyncMutex, task};

use crate::metrics_config_orchestrator::message_builder::MessageBuilder;
use crate::metrics_config_orchestrator::trigger::TriggerSubscriber;
use crate::metrics_config_orchestrator::Trigger;

#[async_trait]
pub(crate) trait MetricsReportGenerator: Send + Sync {
    fn get_name(&self) -> &str;

    async fn activate(self: &Arc<Self>);
    async fn deactivate(&self);
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
    report_number: AtomicI32,
    subscriber: TriggerSubscriber,
    message_builder: Box<dyn MessageBuilder>,
}

#[async_trait]
impl MetricsReportGenerator for MetricsReportGeneratorImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn activate(self: &Arc<Self>) {
        let mut is_active = self.is_active.lock().await;
        assert!(!*is_active, "Unable to activate metrics report generator: it's already active");

        // Subscribing to triggers one by one. In theory, it should be possible to also spawn
        // subscription tasks in parallel as an optimization.
        for trigger in &self.triggers {
            let self_clone = self.clone();
            let trigger_name_clone = trigger.get_name().to_owned();
            trigger
                .clone()
                .subscribe(
                    &self.subscriber,
                    Box::new(move || self_clone.on_trigger_fired(&trigger_name_clone)),
                )
                .await;
        }

        *is_active = true;
    }

    async fn deactivate(&self) {
        let mut is_active = self.is_active.lock().await;
        assert!(*is_active, "Unable to deactivate metrics report generator: it's already inactive");

        // Unsubscribing from triggers one by one. In theory, it should be possible to also
        // spawn activation tasks in parallel as an optimization.
        for trigger in &self.triggers {
            trigger.unsubscribe(&self.subscriber).await;
        }

        *is_active = false;
    }
}

impl MetricsReportGeneratorImpl {
    pub(super) fn new(
        metrics_report_config: &MetricsReportConfig,
        config_uuid: String,
        config_version: u32,
        report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,
        triggers: Vec<Arc<dyn Trigger>>,
        message_builder: Box<dyn MessageBuilder>,
    ) -> Self {
        let subscriber =
            TriggerSubscriber::MetricsReportGenerator { name: metrics_report_config.name.clone() };
        Self {
            name: metrics_report_config.name.clone(),
            config_uuid,
            config_version,
            report_incomplete: metrics_report_config.report_incomplete,
            report_ready_callback,
            triggers,
            is_active: false.into(),
            report_number: 1.into(),
            subscriber,
            message_builder,
        }
    }

    fn on_trigger_fired(self: &Arc<Self>, trigger_name: &str) {
        // Send report in a non-blocking task since it's handled outside of orchestrator
        // by a function that is not allowed to mutate itself.
        let self_clone = self.clone();
        let trigger_name = trigger_name.to_owned();
        task::spawn(async move {
            match self_clone.message_builder.build() {
                Ok(report_data) => {
                    let mut report = MetricsReport::new();
                    report.report_number = self_clone.report_number.fetch_add(1, Ordering::SeqCst);
                    report.report_timestamp = Some(Timestamp::now()).into();
                    report.report_reason = ReportReason::REGULAR.into();
                    report.metrics_config_uuid = self_clone.config_uuid.clone();
                    report.metrics_config_version = self_clone.config_version;
                    report.report_config_name = self_clone.name.clone();
                    report.report_data = Some(Any::pack_dyn(report_data.as_ref()).unwrap()).into();

                    debug!(
                        "Metrics Report {} #{} was generated on trigger {} for metrics config {}",
                        report.report_config_name,
                        report.report_number,
                        trigger_name,
                        report.metrics_config_uuid
                    );

                    (self_clone.report_ready_callback)(report);
                }
                Err(e) => {
                    error!("Unable to build report due to error in MessageBuilder: {:?}", e);
                }
            }
        });
    }
}

#[cfg(test)]
mod test {
    use lazy_static::lazy_static;
    use std::{sync::atomic::AtomicU32, thread, time::Duration};
    use tokio::sync::watch;

    use super::*;
    use crate::metrics_config_orchestrator::{
        message_builder::mock::MockMessageBuilder, trigger::mock::MockTrigger,
    };
    use sdv_telemetry_test_proto::test_message::TestMessage;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const CONFIG_VERSION: u32 = 42;
    const REPORT_CONFIG_NAME: &str = "name";

    lazy_static! {
        static ref NOOP_REPORT_READY_CALLBACK: Arc<dyn Fn(MetricsReport) + Send + Sync> =
            Arc::new(|_| {});
        static ref MOCK_MESSAGE_BUILDER: MockMessageBuilder = MockMessageBuilder::new();
    }

    fn create_report_config_proto() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        config.name = REPORT_CONFIG_NAME.to_owned();
        config.report_incomplete = true;
        config
    }

    fn create_generator() -> MetricsReportGeneratorImpl {
        MetricsReportGeneratorImpl::new(
            &create_report_config_proto(),
            CONFIG_UUID.to_owned(),
            CONFIG_VERSION,
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
    #[should_panic]
    async fn activate_already_active_panics() {
        let generator = Arc::new(create_generator());

        generator.activate().await;
        generator.activate().await;
    }

    #[tokio::test]
    async fn activate_subscribes_to_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = num_fired.clone();
        trigger.subscribe_callback.replace(Box::new(move |_| {
            num_fired_clone.fetch_add(1, Ordering::SeqCst);
        }));

        let mut generator = create_generator();
        generator.triggers.push(Arc::new(trigger));

        Arc::new(generator).activate().await;

        assert_eq!(num_fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn on_trigger_fired_creates_reports() {
        let trigger = Arc::new(create_trigger());

        let mut generator = create_generator();
        let (tx, mut rx) = watch::channel(None);
        generator.report_ready_callback = Arc::new(move |report| tx.send(Some(report)).unwrap());
        generator.triggers.push(trigger.clone());
        Arc::new(generator).activate().await;

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
        assert_eq!(report.report_config_name, REPORT_CONFIG_NAME);
        // as built by MockMessageBuilder
        assert!(report.report_data.is::<TestMessage>());
    }

    #[tokio::test]
    async fn report_ready_callback_doesnt_block_trigger_fire() {
        let trigger = Arc::new(create_trigger());

        let mut generator = create_generator();
        generator.report_ready_callback = Arc::new(|_| thread::sleep(Duration::MAX));
        generator.triggers.push(trigger.clone());
        Arc::new(generator).activate().await;

        trigger.fire().await;
    }

    #[tokio::test]
    #[should_panic]
    async fn deactivate_already_inactive_panics() {
        let generator = Arc::new(create_generator());

        generator.deactivate().await;
    }

    #[tokio::test]
    async fn deactivate_unsubscribes_from_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = num_fired.clone();
        trigger.set_unsubscribe_callback(move |_| {
            num_fired_clone.fetch_add(1, Ordering::SeqCst);
        });

        let mut generator = create_generator();
        generator.triggers.push(Arc::new(trigger));
        let generator = Arc::new(generator);

        generator.activate().await;
        generator.deactivate().await;

        assert_eq!(num_fired.load(Ordering::SeqCst), 1);
    }
}
