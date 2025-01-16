// Copyright 2023 Google LLC

use async_trait::async_trait;
use log::debug;
use protobuf::well_known_types::{any::Any, timestamp::Timestamp};
use protobuf::MessageDyn;
use sdv_telemetry_proto_rust::metrics_configuration::{
    metrics_report::ReportReason, MetricsReport, MetricsReportConfig,
};
use sdv_telemetry_utils::value_holder::ValueHolder;
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::metrics_config_orchestrator::message_builder::{MessageBuilder, MessageBuilderError};
use crate::metrics_config_orchestrator::trigger::TriggerSubscriber;
use crate::metrics_config_orchestrator::Trigger;
use crate::metrics_config_runtime_error::MetricsReportGeneratorError;

#[async_trait]
#[cfg_attr(test, sdv_telemetry_utils_macros::with_super_trait(downcast_rs::DowncastSync))]
pub(crate) trait MetricsReportGenerator: Send + Sync {
    async fn activate(self: Arc<Self>);
    async fn deactivate(&self, reason: ReportReason);
}

#[cfg(test)]
downcast_rs::impl_downcast!(sync MetricsReportGenerator);

pub(crate) struct MetricsReportGeneratorImpl {
    config_activation_timestamp: Timestamp,
    config_uuid: String,
    config_version: u32,
    is_active: AsyncMutex<bool>,
    message_builder: Arc<dyn MessageBuilder>,
    name: String,
    report_incomplete: bool,
    report_number: AtomicI32,
    subscriber: TriggerSubscriber,
    triggers: Vec<Arc<dyn Trigger>>,

    error_callback: Arc<dyn Fn(MetricsReportGeneratorError) + Send + Sync>,
    report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,
}

#[async_trait]
impl MetricsReportGenerator for MetricsReportGeneratorImpl {
    async fn activate(self: Arc<Self>) {
        let mut is_active = self.is_active.lock().await;
        assert!(!*is_active, "Unable to activate metrics report generator: it's already active");

        // Subscribing to triggers one by one. In theory, it should be possible to also spawn
        // subscription tasks in parallel as an optimization.
        for trigger in &self.triggers {
            let self_clone = Arc::clone(&self);
            let trigger_name_clone = trigger.get_name().to_owned();
            Arc::clone(trigger)
                .subscribe(
                    &self.subscriber,
                    Box::new(move |context| {
                        self_clone.on_trigger_fired(&trigger_name_clone, &context)
                    }),
                )
                .await;
        }

        *is_active = true;
    }

    async fn deactivate(&self, reason: ReportReason) {
        let mut is_active = self.is_active.lock().await;
        assert!(*is_active, "Unable to deactivate metrics report generator: it's already inactive");

        // Unsubscribing from triggers one by one. In theory, it should be possible to also
        // spawn activation tasks in parallel as an optimization.
        for trigger in &self.triggers {
            trigger.unsubscribe(&self.subscriber).await;
        }

        if self.report_incomplete {
            let res =
                Arc::clone(&self.message_builder).build(&Arc::new(NodeEvaluationContext::new()));
            self.generate_report(res, reason, None).await;
        }

        *is_active = false;
    }
}

impl MetricsReportGeneratorImpl {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        metrics_report_config: &MetricsReportConfig,
        config_activation_timestamp: Timestamp,
        config_uuid: String,
        config_version: u32,
        message_builder: Arc<dyn MessageBuilder>,
        triggers: Vec<Arc<dyn Trigger>>,
        error_callback: Arc<dyn Fn(MetricsReportGeneratorError) + Send + Sync>,
        report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,
    ) -> Self {
        let subscriber =
            TriggerSubscriber::MetricsReportGenerator { name: metrics_report_config.name.clone() };
        Self {
            config_activation_timestamp,
            config_uuid,
            config_version,
            is_active: false.into(),
            message_builder,
            name: metrics_report_config.name.clone(),
            report_incomplete: metrics_report_config.report_incomplete,
            report_number: 1.into(),
            subscriber,
            triggers,

            error_callback,
            report_ready_callback,
        }
    }

    async fn generate_report(
        &self,
        build_result: ValueHolder<'static, Result<Box<dyn MessageDyn>, MessageBuilderError>>,
        report_reason: ReportReason,
        trigger_name: Option<&str>,
    ) {
        match build_result.into_future().await {
            Ok(report_data) => {
                let mut report = MetricsReport::new();
                report.config_activation_timestamp =
                    Some(self.config_activation_timestamp.clone()).into();
                report.metrics_config_uuid.clone_from(&self.config_uuid);
                report.metrics_config_version = self.config_version;
                report.report_config_name.clone_from(&self.name);
                report.report_data = Some(Any::pack_dyn(report_data.as_ref()).unwrap()).into();
                report.report_number = self.report_number.fetch_add(1, Ordering::AcqRel);
                report.report_timestamp = Some(Timestamp::now()).into();
                report.report_reason = report_reason.into();

                debug!(
                    "Metrics Report {} #{} was generated with reason {:?} on trigger {} for metrics config {}",
                    report.report_config_name,
                    report.report_number,
                    report_reason,
                    trigger_name.unwrap_or("[none]"),
                    report.metrics_config_uuid,
                );

                (self.report_ready_callback)(report);
            }
            Err(message_builder_error) => {
                (self.error_callback)(MetricsReportGeneratorError {
                    metrics_config_uuid: self.config_uuid.clone(),
                    metrics_report_name: self.name.clone(),
                    message_builder_error,
                });
            }
        }
    }

    fn on_trigger_fired(
        self: &Arc<Self>,
        trigger_name: &str,
        context: &Arc<NodeEvaluationContext>,
    ) {
        let res = Arc::clone(&self.message_builder).build(context);
        let self_clone = Arc::clone(self);
        let trigger_name_clone = trigger_name.to_owned();
        tokio::spawn(async move {
            self_clone.generate_report(res, ReportReason::REGULAR, Some(&trigger_name_clone)).await;
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use futures::FutureExt;
    use protobuf::MessageDyn;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::oneshot_callback;
    use std::{future, sync::atomic::AtomicU32, thread, time::Duration};
    use tokio::sync::mpsc;

    use crate::metrics_config_orchestrator::message_builder::{
        mock::MockMessageBuilder, MessageBuilderError,
    };
    use crate::metrics_config_orchestrator::trigger::mock::MockTrigger;
    use crate::metrics_config_orchestrator::NodeEvaluationError;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const CONFIG_VERSION: u32 = 42;
    const REPORT_CONFIG_NAME: &str = "name";

    fn create_report_config_proto() -> MetricsReportConfig {
        let mut config = MetricsReportConfig::new();
        REPORT_CONFIG_NAME.clone_into(&mut config.name);
        config.report_incomplete = true;
        config
    }

    fn create_default_generator() -> MetricsReportGeneratorImpl {
        create_generator(Arc::new(MockMessageBuilder::new()))
    }

    fn create_generator(message_builder: Arc<MockMessageBuilder>) -> MetricsReportGeneratorImpl {
        MetricsReportGeneratorImpl::new(
            &create_report_config_proto(),
            Timestamp::now(),
            CONFIG_UUID.to_owned(),
            CONFIG_VERSION,
            message_builder,
            vec![],
            Arc::new(|err| {
                panic!("Unexpected error: {err:?}");
            }),
            Arc::new(|_| {}),
        )
    }

    fn create_trigger() -> MockTrigger {
        MockTrigger::new_for_subscriber(
            String::default(),
            TriggerSubscriber::MetricsReportGenerator { name: REPORT_CONFIG_NAME.to_owned() },
        )
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn activate_already_active_panics() {
        let generator = Arc::new(create_default_generator());

        Arc::clone(&generator).activate().await;
        Arc::clone(&generator).activate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscribes_to_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = Arc::clone(&num_fired);
        trigger.subscribe_callback.replace(Box::new(move |_| {
            num_fired_clone.fetch_add(1, Ordering::AcqRel);
        }));

        let mut generator = create_default_generator();
        generator.triggers.push(Arc::new(trigger));

        Arc::new(generator).activate().await;

        assert_eq!(num_fired.load(Ordering::Acquire), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn on_trigger_fired_creates_reports() {
        let trigger = Arc::new(create_trigger());

        let message_builder = Arc::new(MockMessageBuilder::new());
        let mut generator = create_generator(Arc::clone(&message_builder));
        let (tx, mut rx) = mpsc::channel(2);
        generator.report_ready_callback = Arc::new(move |report| tx.try_send(report).unwrap());
        generator.triggers.push(Arc::<MockTrigger>::clone(&trigger));
        let generator = Arc::new(generator);

        // Activate the generator.
        Arc::clone(&generator).activate().await;

        // Fire twice to make sure report number is incremented.
        trigger.fire().await;
        trigger.fire().await;

        // Setup generator to return an error to make sure no reports can be generated past this point.
        message_builder.set_next_error(MessageBuilderError::FailedToAssignValueToField {
            field_name: String::default(),
            e: NodeEvaluationError::ConstantValueNotSet,
        });

        // Wait for reports to be delivered.
        rx.recv().await.unwrap();
        let report = rx.recv().await.unwrap();

        // Verify the report.
        assert_eq!(
            report.config_activation_timestamp.unwrap(),
            generator.config_activation_timestamp
        );
        assert_eq!(report.metrics_config_uuid, CONFIG_UUID);
        assert_eq!(report.metrics_config_version, CONFIG_VERSION);
        assert_eq!(report.report_config_name, REPORT_CONFIG_NAME);
        assert_eq!(report.report_number, 2);
        assert!(
            // Less than 1 hour old.
            Timestamp::now().seconds - report.report_timestamp.as_ref().unwrap().seconds < 60 * 60
        );
        assert!(matches!(report.report_reason.unwrap(), ReportReason::REGULAR));

        // as built by MockMessageBuilder
        let message = report.report_data.unpack::<TestMessage>().unwrap().unwrap();
        assert_eq!(message.int_field, 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn on_trigger_fired_creates_reports_with_trigger_context() {
        let trigger = Arc::new(create_trigger());

        let message_builder = MockMessageBuilder::new();
        message_builder.set_result_callback(|context| {
            Ok(Box::new(TestMessage {
                repeated_i64s: vec![i64::try_from(context.root_id()).unwrap()],
                ..Default::default()
            }) as Box<dyn MessageDyn>)
            .into()
        });
        let mut generator = create_generator(Arc::new(message_builder));
        let (tx, rx) = oneshot_callback();
        generator.report_ready_callback = Arc::new(tx);
        generator.triggers.push(Arc::<MockTrigger>::clone(&trigger));

        // Activate the generator.
        Arc::new(generator).activate().await;

        let context = Arc::new(NodeEvaluationContext::new());
        trigger.fire_with_context(Arc::clone(&context)).await;

        // Wait for the report to be delivered.
        let report = rx.await;

        // Verify the report.
        let test_message = report.report_data.unpack::<TestMessage>().unwrap().unwrap();
        assert_eq!(test_message.repeated_i64s, vec![i64::try_from(context.root_id()).unwrap()]);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn on_trigger_fired_creates_reports_from_async_result() {
        let trigger = Arc::new(create_trigger());

        let message_builder = Arc::new(MockMessageBuilder::new());
        message_builder.set_next_async_result(
            future::ready(Ok(
                Box::new(TestMessage { int_field: 2, ..Default::default() }) as Box<dyn MessageDyn>
            ))
            .boxed(),
        );
        let mut generator = create_generator(Arc::clone(&message_builder));
        let (tx, rx) = oneshot_callback();
        generator.report_ready_callback = Arc::new(tx);
        generator.triggers.push(Arc::<MockTrigger>::clone(&trigger));

        Arc::new(generator).activate().await;
        trigger.fire().await;
        let report = rx.await;

        let message = report.report_data.unpack::<TestMessage>().unwrap().unwrap();
        assert_eq!(message.int_field, 2);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn report_ready_callback_doesnt_block_trigger_fire() {
        let trigger = Arc::new(create_trigger());

        let mut generator = create_default_generator();
        generator.report_ready_callback = Arc::new(|_| thread::sleep(Duration::MAX));
        generator.triggers.push(Arc::<MockTrigger>::clone(&trigger));
        Arc::new(generator).activate().await;

        trigger.fire().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_already_inactive_panics() {
        let generator = Arc::new(create_default_generator());

        generator.deactivate(ReportReason::INCOMPLETE_FINISHED).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_unsubscribes_from_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = Arc::clone(&num_fired);
        trigger.set_unsubscribe_callback(move |_| {
            num_fired_clone.fetch_add(1, Ordering::AcqRel);
        });

        let mut generator = create_default_generator();
        generator.triggers.push(Arc::new(trigger));
        let generator = Arc::new(generator);

        Arc::clone(&generator).activate().await;
        generator.deactivate(ReportReason::INCOMPLETE_FINISHED).await;

        assert_eq!(num_fired.load(Ordering::Acquire), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_generates_incomplete_report() {
        let mut generator = create_default_generator();
        generator.report_incomplete = true;
        let (tx, rx) = oneshot_callback();
        generator.report_ready_callback = Arc::new(tx);
        let generator = Arc::new(generator);

        Arc::clone(&generator).activate().await;
        generator.deactivate(ReportReason::INCOMPLETE_FINISHED).await;
        let report = rx.await;

        assert_eq!(report.report_reason, ReportReason::INCOMPLETE_FINISHED.into());
        let message = report.report_data.unpack::<TestMessage>().unwrap().unwrap();
        assert_eq!(message.int_field, 1);
    }
}
