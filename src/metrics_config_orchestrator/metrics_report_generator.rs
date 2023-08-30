// Copyright 2023 Google LLC. All Rights Reserved.

use log::error;
use protobuf::well_known_types::{any::Any, timestamp::Timestamp, wrappers::Int32Value};
use sdvtelemetry_proto_rust::metrics_configuration::{metrics_report::ReportReason, MetricsReport};
use std::{
    marker::{Send, Sync},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use crate::metrics_config_orchestrator::{trigger::TriggerSubscriber, Trigger};

const REPORT_UUID: &str = "00000000-0000-0000-0000-000000000000";

pub(crate) trait MetricsReportGenerator {
    fn activate(self: &Arc<Self>);
    fn deactivate(&self);
}

#[allow(dead_code)]
pub(crate) struct MetricsReportGeneratorImpl {
    pub(super) name: String,

    pub(super) config_uuid: String,
    pub(super) config_version: u32,
    pub(super) report_incomplete: bool,
    pub(super) report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,
    pub(super) triggers: Vec<Arc<dyn Trigger + Send + Sync>>,

    report_number: AtomicU32,
    subscriber: TriggerSubscriber,
}

impl MetricsReportGeneratorImpl {
    pub(super) fn new(
        name: String,
        config_uuid: String,
        config_version: u32,
        report_incomplete: bool,
        report_ready_callback: Arc<dyn Fn(MetricsReport) + Send + Sync>,
        triggers: Vec<Arc<dyn Trigger + Send + Sync>>,
    ) -> Self {
        let subscriber = TriggerSubscriber::MetricsReportGenerator { name: name.clone() };
        MetricsReportGeneratorImpl {
            name,
            config_uuid,
            config_version,
            report_incomplete,
            report_ready_callback,
            triggers,
            report_number: AtomicU32::new(1),
            subscriber,
        }
    }

    fn on_trigger_fired(self: &Arc<Self>) {
        let mut report = MetricsReport::new();
        report.uuid = REPORT_UUID.to_owned();
        report.report_number = self.report_number.fetch_add(1, Ordering::SeqCst);
        report.report_timestamp = Some(Timestamp::now()).into();
        report.report_reason = ReportReason::REGULAR.into();
        report.metrics_config_uuid = self.config_uuid.clone();
        report.metrics_config_version = self.config_version;
        report.report_data = Some(Any::pack_dyn(&Int32Value::new()).unwrap()).into();
        (self.report_ready_callback)(report);
    }
}

impl MetricsReportGenerator for MetricsReportGeneratorImpl {
    fn activate(self: &Arc<Self>) {
        for trigger in &self.triggers {
            let self_clone = self.clone();
            if let Err(err) =
                trigger.subscribe(&self.subscriber, Box::new(move || self_clone.on_trigger_fired()))
            {
                error!(
                    "Metrics Report Generator {} failed to subscribe to trigger {} with error {}.",
                    self.name,
                    trigger.get_name(),
                    err
                );
            }
        }
    }

    fn deactivate(&self) {
        for trigger in &self.triggers {
            if let Err(err) = trigger.unsubscribe(&self.subscriber) {
                error!(
                    "Metrics Report Generator {} failed to subscribe to trigger {} with error {}.",
                    self.name,
                    trigger.get_name(),
                    err
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use lazy_static::lazy_static;
    use std::sync::Mutex;

    use super::*;
    use crate::metrics_config_orchestrator::trigger::mock::MockTrigger;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const CONFIG_VERSION: u32 = 42;
    const REPORT_CONFIG_NAME: &str = "name";

    lazy_static! {
        static ref NOOP_REPORT_READY_CALLBACK: Arc<dyn Fn(MetricsReport) + Send + Sync> =
            Arc::new(|_| {});
    }

    fn create_generator() -> MetricsReportGeneratorImpl {
        MetricsReportGeneratorImpl::new(
            REPORT_CONFIG_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            CONFIG_VERSION,
            true,
            NOOP_REPORT_READY_CALLBACK.clone(),
            vec![],
        )
    }

    fn create_trigger() -> MockTrigger {
        MockTrigger::new_for_subscriber(
            String::default(),
            TriggerSubscriber::MetricsReportGenerator { name: REPORT_CONFIG_NAME.to_owned() },
        )
    }

    #[test]
    fn activate_subscribes_to_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = num_fired.clone();
        trigger.subscribe_callback.replace(Box::new(move |_| {
            num_fired_clone.fetch_add(1, Ordering::SeqCst);
            None
        }));

        let mut generator = create_generator();
        generator.triggers.push(Arc::new(trigger));

        Arc::new(generator).activate();

        assert_eq!(num_fired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn on_trigger_fired_creates_reports() {
        let trigger = Arc::new(create_trigger());

        let mut generator = create_generator();
        let report_container = Arc::new(Mutex::new(None));
        let report_container_clone = report_container.clone();
        generator.report_ready_callback = Arc::new(move |report| {
            report_container_clone.lock().unwrap().replace(report);
        });
        generator.triggers.push(trigger.clone());
        Arc::new(generator).activate();

        trigger.fire();
        trigger.fire();

        let report_guard = report_container.lock().unwrap();
        let report = report_guard.as_ref().unwrap();
        assert_eq!(report.report_number, 2);
        // Less than 1 hour old.
        assert!(
            Timestamp::now().seconds - report.report_timestamp.as_ref().unwrap().seconds < 60 * 60
        );
        assert!(matches!(report.report_reason.unwrap(), ReportReason::REGULAR));
        assert_eq!(report.metrics_config_uuid, CONFIG_UUID);
        assert_eq!(report.metrics_config_version, CONFIG_VERSION);
        assert!(report.report_data.is::<Int32Value>());
    }

    #[test]
    fn deactivate_unsubscribes_from_trigger() {
        let mut trigger = create_trigger();
        let num_fired = Arc::new(AtomicU32::new(0));
        let num_fired_clone = num_fired.clone();
        trigger.unsubscribe_callback.replace(Box::new(move |_| {
            num_fired_clone.fetch_add(1, Ordering::SeqCst);
            None
        }));

        let mut generator = create_generator();
        generator.triggers.push(Arc::new(trigger));
        let generator = Arc::new(generator);

        generator.activate();
        generator.deactivate();

        assert_eq!(num_fired.load(Ordering::SeqCst), 1);
    }
}
