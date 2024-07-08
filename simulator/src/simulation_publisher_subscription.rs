// Copyright 2024 Google LLC

use binder::{Interface, Result as BinderResult, Strong, Status, ExceptionCode};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ISubscriber::ISubscriber, IConfigurablePublisherSubscription::IConfigurablePublisherSubscription};
use log::debug;
use std::sync::Mutex;
use tokio::task::AbortHandle;
use protobuf::text_format::print_to_string_pretty;

use crate::publisher_config::ParsedDataEntry;

pub struct SimulationPublisherSubscription {
    subscriber: Strong<dyn ISubscriber>,
    data_publishing_handle: Mutex<Option<AbortHandle>>,
}

impl Interface for SimulationPublisherSubscription {}

impl SimulationPublisherSubscription {
    pub fn new(subscriber: Strong<dyn ISubscriber>) -> Self {
        Self { subscriber, data_publishing_handle: None.into() }
    }

    pub fn publish_data(&mut self, data: Vec<ParsedDataEntry>, loop_data: bool) {
        let subscriber_clone = self.subscriber.clone();
        self.data_publishing_handle = Some(
            tokio::spawn(async move {
                publish_data(data, subscriber_clone, loop_data).await;
            })
            .abort_handle(),
        )
        .into();
    }
}

async fn publish_data(
    data: Vec<ParsedDataEntry>,
    subscriber: Strong<dyn ISubscriber>,
    loop_data: bool,
) {
    if loop_data {
        loop {
            publish_data_once(&data, &subscriber).await;
        }
    } else {
        publish_data_once(&data, &subscriber).await;
    }
}

async fn publish_data_once(data: &Vec<ParsedDataEntry>, subscriber: &Strong<dyn ISubscriber>) {
    for data_entry in data {
        debug!("Publishing message: {}", print_to_string_pretty(data_entry.message.as_ref()));
        let _ = subscriber.sendData(&data_entry.message_bytes);
        tokio::time::sleep(data_entry.delay).await;
    }
}

impl IConfigurablePublisherSubscription for SimulationPublisherSubscription {
    fn unsubscribe(&self) -> BinderResult<()> {
        if let Some(handle) = self
            .data_publishing_handle
            .lock()
            .map_err(|_| {
                Status::new_exception_str(
                    ExceptionCode::ILLEGAL_STATE,
                    Some("Poisoned \"abort handle\" mutex guard"),
                )
            })?
            .take()
        {
            if !handle.is_finished() {
                handle.abort();
            }
        }
        Ok(())
    }
}
