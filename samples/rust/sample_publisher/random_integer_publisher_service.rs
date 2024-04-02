// Copyright 2023 Google LLC. All Rights Reserved.

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    ISubscriber::ISubscriber, IConfigurablePublisher::IConfigurablePublisher,
};
use google_sdv_telemetry_configurable_publisher::binder::{Interface, Status, Strong, Result as BinderResult};

use crate::random_integer_publisher::RandomIntegerPublisher;

use log::info;
use std::fmt::Debug;
use std::sync::Arc;

/// Convert a Result<T, E> to BinderResult<T> to allow it to be returned from a binder RPC,
/// preserving the content as far as possible.
/// Also log the error if there is one.
pub fn to_binder_result<T, E: Debug>(result: Result<T, E>) -> BinderResult<T> {
    result.map_err(|e| {
        let message = format!("{:?}", e);
        info!("Returning binder error: {}", &message);
        Status::new_service_specific_error_str(-1, Some(message))
    })
}

pub struct RandomIntegerPublisherService {
    pub publisher: Arc<RandomIntegerPublisher>,
}

impl Interface for RandomIntegerPublisherService {}

impl IConfigurablePublisher for RandomIntegerPublisherService {
    fn subscribe(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> BinderResult<()> {
        to_binder_result(self.publisher.subscribe(subscriber, config))
    }

    fn unsubscribe(&self, config: Option<&[u8]>) -> BinderResult<()> {
        to_binder_result(self.publisher.unsubscribe(config))
    }
}
