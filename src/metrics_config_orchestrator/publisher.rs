// Copyright 2023 Google LLC. All Rights Reserved.

pub(super) mod service_adapter;

use log::error;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_ast::Node;
use sdvtelemetry_proto_rust::{
    expression::NewProtoNode, metrics_configuration::declarative_aggregation::AggregationType,
};
use std::{
    marker::{Send, Sync},
    sync::{Arc, Mutex},
};
use thiserror::Error;

use crate::metrics_config_orchestrator::TriggerImpl;
use service_adapter::{ServiceAdapter, ServiceAdapterError, ServiceSubscription};

pub(crate) trait Publisher {
    fn get_name(&self) -> &str;
    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>>;

    fn activate(self: &Arc<Self>) -> Result<(), PublisherError>;
    fn deactivate(&self) -> Result<(), PublisherError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum PublisherError {
    #[error("Unable to activate publisher: it's already active")]
    AlreadyActive,
    #[error("Unable to deactivate publisher: it's already inactive")]
    AlreadyInactive,
    #[error("Activation logic for signal publishers is not implemented")]
    SignalPublishersNotImplemented,
    #[error("Activation logic for aggregation publishers is not implemented")]
    AggregationPublishersNotImplemented,
    #[error("Service adapter failed to process subscription request: {error}")]
    ServiceAdapterError {
        #[from]
        error: ServiceAdapterError,
    },
}

pub(crate) struct PublisherImpl {
    pub(super) name: String,
    pub(super) publisher_type: PublisherType,

    latest_message: Mutex<Option<Box<dyn MessageDyn>>>,
}

#[allow(dead_code)]
pub(crate) enum PublisherType {
    Signal {
        signal_name: String,
    },
    Service {
        service_name: String,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
        service_subscription: Mutex<Option<ServiceSubscription>>,
    },
    Aggregation {
        triggers: Vec<Arc<TriggerImpl>>,
        message_descriptor: MessageDescriptor,
        reset_on_get: bool,
        declarative_aggregations: Vec<DeclarativeAggregation>,
    },
}

#[allow(dead_code)]
pub(crate) struct DeclarativeAggregation {
    pub(super) field_name: String,
    pub(super) selector_expression: Arc<Node>,
    pub(super) aggregation_type: AggregationType,
    // TODO(b/293213911): Create new rust type and remove reference to protobuf message
    pub(super) seed_expression: Option<NewProtoNode>,
    pub(super) aggregation_expression: Option<NewProtoNode>,
}

impl PublisherImpl {
    pub(super) fn new(name: String, publisher_type: PublisherType) -> Self {
        PublisherImpl { name, publisher_type, latest_message: Mutex::new(None) }
    }

    pub(super) fn new_service_publisher(
        name: String,
        service_name: String,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    ) -> Self {
        Self::new(
            name,
            PublisherType::Service {
                service_name,
                service_adapter,
                service_subscription: Mutex::new(None),
            },
        )
    }

    fn on_service_publisher_new_message(self: &Arc<Self>, message: &dyn MessageDyn) {
        *self.latest_message.lock().unwrap() = Some(message.clone_box());
    }
}

impl Publisher for PublisherImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>> {
        self.latest_message.lock().unwrap().clone()
    }

    fn activate(self: &Arc<Self>) -> Result<(), PublisherError> {
        match &self.publisher_type {
            PublisherType::Signal { .. } => {
                return Err(PublisherError::SignalPublishersNotImplemented)
            }
            PublisherType::Service { service_name, service_adapter, service_subscription } => {
                let mut subscription = service_subscription.lock().unwrap();
                if subscription.is_some() {
                    return Err(PublisherError::AlreadyActive);
                }
                let self_clone = self.clone();
                *subscription = Some(service_adapter.subscribe(
                    service_name,
                    Box::new(move |msg| self_clone.on_service_publisher_new_message(msg)),
                ));
            }
            PublisherType::Aggregation { .. } => {
                return Err(PublisherError::AggregationPublishersNotImplemented)
            }
        }

        Ok(())
    }

    fn deactivate(&self) -> Result<(), PublisherError> {
        match &self.publisher_type {
            PublisherType::Signal { .. } => {
                return Err(PublisherError::SignalPublishersNotImplemented)
            }
            PublisherType::Service { service_adapter, service_subscription, .. } => {
                let mut maybe_subscription = service_subscription.lock().unwrap();
                if let Some(subscription) = maybe_subscription.take() {
                    service_adapter.unsubscribe(subscription)?;
                } else {
                    return Err(PublisherError::AlreadyInactive);
                }
            }
            PublisherType::Aggregation { .. } => {
                return Err(PublisherError::AggregationPublishersNotImplemented)
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use protobuf::{well_known_types::wrappers::Int32Value, MessageFull};
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };

    use super::*;

    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";

    struct MockServiceAdapter {
        num_subscribe_calls: AtomicU32,
        num_unsubscribe_calls: AtomicU32,
        subscribe_callback:
            Option<Box<dyn Fn(Box<dyn Fn(&dyn MessageDyn) + Send + Sync>) + Send + Sync>>,
    }

    impl MockServiceAdapter {
        fn new() -> Self {
            MockServiceAdapter {
                num_subscribe_calls: AtomicU32::new(0),
                num_unsubscribe_calls: AtomicU32::new(0),
                subscribe_callback: None,
            }
        }

        fn set_subscribe_callback(
            &mut self,
            callback: impl Fn(Box<dyn Fn(&dyn MessageDyn) + Send + Sync>) + Send + Sync + 'static,
        ) {
            self.subscribe_callback = Some(Box::new(callback));
        }
    }

    impl ServiceAdapter for MockServiceAdapter {
        fn subscribe(
            &self,
            _service_name: &str,
            new_message_callback: Box<dyn Fn(&dyn MessageDyn) + Send + Sync>,
        ) -> ServiceSubscription {
            if let Some(callback) = &self.subscribe_callback {
                callback(new_message_callback);
            }
            self.num_subscribe_calls.fetch_add(1, Ordering::Release);
            ServiceSubscription { service_name: String::default(), subscription_id: u32::MAX }
        }

        fn unsubscribe(
            &self,
            _subscription: ServiceSubscription,
        ) -> Result<(), ServiceAdapterError> {
            self.num_unsubscribe_calls.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    #[test]
    fn activate_signal_publisher_fails_not_implemented() {
        let publisher = Arc::new(PublisherImpl::new(
            PUBLISHER_NAME.to_owned(),
            PublisherType::Signal { signal_name: String::default() },
        ));

        let err = publisher.activate().err().unwrap();

        assert!(
            matches!(&err, PublisherError::SignalPublishersNotImplemented),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn activate_service_publisher_subscription_changes_last_message() {
        let mut service_adapter = MockServiceAdapter::new();
        let new_message_callback = Arc::new(Mutex::new(None));
        let new_message_callback_ref = new_message_callback.clone();
        service_adapter.set_subscribe_callback(move |new_message_callback| {
            *new_message_callback_ref.lock().unwrap() = Some(new_message_callback)
        });

        let service_adapter = Arc::new(service_adapter);
        let publisher = Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            service_adapter.clone(),
        ));
        let mut msg = Int32Value::new();
        msg.value = 42;

        publisher.activate().unwrap();
        new_message_callback.lock().unwrap().as_ref().unwrap()(&msg);

        assert_eq!(
            &publisher.get_latest_message().unwrap(),
            &(Box::new(msg) as Box<dyn MessageDyn>)
        );
        assert_eq!(service_adapter.num_subscribe_calls.load(Ordering::Acquire), 1);
        assert_eq!(service_adapter.num_unsubscribe_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn activate_service_publisher_already_activated_fails() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            service_adapter.clone(),
        ));

        publisher.activate().unwrap();
        let err = publisher.activate().err().unwrap();

        assert!(matches!(&err, PublisherError::AlreadyActive), "Unexpected error: {}", err);
        assert_eq!(service_adapter.num_subscribe_calls.load(Ordering::Acquire), 1);
        assert_eq!(service_adapter.num_unsubscribe_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn activate_aggregation_publisher_fails_not_implemented() {
        let publisher = Arc::new(PublisherImpl::new(
            PUBLISHER_NAME.to_owned(),
            PublisherType::Aggregation {
                triggers: vec![],
                message_descriptor: Int32Value::descriptor(),
                reset_on_get: false,
                declarative_aggregations: vec![],
            },
        ));

        let err = publisher.activate().err().unwrap();

        assert!(
            matches!(&err, PublisherError::AggregationPublishersNotImplemented),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn dectivate_signal_publisher_fails_not_implemented() {
        let publisher = PublisherImpl::new(
            PUBLISHER_NAME.to_owned(),
            PublisherType::Signal { signal_name: String::default() },
        );

        let err = publisher.deactivate().err().unwrap();

        assert!(
            matches!(&err, PublisherError::SignalPublishersNotImplemented),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn deactivate_service_publisher_calls_unsubscribe() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            service_adapter.clone(),
        ));

        publisher.activate().unwrap();
        publisher.deactivate().unwrap();

        assert_eq!(service_adapter.num_subscribe_calls.load(Ordering::Acquire), 1);
        assert_eq!(service_adapter.num_unsubscribe_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn deactivate_service_publisher_not_activated_fails() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            service_adapter.clone(),
        );

        let err = publisher.deactivate().err().unwrap();

        assert!(matches!(&err, PublisherError::AlreadyInactive), "Unexpected error: {}", err);
        assert_eq!(service_adapter.num_subscribe_calls.load(Ordering::Acquire), 0);
        assert_eq!(service_adapter.num_unsubscribe_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn deactivate_aggregation_fails_not_implemented() {
        let publisher = PublisherImpl::new(
            PUBLISHER_NAME.to_owned(),
            PublisherType::Aggregation {
                triggers: vec![],
                message_descriptor: Int32Value::descriptor(),
                reset_on_get: false,
                declarative_aggregations: vec![],
            },
        );

        let err = publisher.deactivate().err().unwrap();

        assert!(
            matches!(&err, PublisherError::AggregationPublishersNotImplemented),
            "Unexpected error: {}",
            err
        );
    }
}
