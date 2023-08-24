// Copyright 2023 Google LLC. All Rights Reserved.

use sdv_ast::Node;
use std::{
    collections::HashMap,
    fmt,
    marker::{Send, Sync},
    sync::{Arc, Mutex, Weak},
};
use tokio::time::Duration;

use crate::collection_extension::HashMapExtension;
use crate::metrics_config_orchestrator::PublisherImpl;

pub(crate) trait Trigger {
    /// Returns the name of the trigger.
    fn get_name(&self) -> &str;

    /// Subscribe to trigger fire events.
    /// First subscription activates the trigger.
    fn subscribe(
        &self,
        subscriber: &TriggerSubscriber,
        on_fired_callback: Box<dyn Fn() + Send + Sync>,
    ) -> Result<(), String>;

    /// Unsubscribe from trigger fire events.
    /// Last unsubscription deactivates the trigger.
    fn unsubscribe(&self, subscriber: &TriggerSubscriber) -> Result<(), String>;
}

#[allow(dead_code)]
#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum TriggerSubscriber {
    AggregationPublisher { name: String },
    MetricsConfigStart,
    MetricsConfigEnd,
    MetricsConfigFinish,
    MetricsReportGenerator { name: String },
    PeriodicTrigger { name: String },
}

#[allow(dead_code)]
pub(crate) struct TriggerImpl {
    pub(super) name: String,
    pub(super) trigger_type: TriggerType,

    // The map of functions that need to be called when the trigger fires.
    // The first subscription to TriggerImpl activates underlying trigger type, passing a weak reference
    // to the map. Underlying trigger type must stop firing when the reference becomes invalid.
    // Last unsubscription of TriggerImpl creates a new Arc and frees the previous Arc,
    // making weak reference invalid.
    callback_map: Mutex<Arc<Mutex<HashMap<TriggerSubscriber, Arc<dyn Fn() + Send + Sync>>>>>,
}

#[allow(dead_code)]
pub(crate) enum TriggerType {
    Data { publisher: Arc<PublisherImpl> },
    Periodic { interval_ms: u64 },
    Conditional { triggers: Vec<Arc<TriggerImpl>>, selector_expression: Arc<Node> },
}

impl fmt::Display for TriggerSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerSubscriber::AggregationPublisher { name } => {
                write!(f, "Aggregation Publisher {}", name)
            }
            TriggerSubscriber::MetricsConfigStart => write!(f, "Metrics Config Start"),
            TriggerSubscriber::MetricsConfigEnd => write!(f, "Metrics Config End"),
            TriggerSubscriber::MetricsConfigFinish => write!(f, "Metrics Config Finish"),
            TriggerSubscriber::MetricsReportGenerator { name } => {
                write!(f, "Metrics Report Generator {}", name)
            }
            TriggerSubscriber::PeriodicTrigger { name } => write!(f, "Periodic Trigger {}", name),
        }
    }
}

impl TriggerImpl {
    pub(super) fn new(name: String, trigger_type: TriggerType) -> Self {
        TriggerImpl {
            name,
            trigger_type,
            callback_map: Mutex::new(Arc::new(Mutex::new(HashMap::new()))),
        }
    }
}

impl Trigger for TriggerImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn subscribe(
        &self,
        subscriber: &TriggerSubscriber,
        on_fired_callback: Box<dyn Fn() + Send + Sync>,
    ) -> Result<(), String> {
        let maybe_callbacks_reference = {
            let callbacks_container = self.callback_map.lock().unwrap();
            let mut callbacks = callbacks_container.lock().unwrap();
            if callbacks.try_insert_value(subscriber.clone(), on_fired_callback.into()).is_err() {
                return Err(format!("Subscriber {} is already subscribed to the trigger. Duplicate subscriptions are not allowed.", subscriber));
            }

            // Return a weak reference to the list if this is the first subscription.
            if callbacks.len() == 1 {
                Some(Arc::downgrade(&callbacks_container))
            } else {
                None
            }
        };

        if let Some(callbacks_reference) = maybe_callbacks_reference {
            // If a weak reference was returned, this is the first subscription and underlying
            // trigger type needs to be activated.
            self.trigger_type.activate(callbacks_reference)?;
        }

        Ok(())
    }

    fn unsubscribe(&self, subscriber: &TriggerSubscriber) -> Result<(), String> {
        let mut callbacks_container = self.callback_map.lock().unwrap();
        let is_last_unsubscription = {
            let mut callbacks = callbacks_container.lock().unwrap();
            if callbacks.remove(subscriber).is_none() {
                return Err(format!("Subscriber {} wasn't subscribed to the trigger", subscriber));
            }

            callbacks.is_empty()
        };

        if is_last_unsubscription {
            // Replace the Arc, freeing the previous one. This will deactivate underlying trigger type.
            *callbacks_container = Arc::new(Mutex::new(HashMap::new()));
        }

        Ok(())
    }
}

impl TriggerType {
    pub(super) fn activate(
        &self,
        callbacks_reference: Weak<Mutex<HashMap<TriggerSubscriber, Arc<dyn Fn() + Send + Sync>>>>,
    ) -> Result<(), String> {
        match self {
            TriggerType::Data { .. } => {
                return Err("Activation of Data triggers is not implemented".to_owned())
            }
            TriggerType::Periodic { interval_ms } => {
                tokio::spawn(Self::call_periodically(*interval_ms, callbacks_reference));
            }
            TriggerType::Conditional { .. } => {
                return Err("Activation of Conditional triggers is not implemented".to_owned())
            }
        };
        Ok(())
    }

    /// Periodically invokes callbacks until the weak reference is no longer valid.
    async fn call_periodically(
        interval_ms: u64,
        callbacks_reference: Weak<Mutex<HashMap<TriggerSubscriber, Arc<dyn Fn() + Send + Sync>>>>,
    ) {
        let mut timer = tokio::time::interval(Duration::from_millis(interval_ms));

        // Trigger first tick that returns immediately.
        timer.tick().await;

        loop {
            // Verify that trigger hasn't been deactivated and the Arc hasn't been freed.
            if callbacks_reference.upgrade().is_none() {
                return;
            }

            // Sleep until next interval tick.
            timer.tick().await;

            // Try to retrieve callbacks from the weak reference.
            let callbacks = {
                if let Some(callbacks) = callbacks_reference.upgrade() {
                    callbacks.lock().unwrap().values().cloned().collect::<Vec<_>>()
                } else {
                    return;
                }
            };

            // Fire the trigger.
            for callback in callbacks {
                callback();
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use sdv_ast::NodeValue;
    use std::sync::Mutex;
    use tokio::time;

    use crate::metrics_config_orchestrator::publisher::PublisherType;

    use super::*;

    const INTERVAL_MS: u64 = 10;
    const TRIGGER_NAME: &str = "trigger";

    struct CallbackListener {
        fired_count: u32,
    }

    impl CallbackListener {
        fn new() -> CallbackListener {
            CallbackListener { fired_count: 0 }
        }

        fn on_callback_fired(&mut self) {
            self.fired_count += 1;
        }
    }

    fn create_node() -> Node {
        Node::ConstantLeaf { value: NodeValue::Bool(false) }
    }

    fn create_valid_periodic_trigger() -> TriggerImpl {
        TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL_MS },
        )
    }

    fn empty_fn() {}

    fn create_empty_fn_box() -> Box<dyn Fn() + Send + Sync> {
        Box::new(empty_fn)
    }

    #[tokio::test]
    async fn subscribe_already_subscribed_fails() {
        let trigger = create_valid_periodic_trigger();

        trigger.subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box()).unwrap();
        let err = trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .err()
            .unwrap();

        assert!(err.contains("already subscribed"));
    }

    #[test]
    fn subscribe_to_data_trigger_fails_not_implemented() {
        let trigger = TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Data {
                publisher: Arc::new(PublisherImpl::new(
                    "Publisher".to_owned(),
                    PublisherType::Signal { signal_name: "Signal".to_owned() },
                )),
            },
        );

        let err = trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .err()
            .unwrap();

        assert!(err.contains("not implemented"));
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_immediately_unsubscribe_doesnt_fire() {
        let trigger = create_valid_periodic_trigger();
        let callback_listener = Arc::new(Mutex::new(CallbackListener::new()));
        let callback_listener_clone = callback_listener.clone();

        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.lock().unwrap().on_callback_fired()),
            )
            .unwrap();
        trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).unwrap();
        // Sleep for 1.5 interval to ensure the trigger won't fire.
        time::sleep(Duration::from_millis(INTERVAL_MS * 3 / 2)).await;

        assert_eq!(callback_listener.lock().unwrap().fired_count, 0);
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_fires_twice() {
        let trigger = create_valid_periodic_trigger();
        let callback_listener = Arc::new(Mutex::new(CallbackListener::new()));
        let callback_listener_clone = callback_listener.clone();

        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.lock().unwrap().on_callback_fired()),
            )
            .unwrap();
        // Sleep for 2.5 interval to let trigger fire twice.
        time::sleep(Duration::from_millis(INTERVAL_MS * 5 / 2)).await;
        trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).unwrap();
        // Sleep for 1.5 interval to ensure the trigger won't fire.
        time::sleep(Duration::from_millis(INTERVAL_MS * 3 / 2)).await;

        assert_eq!(callback_listener.lock().unwrap().fired_count, 2);
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_multiple_subscriptions_no_duplicate_fires() {
        let trigger = create_valid_periodic_trigger();
        let callback_listener = Arc::new(Mutex::new(CallbackListener::new()));
        let callback_listener_clone1 = callback_listener.clone();
        let callback_listener_clone2 = callback_listener.clone();
        let callback_listener_clone3 = callback_listener.clone();

        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone1.lock().unwrap().on_callback_fired()),
            )
            .unwrap();
        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigEnd,
                Box::new(move || callback_listener_clone2.lock().unwrap().on_callback_fired()),
            )
            .unwrap();
        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigFinish,
                Box::new(move || callback_listener_clone3.lock().unwrap().on_callback_fired()),
            )
            .unwrap();
        // Sleep for 1.5 interval to let trigger fire once.
        time::sleep(Duration::from_millis(INTERVAL_MS * 3 / 2)).await;

        assert_eq!(callback_listener.lock().unwrap().fired_count, 3);
    }

    #[test]
    fn subscribe_to_conditional_trigger_fails_not_implemented() {
        let trigger = TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Conditional {
                triggers: vec![],
                selector_expression: Arc::new(create_node()),
            },
        );

        let err = trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .err()
            .unwrap();

        assert!(err.contains("not implemented"));
    }

    #[test]
    fn unsubscribe_not_subscribed_fails() {
        let trigger = create_valid_periodic_trigger();

        let err = trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).err().unwrap();

        assert!(err.contains("wasn't subscribed"));
    }
}
