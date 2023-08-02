// Copyright 2023 Google LLC. All Rights Reserved.

use sdv_ast::Node;
use std::{
    marker::Send,
    sync::{Arc, Mutex, Weak},
};
use tokio::time::Duration;

use crate::metrics_config_orchestrator::Publisher;

pub struct Trigger {
    pub name: String,
    pub trigger_type: TriggerType,

    // Specifies whether the trigger is active.
    // Each activation creates a new Arc and spawns a new thread that finishes when the Arc is freed.
    // Deactivation creates a new Arc and frees the previous Arc, finishing the related thread.
    is_active: Mutex<Arc<bool>>,
}

pub enum TriggerType {
    Data { publisher: Weak<Publisher> },
    Periodic { interval_ms: u64 },
    Conditional { triggers: Vec<Weak<Trigger>>, selector_expression: Arc<Node> },
}

impl Trigger {
    pub fn new(name: String, trigger_type: TriggerType) -> Self {
        Trigger { name, trigger_type, is_active: Mutex::new(Arc::new(false)) }
    }

    #[allow(dead_code)]
    pub fn activate(&self, on_fired_callback: Box<dyn Fn() + Send>) -> Result<(), String> {
        let mut is_active = self.is_active.lock().unwrap();

        if **is_active {
            return Err("The trigger is already active".to_owned());
        }

        // Create a new Arc and a weak reference to it to monitor when it is replaced and freed.
        *is_active = Arc::new(true);
        self.trigger_type.activate(on_fired_callback, Arc::downgrade(&is_active))?;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn deactivate(&self) -> Result<(), String> {
        let mut is_active = self.is_active.lock().unwrap();

        if !**is_active {
            return Err("The trigger is not active".to_owned());
        }

        // Replace the Arc, freeing the previous one. This will finish the related thread.
        *is_active = Arc::new(false);

        Ok(())
    }
}

impl TriggerType {
    pub fn activate(
        &self,
        on_fired_callback: Box<dyn Fn() + Send>,
        weak_is_active: Weak<bool>,
    ) -> Result<(), String> {
        match self {
            TriggerType::Data { .. } => {
                return Err("Activation of Data triggers is not implemented".to_owned())
            }
            TriggerType::Periodic { interval_ms } => tokio::spawn(Self::call_periodically(
                *interval_ms,
                on_fired_callback,
                weak_is_active,
            )),
            TriggerType::Conditional { .. } => {
                return Err("Activation of Conditional triggers is not implemented".to_owned())
            }
        };
        Ok(())
    }

    /// Periodically invokes callback until the activation Arc is freed.
    async fn call_periodically(
        interval_ms: u64,
        on_fired_callback: Box<dyn Fn() + Send>,
        weak_is_active: Weak<bool>,
    ) {
        let mut timer = tokio::time::interval(Duration::from_millis(interval_ms));

        // Trigger first tick that returns immediately.
        timer.tick().await;

        loop {
            // Verify that trigger hasn't been deactivated and the Arc hasn't been freed.
            if weak_is_active.upgrade().is_none() {
                return;
            }

            // Sleep until next interval tick.
            timer.tick().await;

            // Verify that the Arc hasn't been freed during sleep.
            if weak_is_active.upgrade().is_none() {
                return;
            }

            // Fire the trigger.
            on_fired_callback();
        }
    }
}

#[cfg(test)]
mod tests {
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

    fn create_valid_periodic_trigger() -> Trigger {
        Trigger::new(TRIGGER_NAME.to_owned(), TriggerType::Periodic { interval_ms: INTERVAL_MS })
    }

    #[tokio::test]
    async fn activate_already_active_fails() {
        let trigger = create_valid_periodic_trigger();

        trigger.activate(Box::new(|| {})).unwrap();
        let err = trigger.activate(Box::new(|| {})).err().unwrap();

        assert!(err.contains("already active"));
    }

    #[test]
    fn activate_data_trigger_fails_not_implemented() {
        let trigger = Trigger::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Data {
                publisher: Arc::downgrade(&Arc::new(Publisher {
                    name: "Publisher".to_owned(),
                    publisher_type: PublisherType::Signal { signal_name: "Signal".to_owned() },
                })),
            },
        );

        let err = trigger.activate(Box::new(|| {})).err().unwrap();

        assert!(err.contains("not implemented"));
    }

    #[tokio::test]
    async fn activate_periodic_immediate_deactivate_doesnt_fire() {
        let trigger = create_valid_periodic_trigger();
        let callback_listener = Arc::new(Mutex::new(CallbackListener::new()));
        let callback_listener_clone = callback_listener.clone();

        trigger
            .activate(Box::new(move || callback_listener_clone.lock().unwrap().on_callback_fired()))
            .unwrap();
        trigger.deactivate().unwrap();
        // Sleep for 1.5 interval to ensure the trigger won't fire.
        time::sleep(Duration::from_millis(INTERVAL_MS * 3 / 2)).await;

        assert_eq!(callback_listener.lock().unwrap().fired_count, 0);
    }

    #[tokio::test]
    async fn activate_periodic_trigger_fires_twice() {
        let trigger = create_valid_periodic_trigger();
        let callback_listener = Arc::new(Mutex::new(CallbackListener::new()));
        let callback_listener_clone = callback_listener.clone();

        trigger
            .activate(Box::new(move || callback_listener_clone.lock().unwrap().on_callback_fired()))
            .unwrap();
        // Sleep for 2.5 interval to let trigger fire twice.
        time::sleep(Duration::from_millis(INTERVAL_MS * 5 / 2)).await;
        trigger.deactivate().unwrap();
        // Sleep for 1.5 interval to ensure the trigger won't fire.
        time::sleep(Duration::from_millis(INTERVAL_MS * 3 / 2)).await;

        assert_eq!(callback_listener.lock().unwrap().fired_count, 2);
    }

    #[test]
    fn activate_conditional_trigger_fails_not_implemented() {
        let trigger = Trigger::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Conditional {
                triggers: vec![],
                selector_expression: Arc::new(Node::default()),
            },
        );

        let err = trigger.activate(Box::new(|| {})).err().unwrap();

        assert!(err.contains("not implemented"));
    }

    #[test]
    fn deactivate_already_inactive_fails() {
        let trigger = create_valid_periodic_trigger();

        let err = trigger.deactivate().err().unwrap();

        assert!(err.contains("is not active"));
    }
}
