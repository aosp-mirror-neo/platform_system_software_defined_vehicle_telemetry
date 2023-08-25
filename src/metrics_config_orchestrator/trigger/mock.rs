// Copyright 2023 Google LLC. All Rights Reserved.

use std::sync::Arc;

use super::{Trigger, TriggerSubscriber};

pub(crate) struct MockTrigger {
    pub name: String,
    pub subscribe_callback:
        Option<Arc<dyn Fn(&str, Box<dyn Fn() + Send + Sync>) -> Option<String> + Send + Sync>>,
    pub unsubscribe_callback: Option<Arc<dyn Fn(&str) -> Option<String> + Send + Sync>>,
}

impl Trigger for MockTrigger {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn subscribe(
        &self,
        _: &TriggerSubscriber,
        on_fired_callback: Box<dyn Fn() + Send + Sync>,
    ) -> Result<(), String> {
        if let Some(callback) = &self.subscribe_callback {
            if let Some(error) = callback(&self.name, Box::new(on_fired_callback)) {
                return Err(error);
            }
        }

        Ok(())
    }

    fn unsubscribe(&self, _: &TriggerSubscriber) -> Result<(), String> {
        if let Some(callback) = &self.unsubscribe_callback {
            if let Some(error) = callback(&self.name) {
                return Err(error);
            }
        }

        Ok(())
    }
}
