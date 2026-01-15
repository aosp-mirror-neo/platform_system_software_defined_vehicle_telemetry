// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisherSubscription::IConfigurablePublisherSubscription,
};

/// Manage Configurable Publisher Subscriptions
pub trait ConfigurablePublisherSubscription: Send + Sync {
    /// Ends the underlying subscription.
    fn unsubscribe(&self);
}

/// Aidl implementation for IConfigurablePublisherSubscription that wraps around
/// ConfigurablePublisherSubscription trait object
pub(crate) struct ConfigurablePublisherSubscriptionAidlImpl {
    configurable_publisher_subscription: Box<dyn ConfigurablePublisherSubscription>,
}

impl ConfigurablePublisherSubscriptionAidlImpl {
    pub(crate) fn new(
        configurable_publisher_subscription: Box<dyn ConfigurablePublisherSubscription>,
    ) -> Self {
        Self { configurable_publisher_subscription }
    }
}

impl binder::Interface for ConfigurablePublisherSubscriptionAidlImpl {}

impl IConfigurablePublisherSubscription for ConfigurablePublisherSubscriptionAidlImpl {
    fn unsubscribe(&self) -> binder::Result<()> {
        self.configurable_publisher_subscription.unsubscribe();
        Ok(())
    }
}
