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

use binder::{Interface, Result as BinderResult};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::IConfigurablePublisherSubscription;

pub(crate) struct Subscription {
    on_unsubscribe: Box<dyn Fn() + Send + Sync + 'static>,
}

impl Interface for Subscription {}

impl Subscription {
    pub(crate) fn new(on_unsubscribe: impl Fn() + Send + Sync + 'static) -> Self {
        Self { on_unsubscribe: Box::new(on_unsubscribe) }
    }
}

impl IConfigurablePublisherSubscription for Subscription {
    fn unsubscribe(&self) -> BinderResult<()> {
        (self.on_unsubscribe)();
        Ok(())
    }
}
