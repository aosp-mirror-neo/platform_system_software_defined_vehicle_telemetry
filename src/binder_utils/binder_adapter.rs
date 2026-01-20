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

//! Provides adapter binder interface

use binder::IBinder;

/// Adapter interface in order to fake certain binder API calls that are only implemented
/// for remote binders in tests, which use local binders.
pub trait BinderAdapter<T: binder::FromIBinder + ?Sized>: Send + Sync {
    /// Subscribe to death notifications from the binder.
    /// Subscription is removed when the returned death recipient is dropped.
    fn link_to_death(
        &self,
        binder: &binder::Strong<T>,
        callback: Box<dyn Fn() + Send + Sync>,
    ) -> Result<binder::DeathRecipient, binder::StatusCode>;
}

/// Implementation of adapter that uses real binder.
pub struct BinderAdapterImpl {}

impl<T: binder::FromIBinder + ?Sized> BinderAdapter<T> for BinderAdapterImpl {
    fn link_to_death(
        &self,
        binder: &binder::Strong<T>,
        callback: Box<dyn Fn() + Send + Sync>,
    ) -> Result<binder::DeathRecipient, binder::StatusCode> {
        let mut death_recipient = binder::DeathRecipient::new(callback);
        binder.as_binder().link_to_death(&mut death_recipient)?;
        Ok(death_recipient)
    }
}
