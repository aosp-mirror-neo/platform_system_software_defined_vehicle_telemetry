// Copyright 2024 Google LLC

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
