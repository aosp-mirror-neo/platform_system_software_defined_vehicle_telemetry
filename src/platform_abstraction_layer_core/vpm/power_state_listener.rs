// Copyright 2025 Google LLC

use google_sdv_vpm_aidl::aidl::google::sdv::vpm::IPowerStateClientApi::IPowerStateClientApi;
use google_sdv_vpm_aidl::aidl::google::sdv::vpm::IPowerStateReportListener::{
    BnPowerStateReportListener, IPowerStateReportListener, IPowerStateReportListenerAsyncServer,
};
use google_sdv_vpm_aidl::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;

use async_trait::async_trait;
use binder_tokio::TokioRuntime;
use log::debug;
use parking_lot::RwLock;
use persistent_binder::PersistentBinderConnection;
use sdv_telemetry_time_wrapper::get_monotonic_time_since_boot;
use std::sync::{Arc, Weak};

use super::{log_error, ResumedAt};

/// Wrapper around a weak pointer to `ResumedAt` that subscribes to the VPM as a power state client.
/// Whenever it receives a power state report, it updates `resumed_at`, if applicable.
#[derive(Clone)]
pub(super) struct PowerStateListener {
    pub(super) resumed_at: Weak<RwLock<ResumedAt>>,
}

impl binder::Interface for PowerStateListener {}

#[async_trait]
impl IPowerStateReportListenerAsyncServer for PowerStateListener {
    async fn onPowerStateReportChanged(
        &self,
        power_state_report: PowerStateReport,
    ) -> binder::Result<()> {
        // BEWARE: This method must return as fast as possible, because the VPM will wait for this
        // method to complete, and only then change the vehicle power state.

        debug!("Received power state report: {power_state_report:?}");
        match power_state_report {
            PowerStateReport::SUSPEND_TO_RAM_EXIT | PowerStateReport::SUSPEND_TO_DISK_EXIT => {
                // Get the timestamp BEFORE waiting for the lock (waiting for the lock should be
                // super quick, but let's get the timestamp here just in case).
                let now = get_monotonic_time_since_boot();

                if let Some(resumed_at) = self.resumed_at.upgrade() {
                    *resumed_at.write() = Some(now);
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl PowerStateListener {
    /// Creates a new `PowerStateListener` that subscribes to power state reports and updates
    /// `resumed_at` whenever a power state report notifies it of the VM resuming from sleep. The
    /// binder connection is managed by the `PersistentBinderConnection`, which this function
    /// returns, and which automatically resubscribes on binder death, as long as `resumed_at` still
    /// points somewhere.
    ///
    /// TODO: b/353466210 - Unfortunately, `PersistentBinderConnection` uses and expects blocking
    /// binder calls. We should refactor it to use async binder calls.
    pub(super) fn blocking_subscribe(
        resumed_at: Weak<RwLock<ResumedAt>>,
        power_state_client_api_factory: impl Fn() -> binder::Strong<dyn IPowerStateClientApi>
            + Send
            + Sync
            + 'static,
    ) -> Arc<PersistentBinderConnection<dyn IPowerStateClientApi>> {
        let tokio_runtime_handle = tokio::runtime::Handle::current();

        let binder_connection = PersistentBinderConnection::new(power_state_client_api_factory);

        let power_state_listener = Self { resumed_at };

        {
            let binder_mutex = binder_connection.get_binder();
            let power_state_client_api = binder_mutex.lock().unwrap();
            if let Err(err) = power_state_client_api.subscribeToPowerStateReport(
                &power_state_listener.clone().into_binder(tokio_runtime_handle.clone()),
            ) {
                // TODO: b/353466210 - There is no retry logic here. Is it needed? Other examples
                // using `PersistentBinderConnection` also do not retry here.
                log_error(format!("Failed to subscribe to power state reports: {err}"));
            }
        }
        binder_connection.set_reconnect_listener(move |power_state_client_api| {
            // This callback is called directly from Binder and run outside of a Tokio
            // runtime, hence we cannot access the current Tokio runtime handle here,
            // and instead need to re-use the handle saved in `tokio_runtime_handle`.

            // Only resubscribe to Binder if `resumed_at` is still pointing somewhere.
            if power_state_listener.resumed_at.upgrade().is_some() {
                if let Err(err) = power_state_client_api.subscribeToPowerStateReport(
                    &power_state_listener.clone().into_binder(tokio_runtime_handle.clone()),
                ) {
                    // TODO: b/353466210 - There is no retry logic here. Is it needed? Other
                    // examples using `PersistentBinderConnection` also do not retry here.
                    log_error(format!("Failed to subscribe to power state reports: {err}"));
                }
            }
        });

        binder_connection
    }

    fn into_binder(
        self,
        tokio_runtime_handle: tokio::runtime::Handle,
    ) -> binder::Strong<dyn IPowerStateReportListener> {
        BnPowerStateReportListener::new_async_binder(
            self,
            TokioRuntime(tokio_runtime_handle),
            binder::BinderFeatures::default(),
        )
    }
}
