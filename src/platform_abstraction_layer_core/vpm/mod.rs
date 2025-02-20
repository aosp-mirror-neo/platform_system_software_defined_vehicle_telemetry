// Copyright 2025 Google LLC

mod power_state_listener;

use google_sdv_vpm_aidl::aidl::google::sdv::vpm::IPowerStateClientApi::{
    BpPowerStateClientApi, IPowerStateClientApi,
};

use log::{debug, error};
use parking_lot::RwLock;
use persistent_binder::PersistentBinderConnection;
use sdv_telemetry_time_wrapper::Microseconds;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;

use self::power_state_listener::PowerStateListener;

type ResumedAt = Option<Microseconds>;

fn log_error(message: String) {
    // TODO: b/353466210 - These errors should probably also be reported via the misc status
    // callback(?)
    error!("{message}");
}

fn blocking_wait_for_power_state_client_api() -> binder::Strong<dyn IPowerStateClientApi> {
    let descriptor =
        format!("{}/default", <BpPowerStateClientApi as IPowerStateClientApi>::get_descriptor());

    debug!("Waiting for binder interface: \"{descriptor}\"");
    let binder_service = binder::wait_for_interface(&descriptor).unwrap_or_else(|error| {
        log_error(format!("Failed to wait for binder interface \"{descriptor}\": {error}"));
        blocking_wait_for_power_state_client_api()
    });
    debug!("Done waiting for binder interface: \"{descriptor}\"");

    binder_service
}

pub(crate) struct VpmClient {
    resumed_at: Arc<RwLock<ResumedAt>>,
    /// A reference to the handle of the task that creates the `PersistentBinderConnection`.
    ///
    /// It is necessary to keep this reference to ensure the connection isn't dropped before `Self`
    /// is dropped.
    #[cfg_attr(not(test), expect(dead_code))]
    binder_connection_task_handle: AbortOnDropHandle<
        Result<Arc<PersistentBinderConnection<dyn IPowerStateClientApi>>, JoinError>,
    >,
}

impl Debug for VpmClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VpmClient").field("resumed_at", &self.resumed_at).finish_non_exhaustive()
    }
}

impl Default for VpmClient {
    fn default() -> Self {
        Self::new(blocking_wait_for_power_state_client_api)
    }
}

impl VpmClient {
    pub(crate) fn new(
        power_state_client_api_factory: impl Fn() -> binder::Strong<dyn IPowerStateClientApi>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        let resumed_at = Arc::new(RwLock::new(None));

        // We do not want to wait for the connection to the VPM, so that the Telemetry Service
        // startup is as fast as possible. Hence, we spawn a new task to create the connection.
        let weak_resumed_at = Arc::downgrade(&resumed_at);
        let handle = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                PowerStateListener::blocking_subscribe(
                    weak_resumed_at,
                    power_state_client_api_factory,
                )
            })
            .await
        });

        Self { resumed_at, binder_connection_task_handle: AbortOnDropHandle::new(handle) }
    }

    pub(crate) fn get_resumed_at(&self) -> Option<Microseconds> {
        *self.resumed_at.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use google_sdv_vpm_aidl::aidl::google::sdv::vpm::IPowerStateClientApi::{
        BnPowerStateClientApi, MockIPowerStateClientApi,
    };
    use google_sdv_vpm_aidl::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;

    use binder::BinderFeatures;
    use googletest::expect_that;
    use googletest::matcher::MatcherBase;
    use googletest::matchers::{eq, ge, le, none, ok, some};
    use sdv_telemetry_test_utils::parameterized_test;
    use sdv_telemetry_time_wrapper::get_monotonic_time_since_boot;
    use std::time::Duration;
    use tokio::sync::mpsc;

    const TOLERANCE: Microseconds = Microseconds(Duration::from_millis(100).as_micros() as i64);

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn successfully_creates_binder_connection() {
        let client = VpmClient::new(blocking_wait_for_power_state_client_api);

        let result = client.binder_connection_task_handle.await;

        // Explicit type hint to ensure we call `unwrap` until reaching the
        // `PersistentBinderConnection`.
        let _connection: Arc<PersistentBinderConnection<_>> = result.unwrap().unwrap();
    }

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        handles_power_state_report,

        |report: PowerStateReport, matcher_factory| async {
            let (tx, mut rx) = mpsc::channel(1);

            let client = VpmClient::new(move || {
                let mut mock = MockIPowerStateClientApi::new();

                let tx = tx.clone();
                mock.expect_subscribeToPowerStateReport().return_once(move |listener| {
                    tx.try_send(listener.clone()).unwrap();
                    Ok(PowerStateReport::REPORT_UNSPECIFIED)
                });

                BnPowerStateClientApi::new_binder(mock, BinderFeatures::default())
            });

            let listener = rx.recv().await.unwrap();

            let now = get_monotonic_time_since_boot();
            let result = tokio::task::spawn_blocking(move || {
                listener.onPowerStateReportChanged(report)
            }).await;

            expect_that!(result, ok(ok(eq(&()))));
            expect_that!(client.get_resumed_at(), matcher_factory(now));
        },

        on: (PowerStateReport::ON, |_| none()),
        suspend_to_ram_enter: (PowerStateReport::SUSPEND_TO_RAM_ENTER, |_| none()),
        suspend_to_disk_enter: (PowerStateReport::SUSPEND_TO_DISK_ENTER, |_| none()),
        suspend_to_ram_post_finish: (PowerStateReport::SUSPEND_TO_RAM_POST_FINISH, |_| none()),
        suspend_to_disk_post_finish: (PowerStateReport::SUSPEND_TO_DISK_POST_FINISH, |_| none()),

        suspend_to_ram_exit: (
            PowerStateReport::SUSPEND_TO_RAM_EXIT,
            |now: Microseconds|
            some(ge(Microseconds(now.0 - TOLERANCE.0)).and(le(Microseconds(now.0 + TOLERANCE.0))))
        ),
        suspend_to_disk_exit: (
            PowerStateReport::SUSPEND_TO_DISK_EXIT,
            |now: Microseconds|
            some(ge(Microseconds(now.0 - TOLERANCE.0)).and(le(Microseconds(now.0 + TOLERANCE.0))))
        ),
    );
}
