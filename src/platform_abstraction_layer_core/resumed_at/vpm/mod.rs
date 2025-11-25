// Copyright 2025 Google LLC

mod power_state_listener;

use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::{
    BpPowerStateClientApi, IPowerStateClientApi,
};

use log::debug;
use persistent_binder::BinderConnectionManager;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::{sync::watch, task::JoinError};
use tokio_util::task::AbortOnDropHandle;

use self::power_state_listener::PowerStateListener;
use crate::resumed_at::{log_error, ResumedAt};

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
    /// A reference to the handle of the task that creates the `BinderConnectionManager`.
    ///
    /// It is necessary to keep this reference to ensure the connection isn't dropped before `Self`
    /// is dropped.
    #[cfg_attr(not(test), expect(dead_code))]
    binder_connection_task_handle: AbortOnDropHandle<
        Result<Arc<dyn BinderConnectionManager<dyn IPowerStateClientApi>>, JoinError>,
    >,
}

impl Debug for VpmClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VpmClient").finish_non_exhaustive()
    }
}

impl VpmClient {
    pub(crate) fn new(resumed_at_tx: watch::Sender<ResumedAt>) -> Self {
        Self::with_custom_power_state_client_api_factory(
            resumed_at_tx,
            blocking_wait_for_power_state_client_api,
        )
    }

    /// Only exposed for testing: Constructs `Self` with a customizable `IPowerStateClientApi`
    /// factory, instead of using the default.
    pub(crate) fn with_custom_power_state_client_api_factory(
        resumed_at_tx: watch::Sender<ResumedAt>,
        power_state_client_api_factory: impl Fn() -> binder::Strong<dyn IPowerStateClientApi>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        // We do not want to wait for the connection to the VPM, so that the Telemetry Service
        // startup is as fast as possible. Hence, we spawn a new task to create the connection.
        let handle = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                PowerStateListener::blocking_subscribe(
                    resumed_at_tx,
                    power_state_client_api_factory,
                )
            })
            .await
        });

        Self { binder_connection_task_handle: AbortOnDropHandle::new(handle) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::{
        BnPowerStateClientApi, MockIPowerStateClientApi,
    };
    use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;

    use binder::BinderFeatures;
    use googletest::expect_that;
    use googletest::matcher::MatcherBase;
    use googletest::matchers::{anything, eq, ge, le, none, ok, some};
    use sdv_telemetry_test_utils::parameterized_test;
    use sdv_telemetry_time_wrapper::get_monotonic_time_since_boot;
    use sdv_telemetry_time_wrapper::Microseconds;
    use std::time::Duration;
    use tokio::{select, sync::mpsc, time::sleep};

    const TOLERANCE: Microseconds = Microseconds(Duration::from_millis(100).as_micros() as i64);

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn successfully_creates_binder_connection() {
        let (resumed_at_tx, _) = watch::channel(None);
        let client = VpmClient::new(resumed_at_tx);

        let result = client.binder_connection_task_handle.await;

        // Explicit type hint to ensure we call `unwrap` until reaching the
        // `PersistentBinderConnection`.
        let _connection: Arc<dyn BinderConnectionManager<_>> = result.unwrap().unwrap();
    }

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        handles_power_state_report,

        |report: PowerStateReport, matcher_factory| async {
            let (listener_tx, mut listener_rx) = mpsc::channel(1);
            let (resumed_at_tx, mut resumed_at_rx) = watch::channel(None);

            let _client = VpmClient::with_custom_power_state_client_api_factory(
                resumed_at_tx,
                move || {
                    let mut mock = MockIPowerStateClientApi::new();

                    let listener_tx = listener_tx.clone();
                    mock.expect_subscribeToPowerStateReport().return_once(move |listener| {
                        listener_tx.try_send(listener.clone()).unwrap();
                        Ok(PowerStateReport::REPORT_UNSPECIFIED)
                    });

                    BnPowerStateClientApi::new_binder(mock, BinderFeatures::default())
                },
            );

            let listener = listener_rx.recv().await.unwrap();

            let now = get_monotonic_time_since_boot();
            let result = tokio::task::spawn_blocking(move || {
                listener.onPowerStateReportChanged(report)
            }).await;

            expect_that!(result, ok(ok(eq(&()))));

            // Wait for `resumed_at_rx` to change, or at most 300ms. It will never change if an
            // unrelated power state report is received - but it will also not update synchronously
            // if a matching one is received, hence why we cannot just synchronously check
            // `resumed_at_rx`.
            let changed_or_timeout = select! {
                changed = resumed_at_rx.changed() => changed,
                _ = sleep(Duration::from_millis(300)) => Ok(()),
            };
            expect_that!(changed_or_timeout, ok(anything()));
            expect_that!(*resumed_at_rx.borrow(), matcher_factory(now));
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
