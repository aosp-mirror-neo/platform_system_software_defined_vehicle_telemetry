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

mod resumed_at_storage;
mod vpm;

use futures::StreamExt;
use log::error;
use resumed_at_storage::ResumedAtStorageImpl;
use sdv_telemetry_time_wrapper::Microseconds;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_util::task::AbortOnDropHandle;

use self::resumed_at_storage::ResumedAtStorage;
use self::vpm::VpmClient;

pub(crate) type ResumedAt = Option<Microseconds>;

fn log_error(message: String) {
    // TODO: b/403481321 - These errors should probably also be reported via the misc status
    // callback.
    error!("{message}");
}

/// Tracks the time the VM last resumed. It uses `VpmClient` to be notified when the VM resumes, and
/// `ResumedAtStorage` to read the last resumed time on startup, and to write the last resumed time
/// when notified by the `VpmClient`.
#[derive(Debug)]
pub(crate) struct ResumedAtTracker {
    _vpm_client: VpmClient,

    resumed_at_rx: watch::Receiver<ResumedAt>,
    /// A reference to the handle of the task that is responsible for storing all incoming
    /// `ResumedAt` values. We want to stop the task when `Self` is dropped, hence why we store an
    /// `AbortOnDropHandle` to it here.
    _resumed_at_storer_task_handle: AbortOnDropHandle<()>,
}

impl Default for ResumedAtTracker {
    fn default() -> Self {
        Self::new(VpmClient::new, ResumedAtStorageImpl::new())
    }
}

impl ResumedAtTracker {
    fn new(
        vpm_client_factory: impl FnOnce(watch::Sender<ResumedAt>) -> VpmClient,
        mut resumed_at_storage: impl ResumedAtStorage + 'static,
    ) -> Self {
        let (resumed_at_tx, resumed_at_rx) = watch::channel(
            resumed_at_storage
                .read()
                .inspect_err(|err| {
                    log_error(format!("Failed to initialize ResumedAt timestamp: {err:?}"))
                })
                .unwrap_or(None),
        );

        // Start a task that calls `ResumedAtStorage::write` whenever we receive a new `ResumedAt`
        // that is not `None`.
        let mut resumed_at_stream = WatchStream::new(resumed_at_rx.clone());
        let resumed_at_storer_task_handle = tokio::spawn(async move {
            while let Some(resumed_at) = resumed_at_stream.next().await {
                if let Some(resumed_at) = resumed_at {
                    if let Err(err) = resumed_at_storage.write(resumed_at) {
                        log_error(format!("Failed to persist ResumedAt timestamp: {err:?}"));
                    }
                }
            }
        });

        Self {
            resumed_at_rx,
            _resumed_at_storer_task_handle: AbortOnDropHandle::new(resumed_at_storer_task_handle),
            _vpm_client: vpm_client_factory(resumed_at_tx),
        }
    }

    pub(crate) fn get_resumed_at(&self) -> ResumedAt {
        *self.resumed_at_rx.borrow()
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
    use googletest::matcher::MatcherBase;
    use googletest::matchers::{eq, ge, le, none, ok, some};
    use googletest::{assert_that, expect_that};
    use sdv_telemetry_test_utils::parameterized_test;
    use sdv_telemetry_time_wrapper::{get_monotonic_time_since_boot, Microseconds};
    use std::time::Duration;
    use tokio::sync::mpsc;

    use self::resumed_at_storage::{MockResumedAtStorage, SysPropError};

    const TOLERANCE: Microseconds = Microseconds(Duration::from_millis(100).as_micros() as i64);

    parameterized_test!(
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        initializes_resumed_at_from_storage,

        |storage_read_return_value, matcher| async {
            let tracker = ResumedAtTracker::new(
                VpmClient::new,
                {
                    let mut mock = MockResumedAtStorage::new();
                    mock.expect_read().return_once(|| storage_read_return_value);
                    mock
                },
            );

            assert_that!(tracker.get_resumed_at(), matcher);
        },

        ok_none: (Ok(None), none()),
        ok_some: (Ok(Some(Microseconds(123))), some(eq(Microseconds(123)))),
        // Errors should be ignored and default to `None`.
        err: (Err(SysPropError::ParseError(String::default())), none()),
    );

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        handles_power_state_report,

        |report: PowerStateReport, matcher_factory| async {
            let (listener_tx, mut listener_rx) = mpsc::channel(1);

            let tracker = ResumedAtTracker::new(
                move |resumed_at_tx| {
                    VpmClient::with_custom_power_state_client_api_factory(
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
                    )
                },
                {
                    let mut mock = MockResumedAtStorage::new();
                    mock.expect_read().return_once(|| Ok(None));
                    mock.expect_write().return_once(|_| Ok(()));
                    mock
                }
            );

            let listener = listener_rx.recv().await.unwrap();

            let now = get_monotonic_time_since_boot();
            let result = tokio::task::spawn_blocking(move || {
                listener.onPowerStateReportChanged(report)
            }).await;

            expect_that!(result, ok(ok(eq(&()))));
            expect_that!(tracker.get_resumed_at(), matcher_factory(now));
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
