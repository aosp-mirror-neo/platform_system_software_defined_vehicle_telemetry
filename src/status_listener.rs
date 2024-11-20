// Copyright 2024 Google LLC

use google_sdv_telemetry::aidl::google::sdv::telemetry::{
    ITelemetryApiStatusListener::ITelemetryApiStatusListener,
    ITelemetryMiscStatusListener::ITelemetryMiscStatusListener,
    TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
};
use log::warn;
use parking_lot::Mutex;
use std::collections::VecDeque;

const MAX_MISC_ERROR_STORE_SIZE: usize = 128;

pub(super) trait StatusListener: Send + Sync {
    fn send_api_status(&self, api_call_id: i32, status_code: StatusCodes, status_message: &str);
    fn send_misc_status(&self, status_code: StatusCodes, status_message: &str);

    fn clear_api_status_listener(&self);
    fn clear_misc_status_listener(&self);

    fn set_api_status_listener(
        &self,
        new_listener: &binder::Strong<dyn ITelemetryApiStatusListener>,
    ) -> binder::Result<()>;

    fn set_misc_status_listener(
        &self,
        api_call_id: i32,
        new_listener: &binder::Strong<dyn ITelemetryMiscStatusListener>,
    );
}

pub(super) struct StatusListenerImpl {
    /// Status Listener to send back status to client.
    api_status_listener: Mutex<Option<binder::Strong<dyn ITelemetryApiStatusListener>>>,
    /// Misc Status Listener to send back status to client.
    misc_status_listener: Mutex<Option<binder::Strong<dyn ITelemetryMiscStatusListener>>>,
    /// Store for miscellaneous errors.
    misc_error_store: Mutex<VecDeque<(StatusCodes, String)>>,
}

impl StatusListener for StatusListenerImpl {
    /// Uses the listener object set by the telemetry service cpp client to send back status codes
    /// and error messages for API calls
    fn send_api_status(&self, api_call_id: i32, status_code: StatusCodes, status_message: &str) {
        if let Some(listener_ref) = self.api_status_listener.lock().as_ref() {
            if let Err(e) = listener_ref.sendStatus(api_call_id, status_code, status_message) {
                warn!(
                    "Error when sending status code {:?} for API call {} to the client: {:?}",
                    status_code, api_call_id, e
                );
            }
        }
    }

    /// Uses the global listener object set by the client to send back status codes
    /// and error messages for API calls
    fn send_misc_status(&self, status_code: StatusCodes, status_message: &str) {
        if let Some(listener_ref) = self.misc_status_listener.lock().as_ref() {
            if let Err(e) = listener_ref.sendStatus(status_code, status_message) {
                warn!("Error when sending status code {:?} to the client: {:?}", status_code, e);
            }
        } else {
            let mut guard = self.misc_error_store.lock();
            guard.push_back((status_code, status_message.to_string()));
            while guard.len() > MAX_MISC_ERROR_STORE_SIZE {
                guard.pop_front();
            }
        }
    }

    fn clear_api_status_listener(&self) {
        *self.api_status_listener.lock() = None;
    }

    fn clear_misc_status_listener(&self) {
        *self.misc_status_listener.lock() = None;
    }

    fn set_api_status_listener(
        &self,
        new_listener: &binder::Strong<dyn ITelemetryApiStatusListener>,
    ) -> binder::Result<()> {
        let mut guard = self.api_status_listener.lock();
        if guard.is_some() {
            Err(binder::Status::new_exception_str(
                binder::ExceptionCode::ILLEGAL_STATE,
                Some("API Status Listener is already set"),
            ))
        } else {
            guard.replace(new_listener.clone());
            Ok(())
        }
    }

    fn set_misc_status_listener(
        &self,
        api_call_id: i32,
        new_listener: &binder::Strong<dyn ITelemetryMiscStatusListener>,
    ) {
        let (status_code, status_message) = {
            let mut guard = self.misc_status_listener.lock();
            if guard.is_some() {
                (
                    StatusCodes::SET_MISC_STATUS_LISTENER_FAILED,
                    "Misc Status listener is already set",
                )
            } else {
                guard.replace(new_listener.clone());
                (
                    StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                    "Misc Status listener successfully set",
                )
            }
        };
        self.send_api_status(api_call_id, status_code, status_message);

        if status_code == StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
            let mut misc_errors = self.misc_error_store.lock();
            while let Some((status_code, status_message)) = misc_errors.pop_front() {
                self.send_misc_status(status_code, &status_message);
            }
        }
    }
}

impl StatusListenerImpl {
    pub(super) fn new() -> Self {
        Self {
            api_status_listener: None.into(),
            misc_status_listener: None.into(),
            misc_error_store: VecDeque::new().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use binder::BinderFeatures;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::ITelemetryApiStatusListener::BnTelemetryApiStatusListener;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::ITelemetryMiscStatusListener::BnTelemetryMiscStatusListener;
    use std::sync::Arc;

    #[derive(Clone)]
    struct MockTelemetryApiStatusListener {
        last_call: Arc<Mutex<Option<(i32, StatusCodes)>>>,
    }

    impl binder::Interface for MockTelemetryApiStatusListener {}

    impl MockTelemetryApiStatusListener {
        fn new() -> Self {
            Self { last_call: Arc::new(Mutex::new(None)) }
        }

        pub fn get_last_call(&self) -> Option<(i32, StatusCodes)> {
            *self.last_call.lock()
        }
    }

    impl ITelemetryApiStatusListener for MockTelemetryApiStatusListener {
        fn sendStatus(
            &self,
            api_call_id: i32,
            status_code: StatusCodes,
            _status_msg: &str,
        ) -> binder::Result<()> {
            self.last_call.lock().replace((api_call_id, status_code));
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryMiscStatusListener AIDL interface for unit
    /// testing purposes
    #[derive(Clone)]
    struct MockTelemetryMiscStatusListener {
        last_call: Arc<Mutex<Option<StatusCodes>>>,
    }

    impl binder::Interface for MockTelemetryMiscStatusListener {}

    impl MockTelemetryMiscStatusListener {
        fn new() -> Self {
            Self { last_call: Arc::new(Mutex::new(None)) }
        }

        pub fn get_last_call(&self) -> Option<StatusCodes> {
            *self.last_call.lock()
        }
    }

    impl ITelemetryMiscStatusListener for MockTelemetryMiscStatusListener {
        fn sendStatus(&self, status_code: StatusCodes, _status_msg: &str) -> binder::Result<()> {
            self.last_call.lock().replace(status_code);
            Ok(())
        }
    }

    #[test]
    fn send_api_status_with_listener_success() {
        let status_listener = StatusListenerImpl::new();
        let mock_listener = MockTelemetryApiStatusListener::new();
        let binder_listener = BnTelemetryApiStatusListener::new_binder(
            mock_listener.clone(),
            BinderFeatures::default(),
        );
        status_listener.set_api_status_listener(&binder_listener).unwrap();

        status_listener.send_api_status(
            1,
            StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR,
            "Error message",
        );

        assert_eq!(
            mock_listener.get_last_call(),
            Some((1, StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR))
        );
    }

    #[test]
    fn send_api_status_without_listener_success() {
        let status_listener = StatusListenerImpl::new();
        status_listener.send_api_status(
            1,
            StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR,
            "Error message",
        );
    }

    #[test]
    fn send_misc_status_with_listener_success() {
        let status_listener = StatusListenerImpl::new();
        let mock_listener = MockTelemetryMiscStatusListener::new();
        let binder_listener = BnTelemetryMiscStatusListener::new_binder(
            mock_listener.clone(),
            BinderFeatures::default(),
        );
        status_listener.set_misc_status_listener(1, &binder_listener);

        status_listener
            .send_misc_status(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR, "Error message");

        assert_eq!(
            mock_listener.get_last_call(),
            Some(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)
        );
    }

    #[test]
    fn send_misc_status_without_listener_success() {
        let status_listener = StatusListenerImpl::new();
        status_listener
            .send_misc_status(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR, "Error message");

        let misc_errors = status_listener.misc_error_store.lock();
        assert_eq!(misc_errors.len(), 1);
        assert_eq!(misc_errors[0].0, StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR);
        assert_eq!(misc_errors[0].1, "Error message");
    }

    #[test]
    fn send_misc_status_overflow() {
        let status_listener = StatusListenerImpl::new();
        for i in 0..MAX_MISC_ERROR_STORE_SIZE + 1 {
            status_listener.send_misc_status(
                StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR,
                &format!("Error {}", i),
            );
        }
        let misc_errors = status_listener.misc_error_store.lock();
        assert_eq!(misc_errors.len(), MAX_MISC_ERROR_STORE_SIZE);

        // Check that the oldest error ("Error 0") was removed.
        assert!(!misc_errors.iter().any(|(_, msg)| msg == "Error 0"));
    }

    #[test]
    fn set_api_status_listener_twice_fails() {
        let status_listener = StatusListenerImpl::new();
        let binder_listener = BnTelemetryApiStatusListener::new_binder(
            MockTelemetryApiStatusListener::new(),
            BinderFeatures::default(),
        );
        status_listener.set_api_status_listener(&binder_listener).unwrap();

        let result = status_listener.set_api_status_listener(&binder_listener);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().exception_code(), binder::ExceptionCode::ILLEGAL_STATE);
    }

    #[test]
    fn set_misc_status_listener_twice_fails() {
        let status_listener = StatusListenerImpl::new();
        let mock_api_status_listener = MockTelemetryApiStatusListener::new();
        let binder_api_status_listener = BnTelemetryApiStatusListener::new_binder(
            mock_api_status_listener.clone(),
            BinderFeatures::default(),
        );
        let binder_misc_status_listener = BnTelemetryMiscStatusListener::new_binder(
            MockTelemetryMiscStatusListener::new(),
            BinderFeatures::default(),
        );

        status_listener.set_misc_status_listener(1, &binder_misc_status_listener);

        status_listener.set_api_status_listener(&binder_api_status_listener).unwrap();

        status_listener.set_misc_status_listener(2, &binder_misc_status_listener);

        // Check if the correct API status was sent for failure
        assert_eq!(
            mock_api_status_listener.get_last_call(),
            Some((2, StatusCodes::SET_MISC_STATUS_LISTENER_FAILED))
        );
    }

    #[test]
    fn set_misc_status_listener_sends_error_store() {
        let status_listener = StatusListenerImpl::new();
        status_listener.send_misc_status(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR, "Error");

        let mock_listener = MockTelemetryMiscStatusListener::new();
        let binder_listener = BnTelemetryMiscStatusListener::new_binder(
            mock_listener.clone(),
            BinderFeatures::default(),
        );

        status_listener.set_misc_status_listener(1, &binder_listener);

        assert_eq!(
            mock_listener.get_last_call(),
            Some(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)
        );
    }

    #[test]
    fn clear_api_status_listener_success() {
        let status_listener = StatusListenerImpl::new();
        let binder_listener = BnTelemetryApiStatusListener::new_binder(
            MockTelemetryApiStatusListener::new(),
            BinderFeatures::default(),
        );
        status_listener.set_api_status_listener(&binder_listener).unwrap();

        status_listener.clear_api_status_listener();

        assert!(status_listener.api_status_listener.lock().is_none());
    }

    #[test]
    fn clear_misc_status_listener_success() {
        let status_listener = StatusListenerImpl::new();
        let binder_listener = BnTelemetryMiscStatusListener::new_binder(
            MockTelemetryMiscStatusListener::new(),
            BinderFeatures::default(),
        );
        status_listener.set_misc_status_listener(1, &binder_listener);

        status_listener.clear_misc_status_listener();

        assert!(status_listener.misc_status_listener.lock().is_none());
    }
}
