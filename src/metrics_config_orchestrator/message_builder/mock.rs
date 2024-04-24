// Copyright 2023 Google LLC. All Rights Reserved.

use super::{MessageBuilder, MessageBuilderError};
use protobuf::MessageDyn;
use sdv_telemetry_test_proto::test_message::TestMessage;
use std::sync::{Arc, Mutex};

/// The builder will build a TestMessage defined in `src/test_utils/test_message.proto`
#[derive(Clone)]
pub(crate) struct MockMessageBuilder {
    next_error: Arc<Mutex<Option<MessageBuilderError>>>,
}

impl MessageBuilder for MockMessageBuilder {
    fn build(&self) -> Result<Box<dyn MessageDyn>, MessageBuilderError> {
        if let Some(error) = self.next_error.lock().unwrap().take() {
            return Err(error);
        }

        let mut message = TestMessage::new();
        message.int_field = 1;
        Ok(Box::new(message))
    }
}

impl MockMessageBuilder {
    pub(crate) fn new() -> Self {
        Self { next_error: Arc::new(Mutex::new(None)) }
    }

    pub(crate) fn set_next_error(&self, error: MessageBuilderError) {
        self.next_error.lock().unwrap().replace(error);
    }
}
