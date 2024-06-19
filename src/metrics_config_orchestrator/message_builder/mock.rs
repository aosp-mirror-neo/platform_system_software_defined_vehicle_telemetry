// Copyright 2023 Google LLC. All Rights Reserved.

use super::{MessageBuilder, MessageBuilderError};
use crate::expr::ast::NodeEvaluationContext;
use protobuf::MessageDyn;
use sdv_telemetry_test_proto::test_message::TestMessage;
use std::sync::{Arc, Mutex};

type MockResultCallbackBox = Box<
    dyn Fn(&NodeEvaluationContext) -> Result<Box<dyn MessageDyn>, MessageBuilderError>
        + Send
        + Sync,
>;

/// The builder will build a TestMessage defined in `src/test_utils/test_message.proto`
#[derive(Clone)]
pub(crate) struct MockMessageBuilder {
    next_error: Arc<Mutex<Option<MessageBuilderError>>>,
    result_callback: Arc<Mutex<Option<MockResultCallbackBox>>>,
}

impl MessageBuilder for MockMessageBuilder {
    fn build(
        &self,
        context: &NodeEvaluationContext,
    ) -> Result<Box<dyn MessageDyn>, MessageBuilderError> {
        if let Some(error) = self.next_error.lock().unwrap().take() {
            return Err(error);
        }

        if let Some(result_callback) = self.result_callback.lock().unwrap().as_ref() {
            result_callback(context)
        } else {
            Ok(Box::new(TestMessage { int_field: 1, ..Default::default() }))
        }
    }
}

impl MockMessageBuilder {
    pub(crate) fn new() -> Self {
        Self { next_error: Arc::new(None.into()), result_callback: Arc::new(None.into()) }
    }

    pub(crate) fn set_next_error(&self, error: MessageBuilderError) {
        self.next_error.lock().unwrap().replace(error);
    }

    pub(crate) fn set_result_callback(
        &self,
        callback: impl Fn(&NodeEvaluationContext) -> Result<Box<dyn MessageDyn>, MessageBuilderError>
            + Send
            + Sync
            + 'static,
    ) {
        self.result_callback.lock().unwrap().replace(Box::new(callback));
    }
}
