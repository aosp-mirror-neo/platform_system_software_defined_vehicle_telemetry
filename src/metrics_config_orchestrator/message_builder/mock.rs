// Copyright 2023 Google LLC

use super::*;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use protobuf::MessageDyn;
use sdv_telemetry_test_proto::test_message::TestMessage;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::sync::Arc;

use crate::expr::node::NodeEvaluationContext;

/// The builder will build a TestMessage defined in `src/test_utils/test_message.proto`
pub(crate) struct MockMessageBuilder {
    next_error: Mutex<Option<MessageBuilderError>>,
    next_async_result:
        Mutex<Option<BoxFuture<'static, Result<Box<dyn MessageDyn>, MessageBuilderError>>>>,
    result_callback: Mutex<
        Option<
            Box<
                dyn Fn(
                        &NodeEvaluationContext,
                    )
                        -> OperationResult<'static, Box<dyn MessageDyn>, MessageBuilderError>
                    + Send
                    + Sync,
            >,
        >,
    >,
}

impl MessageBuilder for MockMessageBuilder {
    fn build(
        self: Arc<Self>,
        context: &Arc<NodeEvaluationContext>,
    ) -> OperationResult<'static, Box<dyn MessageDyn>, MessageBuilderError> {
        if let Some(error) = self.next_error.lock().take() {
            return Err(error).into();
        }

        if let Some(async_result) = self.next_async_result.lock().take() {
            async_result.into()
        } else if let Some(result_callback) = self.result_callback.lock().as_ref() {
            result_callback(context)
        } else {
            OperationResult::Synchronous(Ok(Box::new(TestMessage {
                int_field: 1,
                ..Default::default()
            })))
        }
    }
}

impl MockMessageBuilder {
    pub(crate) fn new() -> Self {
        Self {
            next_error: None.into(),
            next_async_result: None.into(),
            result_callback: None.into(),
        }
    }

    pub(crate) fn set_next_error(&self, error: MessageBuilderError) {
        self.next_error.lock().replace(error);
    }

    pub fn set_next_async_result(
        &self,
        future: BoxFuture<'static, Result<Box<dyn MessageDyn>, MessageBuilderError>>,
    ) {
        self.next_async_result.lock().replace(future);
    }

    pub(crate) fn set_result_callback(
        &self,
        callback: impl Fn(
                &NodeEvaluationContext,
            ) -> OperationResult<'static, Box<dyn MessageDyn>, MessageBuilderError>
            + Send
            + Sync
            + 'static,
    ) {
        self.result_callback.lock().replace(Box::new(callback));
    }
}
