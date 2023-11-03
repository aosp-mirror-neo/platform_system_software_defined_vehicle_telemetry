use super::{MessageBuilder, MessageBuilderError};
use protobuf::MessageDyn;
use sdv_telemetry_test_proto::test_message::TestMessage;

/// The builder will build a TestMessage defined in `src/test_utils/test_message.proto`
#[derive(Clone)]
pub(crate) struct MockMessageBuilder {}

impl MessageBuilder for MockMessageBuilder {
    fn build(&self) -> Result<Box<dyn MessageDyn>, MessageBuilderError> {
        let mut message = TestMessage::new();
        message.int_field = 1;
        Ok(Box::new(message))
    }
}

impl MockMessageBuilder {
    pub fn new() -> Self {
        MockMessageBuilder {}
    }
}
