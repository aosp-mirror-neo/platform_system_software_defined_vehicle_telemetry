use lazy_static::lazy_static;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
use sdvtelemetry_proto_rust::expression::combination_node::{
    ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
    RoundingOperator,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::expr::ast::{DescriptorProvider, MessageProvider};
use crate::expr::node_errors::{DescriptorProviderError, MessageProviderError};

// Common constants to be reused across tests.

// Use static variable to store the protobuf message that will be returned by the
// `provide_test_message` function, which is an argument to `evaluate()`.
// Cannot use local variable because Rust compiler complains that the proto msg
// returned by the closure does not live long enough, so we must give it a
// static lifetime to ensure it outlives evaluate().
// Lazy static is used because Node::new() does not return a 'static lifetime, so its
// return value cannot be stored in a static variable. Hence we use lazy_static
lazy_static! {
    pub static ref OP_AND: OperatorProto =
        OperatorProto::LogicalOperator(LogicalOperator::AND.into());
    pub static ref OP_OR: OperatorProto =
        OperatorProto::LogicalOperator(LogicalOperator::OR.into());
    pub static ref OP_XOR: OperatorProto =
        OperatorProto::LogicalOperator(LogicalOperator::XOR.into());
    pub static ref OP_NOT: OperatorProto =
        OperatorProto::LogicalOperator(LogicalOperator::NOT.into());
    pub static ref OP_ADD: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::ADD.into());
    pub static ref OP_SUBTRACT: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::SUBTRACT.into());
    pub static ref OP_MULTIPLY: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::MULTIPLY.into());
    pub static ref OP_DIVIDE: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::DIVIDE.into());
    pub static ref OP_MODULO_TRUNC: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::MODULO_TRUNC.into());
    pub static ref OP_POWER: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::POWER.into());
    pub static ref OP_UNARY_MINUS: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::UNARY_MINUS.into());
    pub static ref OP_EQ: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::EQ.into());
    pub static ref OP_NOT_EQ: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::NOT_EQ.into());
    pub static ref OP_GT: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::GT.into());
    pub static ref OP_GT_OR_EQ: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::GT_OR_EQ.into());
    pub static ref OP_LT: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::LT.into());
    pub static ref OP_LT_OR_EQ: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::LT_OR_EQ.into());
    pub static ref OP_CONTAINS: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::CONTAINS.into());
    pub static ref OP_DOES_NOT_CONTAIN: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::DOES_NOT_CONTAIN.into());
    pub static ref OP_ALL_EQ: OperatorProto =
        OperatorProto::RelationalOperator(RelationalOperator::ALL_EQ.into());
    pub static ref OP_FLOOR: OperatorProto =
        OperatorProto::RoundingOperator(RoundingOperator::FLOOR.into());
    pub static ref OP_ROUND: OperatorProto =
        OperatorProto::RoundingOperator(RoundingOperator::ROUND.into());
    pub static ref OP_CEIL: OperatorProto =
        OperatorProto::RoundingOperator(RoundingOperator::CEIL.into());
    pub static ref TEST_MESSAGE: TestMessage = {
        let mut test_message = TestMessage::new();
        // initialize scalar (optional) fields
        test_message.bool_field = true;
        test_message.int_field = 80;
        test_message.float_field = 100.0;
        test_message.string_field = "Hello world".to_string();
        test_message.enum_field = TestEnum::SOME.into();
        test_message.bytes_field = "Hello world".as_bytes().to_vec();

        // initialize nested field
        let mut test_nested_message = TestNestedMessage::new();
        test_nested_message.bool_field = true;
        test_nested_message.repeated_i32s = vec![1, 2];
        test_message.message_field = protobuf::MessageField::some(test_nested_message.clone());

        // initialize repeated fields
        test_message.repeated_bools = vec![true, true, false, true];
        test_message.repeated_i64s = vec![3, 4, 5];
        test_message.repeated_doubles = vec![
            std::f64::consts::PI, std::f64::consts::TAU, std::f64::consts::E];
        test_message.repeated_enums = vec![
            TestEnum::SOME.into(), TestEnum::SOME.into(), TestEnum::NONE.into()];
        test_message.repeated_messages = vec![
            test_nested_message.clone(), test_nested_message];

        test_message
    };
    pub static ref EMPTY_TEST_MESSAGE: TestMessage = TestMessage::new();
    pub static ref MOCK_MESSAGE_PROVIDER: Arc<MockMessageProvider> = Arc::new(MockMessageProvider::new());
}

pub struct MockMessageProvider {
    pub next_result: Mutex<Option<Result<Option<Box<dyn MessageDyn>>, MessageProviderError>>>,
}

impl MockMessageProvider {
    pub fn new() -> Self {
        Self { next_result: None.into() }
    }

    pub fn set_next_result(
        &self,
        result: Result<Option<Box<dyn MessageDyn>>, MessageProviderError>,
    ) {
        self.next_result.lock().unwrap().replace(result);
    }
}

impl MessageProvider for MockMessageProvider {
    fn get_message(
        &self,
        _publisher_name: &str,
    ) -> Result<Option<Box<dyn MessageDyn>>, MessageProviderError> {
        if let Some(result) = self.next_result.lock().unwrap().take() {
            result
        } else {
            Ok(Some(Box::new(TEST_MESSAGE.clone())))
        }
    }
}

pub struct MockDescriptorProvider {
    /// If set, the function will be used to return an error.
    pub error_provider: Option<Box<dyn Fn() -> DescriptorProviderError + Send + Sync>>,

    /// Known message descriptors.
    pub descriptors: HashMap<String, MessageDescriptor>,
}

impl MockDescriptorProvider {
    pub fn new() -> Self {
        Self { error_provider: None, descriptors: HashMap::new() }
    }
}

impl DescriptorProvider for MockDescriptorProvider {
    fn get_descriptor(
        &self,
        publisher_name: &str,
    ) -> Result<Option<&MessageDescriptor>, DescriptorProviderError> {
        if let Some(error_provider) = &self.error_provider {
            Err(error_provider())
        } else if let Some(descriptor) = self.descriptors.get(publisher_name) {
            Ok(Some(descriptor))
        } else {
            Ok(None)
        }
    }
}
