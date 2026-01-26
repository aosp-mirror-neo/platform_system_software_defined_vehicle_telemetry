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

#![cfg(any(test, feature = "test_utils"))]

use futures::future::BoxFuture;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use protobuf::MessageFull;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_proto_rust::expression::combination_node::{
    ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
    RoundingOperator,
};
use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};
use sdv_telemetry_utils::operation_result::OperationResult;
use std::collections::HashMap;
use std::sync::Arc;

use crate::dynamic_message::{DynamicMessage, ProtobufConversionError};
use crate::node_error::{DescriptorProviderError, MessageProviderError, NodeEvaluationError};
use crate::node_factory::{DescriptorProvider, MessageProvider};
use crate::node_value::NodeValue;

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
    pub static ref OP_ABSOLUTE: OperatorProto =
        OperatorProto::ArithmeticOperator(ArithmeticOperator::ABSOLUTE.into());
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
    pub static ref TEST_DYNAMIC_MESSAGE: Arc<MockDynamicMessage> = {
        let mut test_message = MockDynamicMessage::new(TestMessage::descriptor());
        // initialize scalar (optional) fields
        test_message.set_field_value("bool_field".to_owned(), NodeValue::Bool(true));
        test_message.set_field_value("int_field".to_owned(), NodeValue::Int32(80));
        test_message.set_field_value("float_field".to_owned(), NodeValue::Float(100.0));
        test_message.set_field_value("string_field".to_owned(), NodeValue::String("Hello world".to_owned()));
        test_message.set_field_value("bytes_field".to_owned(), NodeValue::ByteList("Hello world".as_bytes().to_vec()));

        // initialize nested field
        let mut test_nested_message = MockDynamicMessage::new(TestNestedMessage::descriptor());
        test_nested_message.set_field_value("bool_field".to_owned(), NodeValue::Bool(true));
        test_nested_message.set_field_value("repeated_i32s".to_owned(), NodeValue::Int32List(vec![1, 2]));
        let test_nested_message = Arc::new(test_nested_message);
        test_message.set_field_value("message_field".to_owned(), NodeValue::Message(Arc::<MockDynamicMessage>::clone(&test_nested_message)));

        // initialize repeated fields
        test_message.set_field_value("repeated_bools".to_owned(), NodeValue::BoolList(vec![true, true, false, true]));
        test_message.set_field_value("repeated_i64s".to_owned(), NodeValue::Int64List(vec![3, 4, 5]));
        test_message.set_field_value("repeated_doubles".to_owned(), NodeValue::DoubleList(vec![std::f64::consts::PI, std::f64::consts::TAU, std::f64::consts::E]));
        test_message.set_field_value("repeated_messages".to_owned(), NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&test_nested_message), test_nested_message]));

        Arc::new(test_message)
    };
}

#[derive(Debug)]
pub struct MockDynamicMessage {
    descriptor: MessageDescriptor,
    field_values: HashMap<String, NodeValue>,
    next_error: Mutex<Option<NodeEvaluationError>>,
}

impl MockDynamicMessage {
    pub fn new(descriptor: MessageDescriptor) -> Self {
        Self { descriptor, field_values: HashMap::new(), next_error: None.into() }
    }

    pub fn set_field_value(&mut self, field_name: String, field_value: NodeValue) {
        assert!(
            self.descriptor.field_by_name(&field_name).is_some(),
            "Unknown field: {field_name}"
        );
        self.field_values.insert(field_name, field_value);
    }

    pub fn set_next_error(&self, error: NodeEvaluationError) {
        self.next_error.lock().replace(error);
    }
}

impl DynamicMessage for MockDynamicMessage {
    fn get_descriptor(&self) -> &MessageDescriptor {
        &self.descriptor
    }

    fn get_field_value(&self, field_name: &str) -> Result<NodeValue, NodeEvaluationError> {
        if let Some(error) = self.next_error.lock().take() {
            return Err(error);
        }

        Ok(self
            .field_values
            .get(field_name)
            .map_or_else(|| NodeValue::None, |node_value| node_value.clone()))
    }

    fn to_protobuf(&self) -> Result<Arc<dyn MessageDyn>, ProtobufConversionError> {
        panic!("to_protobuf should not be called in tests");
    }
}

pub struct MockMessageProvider {
    next_result: Mutex<
        Option<OperationResult<'static, Option<Arc<dyn DynamicMessage>>, MessageProviderError>>,
    >,
    return_default_message: bool,
}

impl Default for MockMessageProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MockMessageProvider {
    pub fn new() -> Self {
        Self { next_result: None.into(), return_default_message: false }
    }

    pub fn with_default_message() -> Self {
        let mut res = Self::new();
        res.return_default_message = true;
        res
    }

    pub fn set_next_async_result(
        &self,
        future: BoxFuture<'static, Result<Option<Arc<dyn DynamicMessage>>, MessageProviderError>>,
    ) {
        self.set_next_result(OperationResult::Asynchronous(future));
    }

    pub fn set_next_sync_result(
        &self,
        result: Result<Option<Arc<dyn DynamicMessage>>, MessageProviderError>,
    ) {
        self.set_next_result(OperationResult::Synchronous(result));
    }

    fn set_next_result(
        &self,
        result: OperationResult<'static, Option<Arc<dyn DynamicMessage>>, MessageProviderError>,
    ) {
        self.next_result.lock().replace(result);
    }
}

impl MessageProvider for MockMessageProvider {
    fn get_message(
        &self,
        _publisher_name: &str,
    ) -> OperationResult<'static, Option<Arc<dyn DynamicMessage>>, MessageProviderError> {
        self.next_result.lock().take().unwrap_or_else(|| {
            OperationResult::Synchronous(if self.return_default_message {
                Ok(Some(Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)))
            } else {
                Err(MessageProviderError::InvalidPublisherName)
            })
        })
    }
}

pub struct MockDescriptorProvider {
    /// If set, the function will be used to return an error.
    pub error_provider: Option<Box<dyn Fn() -> DescriptorProviderError + Send + Sync>>,

    /// Known message descriptors.
    pub descriptors: HashMap<String, MessageDescriptor>,
}

impl Default for MockDescriptorProvider {
    fn default() -> Self {
        Self::new()
    }
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
