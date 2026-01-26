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

use protobuf::{
    reflect::FieldDescriptor, reflect::MessageDescriptor, well_known_types::any::Any, MessageFull,
};
use sdv_telemetry_expr::node::Node;
use sdv_telemetry_expr::node_value_type::{NodeValueType, NodeValueTypeExtension};
use sdv_telemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::Aggregated_field_value as AggregatedFieldValueProto;
use std::sync::Arc;

use crate::metrics_config_orchestrator::message_builder::value_aggregator::{
    avg_aggregator::AvgAggregator, count_aggregator::CountAggregator,
    delta_aggregator::DeltaAggregator, max_aggregator::MaxAggregator,
    min_aggregator::MinAggregator, pass_through_aggregator::PassThroughAggregator,
    std_dev_aggregator::StdDevAggregator, sum_aggregator::SumAggregator,
    vector_aggregator::VectorAggregator,
};
use crate::metrics_config_orchestrator::message_builder::ValueAggregator;

#[derive(thiserror::Error, Debug)]
pub(crate) enum ValueAggregatorFactoryError {
    #[error(
        "Cannot assign values to unsupported protobuf field types, such as strings, bytes, maps"
    )]
    UnsupportedFieldType,
    #[error("Expression node index is not specified. Nothing to aggregate.")]
    MissingExpressionNodeIndex,
    #[error("Expression node index {index} out of bound. Nodes have size {size}")]
    IndexOutOfBound { index: u32, size: usize },
    #[error("Node output type and aggregation type are not compatible: {msg}")]
    IncompatibleAggregation { msg: String },
    #[error("Unrecognized aggregation type: {msg}")]
    UnknownAggregationType { msg: String },
    #[error(
        "Cannot assign aggregated output to field, field expects {field_type}, \
        but actual output type is {aggregated_type}"
    )]
    IncompatibleFieldAssignment { field_type: String, aggregated_type: String },
    #[error("Vector aggregation's max length must be greater than 0.")]
    InvalidMaxLengthForVectorAggregation,
}

pub(crate) trait ValueAggregatorFactory: Send + Sync {
    fn create(
        &self,
        aggregated_field_value: &AggregatedFieldValueProto,
        context: &ValueAggregatorFactoryCreateContext,
    ) -> Result<(Box<dyn ValueAggregator>, Option<Arc<Node>>), ValueAggregatorFactoryError>;

    fn validate(
        &self,
        aggregated_field_value: &AggregatedFieldValueProto,
        context: &ValueAggregatorFactoryValidateContext,
    ) -> Result<(), ValueAggregatorFactoryError>;
}

pub(crate) struct ValueAggregatorFactoryCreateContext<'a> {
    pub field_descriptor: &'a FieldDescriptor,
    pub node_types: &'a [Option<NodeValueType>],
    pub nodes: &'a [Arc<Node>],
}

pub(crate) struct ValueAggregatorFactoryValidateContext<'a> {
    pub field_descriptor: &'a FieldDescriptor,
    pub node_types: &'a [Option<NodeValueType>],
}

pub(crate) struct ValueAggregatorFactoryImpl {}

impl ValueAggregatorFactory for ValueAggregatorFactoryImpl {
    fn create(
        &self,
        aggregated_field_value: &AggregatedFieldValueProto,
        context: &ValueAggregatorFactoryCreateContext,
    ) -> Result<(Box<dyn ValueAggregator>, Option<Arc<Node>>), ValueAggregatorFactoryError> {
        Self::is_field_assignment_valid(
            context.field_descriptor,
            aggregated_field_value,
            context.node_types,
        )?;
        let get_node_func = |i: Option<u32>| Some(Arc::clone(&context.nodes[i.unwrap() as usize]));
        match aggregated_field_value {
            AggregatedFieldValueProto::NoAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(PassThroughAggregator {}), expression_node))
            }
            AggregatedFieldValueProto::VectorAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(VectorAggregator::new(aggr_proto.max_length)), expression_node))
            }
            AggregatedFieldValueProto::MaxAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(MaxAggregator::new()), expression_node))
            }
            AggregatedFieldValueProto::MinAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(MinAggregator::new()), expression_node))
            }
            AggregatedFieldValueProto::AvgAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(AvgAggregator::new()), expression_node))
            }
            AggregatedFieldValueProto::SumAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(SumAggregator::new()), expression_node))
            }
            AggregatedFieldValueProto::StdDevAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(StdDevAggregator::new()), expression_node))
            }
            AggregatedFieldValueProto::DeltaAggregation(aggr_proto) => {
                let expression_node = get_node_func(aggr_proto.expression_node_index);
                Ok((Box::new(DeltaAggregator::new()), expression_node))
            }
            AggregatedFieldValueProto::CountAggregation(_) => {
                Ok((Box::new(CountAggregator::new()), None))
            }
            _ => Err(ValueAggregatorFactoryError::UnknownAggregationType {
                msg: format!("{aggregated_field_value:?}"),
            }),
        }
    }

    fn validate(
        &self,
        aggregated_field_value: &AggregatedFieldValueProto,
        context: &ValueAggregatorFactoryValidateContext,
    ) -> Result<(), ValueAggregatorFactoryError> {
        Self::is_field_assignment_valid(
            context.field_descriptor,
            aggregated_field_value,
            context.node_types,
        )?;
        Ok(())
    }
}

impl ValueAggregatorFactoryImpl {
    // Checks whether the aggregated output can be assigned to the field that is
    // described by the FieldDescriptor.
    fn is_field_assignment_valid(
        field_descriptor: &FieldDescriptor,
        aggregated_field_value: &AggregatedFieldValueProto,
        node_types: &[Option<NodeValueType>],
    ) -> Result<(), ValueAggregatorFactoryError> {
        // validate this field descriptor is supported (no strings, maps, bytes)
        // and get the type of the field
        let field_type = field_descriptor
            .runtime_field_type()
            .get_node_value_type()
            .map_err(|_| ValueAggregatorFactoryError::UnsupportedFieldType)?;

        // get aggregated value type
        let value_type = aggregated_field_value.get_node_value_type(node_types)?;

        // check value type for compatibility if it's available
        if value_type.as_ref().is_none_or(|value_type| Self::is_compatible(&field_type, value_type))
        {
            Ok(())
        } else {
            Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment {
                field_type: format!("{field_type:?}"),
                aggregated_type: format!("{value_type:?}"),
            })
        }
    }

    fn is_compatible(expected: &NodeValueType, actual: &NodeValueType) -> bool {
        match (expected, actual) {
            // MessageBuilder can convert integers to floating points, so assigning an
            // integer to a float field is valid. But the reverse is not true.
            (NodeValueType::Float, NodeValueType::Int)
            | (NodeValueType::FloatList, NodeValueType::IntList) => true,

            (
                NodeValueType::Message { descriptor: expected_descriptor },
                NodeValueType::Message { descriptor: actual_descriptor },
            )
            | (
                NodeValueType::MessageList { descriptor: expected_descriptor },
                NodeValueType::MessageList { descriptor: actual_descriptor },
            ) => match (expected_descriptor, actual_descriptor) {
                (_, None) => {
                    // If the actual descriptor is not known yet, it's valid to assign it to any
                    // message field.
                    true
                }
                (Some(expected_descriptor), Some(actual_descriptor)) => {
                    // Either the message descriptors must be equal...
                    Self::is_message_descriptor_equal(expected_descriptor, actual_descriptor)
                    // ... or the expected descriptor must be Any, which allows assignment of any
                    // actual descriptor.
                    || Self::is_message_descriptor_equal(expected_descriptor, &Any::descriptor())
                }
                (None, Some(_)) => false,
            },

            _ => expected == actual,
        }
    }

    fn is_message_descriptor_equal(a: &MessageDescriptor, b: &MessageDescriptor) -> bool {
        // We want to ensure that both `MessageDescriptor`s describe the same message. We MUST NOT
        // compare the `MessageDescriptor`s directly, because they are only equal if the message is
        // defined in the exact same file and position within that file.
        //
        // Instead, we need to compare the `DescriptorProto`s (returned by
        // `MessageDescriptor::proto`), which describe the actual message formats, irrespective of
        // the file they are in, as well as the "full name" (= package name + message name).
        a.proto() == b.proto() && a.full_name() == b.full_name()
    }
}

impl<'a> ValueAggregatorFactoryCreateContext<'a> {
    /// Constructor for the context used by factory's create() method
    pub(crate) fn new(
        field_descriptor: &'a FieldDescriptor,
        node_types: &'a [Option<NodeValueType>],
        nodes: &'a [Arc<Node>],
    ) -> Self {
        Self { field_descriptor, node_types, nodes }
    }
}

impl<'a> ValueAggregatorFactoryValidateContext<'a> {
    /// Constructor for the context used by factory's validate() method
    pub(crate) fn new(
        field_descriptor: &'a FieldDescriptor,
        node_types: &'a [Option<NodeValueType>],
    ) -> Self {
        Self { field_descriptor, node_types }
    }
}

trait AggregatedFieldValueExtension {
    /// Get aggregation output's type.
    fn get_node_value_type(
        &self,
        node_types: &[Option<NodeValueType>],
    ) -> Result<Option<NodeValueType>, ValueAggregatorFactoryError>;
}

impl AggregatedFieldValueExtension for AggregatedFieldValueProto {
    fn get_node_value_type(
        &self,
        node_types: &[Option<NodeValueType>],
    ) -> Result<Option<NodeValueType>, ValueAggregatorFactoryError> {
        let res = match self {
            AggregatedFieldValueProto::NoAggregation(aggr_proto) => {
                get_node_type(node_types, aggr_proto.expression_node_index)?.clone()
            }
            AggregatedFieldValueProto::VectorAggregation(aggr_proto) => {
                if let Some(max_len) = &aggr_proto.max_length {
                    if *max_len == 0 {
                        return Err(
                            ValueAggregatorFactoryError::InvalidMaxLengthForVectorAggregation,
                        );
                    }
                }

                let node_type = get_node_type(node_types, aggr_proto.expression_node_index)?;
                if let Some(node_type) = node_type {
                    // after vector aggregation, it will be a list type
                    let aggregated_type = match node_type {
                        NodeValueType::Bool => NodeValueType::BoolList,
                        NodeValueType::Int => NodeValueType::IntList,
                        NodeValueType::Float => NodeValueType::FloatList,
                        NodeValueType::Message { descriptor } => {
                            NodeValueType::MessageList { descriptor: descriptor.clone() }
                        }
                        _ => {
                            return Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                                msg: format!(
                                "Cannot apply vector aggregation on node output type {node_type:?}"
                            ),
                            })
                        }
                    };
                    Some(aggregated_type)
                } else {
                    // don't validate if the node type is not available
                    None
                }
            }
            AggregatedFieldValueProto::MaxAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, aggr_proto.expression_node_index)?;
                if node_type.as_ref().is_none_or(|t| t.is_numeric()) {
                    node_type.clone()
                } else {
                    return Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply max aggregation on node output type {node_type:?}"
                        ),
                    });
                }
            }
            AggregatedFieldValueProto::MinAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, aggr_proto.expression_node_index)?;
                if node_type.as_ref().is_none_or(|t| t.is_numeric()) {
                    node_type.clone()
                } else {
                    return Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply min aggregation on node output type {node_type:?}"
                        ),
                    });
                }
            }
            AggregatedFieldValueProto::AvgAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, aggr_proto.expression_node_index)?;
                if node_type.as_ref().is_none_or(|t| t.is_numeric()) {
                    Some(NodeValueType::Float)
                } else {
                    return Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply avg aggregation on node output type {node_type:?}"
                        ),
                    });
                }
            }
            AggregatedFieldValueProto::SumAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, aggr_proto.expression_node_index)?;
                if node_type.as_ref().is_none_or(|t| t.is_numeric()) {
                    node_type.clone()
                } else {
                    return Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply sum aggregation on node output type {node_type:?}"
                        ),
                    });
                }
            }
            AggregatedFieldValueProto::StdDevAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, aggr_proto.expression_node_index)?;
                if node_type.as_ref().is_none_or(|t| t.is_numeric()) {
                    Some(NodeValueType::Float)
                } else {
                    return Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply standard deviation aggregation on node output type {node_type:?}"
                        ),
                    });
                }
            }
            AggregatedFieldValueProto::DeltaAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, aggr_proto.expression_node_index)?;
                if node_type.as_ref().is_none_or(|t| t.is_numeric()) {
                    node_type.clone()
                } else {
                    return Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply delta aggregation on node output type {node_type:?}"
                        ),
                    });
                }
            }
            AggregatedFieldValueProto::CountAggregation(_) => Some(NodeValueType::Int),
            _ => {
                return Err(ValueAggregatorFactoryError::UnknownAggregationType {
                    msg: format!("{self:?}"),
                })
            }
        };

        Ok(res)
    }
}

fn get_node_type(
    node_types: &[Option<NodeValueType>],
    index: Option<u32>,
) -> Result<&Option<NodeValueType>, ValueAggregatorFactoryError> {
    let index = index.ok_or(ValueAggregatorFactoryError::MissingExpressionNodeIndex)?;
    node_types
        .get(index as usize)
        .ok_or(ValueAggregatorFactoryError::IndexOutOfBound { index, size: node_types.len() })
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::reflect::FileDescriptor;
    use protobuf::well_known_types::{timestamp::Timestamp, wrappers::BoolValue};
    use std::sync::LazyLock;
    use protobuf::Message;
    use sdv_telemetry_expr::node_value::NodeValue;
    use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};
    use sdv_telemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::{
        AvgAggregation, CountAggregation, DeltaAggregation, MaxAggregation, MinAggregation,
        NoAggregation, StdDevAggregation, SumAggregation, VectorAggregation,
    };
    use sdv_telemetry_test_proto_bool_value::{
        test_bool_value_1::BoolValue as TestBoolValue1,
        test_bool_value_2::BoolValue as TestBoolValue2,
    };
    use googletest::matchers::eq;
    use googletest::expect_that;
    use sdv_telemetry_test_utils::parameterized_test;

    // See all valid field names in src/utils/test_utils/test_message.proto
    const ANY_FIELD_NAME: &str = "any_field";
    const ANY_LIST_FIELD_NAME: &str = "repeated_any";
    const MESSAGE_FIELD_NAME: &str = "message_field";
    const MESSAGE_LIST_FIELD_NAME: &str = "repeated_messages";
    const BOOL_FIELD_NAME: &str = "bool_field";
    const INT_FIELD_NAME: &str = "int_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
    const BOOL_LIST_FIELD_NAME: &str = "repeated_bools";
    const FLOAT_LIST_FIELD_NAME: &str = "repeated_doubles";
    const REPEATED_INT_FIELD_NAME: &str = "repeated_i64s";
    const STRING_FIELD_NAME: &str = "string_field";
    const BYTES_FIELD_NAME: &str = "bytes_field";

    fn get_field_descriptor_from_name(field_name: &str) -> FieldDescriptor {
        let msg_descriptor = TestMessage::descriptor();
        msg_descriptor.field_by_name(field_name).unwrap()
    }

    fn create_int_nodes_of_size(n: usize) -> (Vec<Arc<Node>>, Vec<Option<NodeValueType>>) {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(0) });
        let node_type = Some(NodeValueType::Int);
        (vec![node; n], vec![node_type; n])
    }

    fn create_float_nodes_of_size(n: usize) -> (Vec<Arc<Node>>, Vec<Option<NodeValueType>>) {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::Float(0.0) });
        let node_type = Some(NodeValueType::Float);
        (vec![node; n], vec![node_type; n])
    }

    fn create_bool_nodes_of_size(n: usize) -> (Vec<Arc<Node>>, Vec<Option<NodeValueType>>) {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(false) });
        let node_type = Some(NodeValueType::Bool);
        (vec![node; n], vec![node_type; n])
    }

    fn create_node_vec() -> Vec<Arc<Node>> {
        vec![Arc::new(Node::ConstantLeaf { value: NodeValue::None })]
    }

    fn create_no_aggregation_proto(idx: Option<u32>) -> NoAggregation {
        let mut aggr = NoAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_vector_aggregation_proto(
        idx: Option<u32>,
        max_len: Option<u32>,
    ) -> VectorAggregation {
        let mut aggr = VectorAggregation::new();
        aggr.expression_node_index = idx;
        aggr.max_length = max_len;
        aggr
    }

    fn create_avg_aggregation_proto(idx: Option<u32>) -> AvgAggregation {
        let mut aggr = AvgAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_delta_aggregation_proto(idx: Option<u32>) -> DeltaAggregation {
        let mut aggr = DeltaAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_min_aggregation_proto(idx: Option<u32>) -> MinAggregation {
        let mut aggr = MinAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_max_aggregation_proto(idx: Option<u32>) -> MaxAggregation {
        let mut aggr = MaxAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_sum_aggregation_proto(idx: Option<u32>) -> SumAggregation {
        let mut aggr = SumAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_std_dev_aggregation_proto(idx: Option<u32>) -> StdDevAggregation {
        let mut aggr = StdDevAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    parameterized_test!(
        #[test]
        validate,

        |field_descriptor_name, node_types, aggregation, result_matcher: Box<dyn FnOnce(&Result<_, _>) -> bool>| {
            let factory = ValueAggregatorFactoryImpl {};
            let field_descriptor = get_field_descriptor_from_name(field_descriptor_name);
            let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, node_types);

            let result = factory.validate(&aggregation, &context);

            assert!(result_matcher(&result), "Unexpected result: {result:?}");
        },

        no_aggregation_ok: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        vector_aggregation_ok: (
            REPEATED_INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        min_aggregation_ok: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::MinAggregation(create_min_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        max_aggregation_ok: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::MaxAggregation(create_max_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        avg_aggregation_ok: (
            FLOAT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::AvgAggregation(create_avg_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        std_dev_aggregation_ok: (
            FLOAT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::StdDevAggregation(create_std_dev_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        sum_aggregation_with_int_ok: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::SumAggregation(create_sum_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        sum_aggregation_with_float_ok: (
            FLOAT_FIELD_NAME,
            &[Some(NodeValueType::Float)],
            AggregatedFieldValueProto::SumAggregation(create_sum_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        delta_aggregation_with_int_ok: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::DeltaAggregation(create_delta_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        delta_aggregation_with_float_ok: (
            FLOAT_FIELD_NAME,
            &[Some(NodeValueType::Float)],
            AggregatedFieldValueProto::DeltaAggregation(create_delta_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        count_aggregation_ok: (
            INT_FIELD_NAME,
            &[],
            AggregatedFieldValueProto::CountAggregation(CountAggregation::new()),
            Box::new(Result::is_ok),
        ),
        bool_no_aggregation_ok: (
            BOOL_FIELD_NAME,
            &[Some(NodeValueType::Bool)],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        bool_vector_aggregation_ok: (
            BOOL_LIST_FIELD_NAME,
            &[Some(NodeValueType::Bool)],
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        node_output_is_int_but_field_is_float_ok: (
            FLOAT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_is_int_list_but_field_is_float_list_ok: (
            FLOAT_LIST_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),

        node_output_message_type_unknown_success: (
            MESSAGE_FIELD_NAME,
            &[Some(NodeValueType::Message { descriptor: None })],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_message_list_type_unknown_success: (
            MESSAGE_LIST_FIELD_NAME,
            &[Some(NodeValueType::Message { descriptor: None })],
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        node_output_message_type_any_success: (
            ANY_FIELD_NAME,
            &[Some(NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) })],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_message_list_type_any_success: (
            ANY_LIST_FIELD_NAME,
            &[Some(NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) })],
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        node_output_message_type_compatible_success: (
            MESSAGE_FIELD_NAME,
            &[Some(NodeValueType::Message { descriptor: Some(TestNestedMessage::descriptor()) })],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_message_list_type_compatible_success: (
            MESSAGE_LIST_FIELD_NAME,
            &[Some(NodeValueType::Message { descriptor: Some(TestNestedMessage::descriptor()) })],
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),

        node_output_message_type_string_success: (
            STRING_FIELD_NAME,
            &[Some(NodeValueType::String)],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_message_type_bytes_success: (
            BYTES_FIELD_NAME,
            &[Some(NodeValueType::ByteList)],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),

        node_index_not_set_fails: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            // Expression node index is `None`
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(None)),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::MissingExpressionNodeIndex))),
        ),
        node_index_out_of_bound_fails: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            // Requires node at index `10`
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(10))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IndexOutOfBound { .. }))),
        ),
        node_output_is_incompatible_with_field_fails: (
            FLOAT_FIELD_NAME,
            &[Some(NodeValueType::Bool)],
            AggregatedFieldValueProto::NoAggregation(
                // node at index 0 is bool type, but field type is float
                // cannot assign boolean to float field
                create_no_aggregation_proto(Some(0)),
            ),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
        node_output_message_type_incompatible_fails: (
            MESSAGE_FIELD_NAME,
            &[Some(NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) })],
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
        vector_aggregation_with_list_typed_node_fails: (
            REPEATED_INT_FIELD_NAME,
            &[Some(NodeValueType::IntList)],
            AggregatedFieldValueProto::VectorAggregation(
                create_vector_aggregation_proto(Some(0), None),
            ),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleAggregation { .. }))),
        ),
        vector_aggregation_with_zero_max_length_fails: (
            REPEATED_INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::VectorAggregation(
                create_vector_aggregation_proto(Some(0), Some(0)),
            ),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::InvalidMaxLengthForVectorAggregation))),
        ),
        avg_aggregation_incompatible_field_type_fails: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::AvgAggregation(create_avg_aggregation_proto(Some(0))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
        std_dev_aggregation_incompatible_field_type_fails: (
            INT_FIELD_NAME,
            &[Some(NodeValueType::Int)],
            AggregatedFieldValueProto::StdDevAggregation(create_std_dev_aggregation_proto(Some(0))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
    );

    parameterized_test!(
        #[test]
        create,

        |field_descriptor_name, nodes_and_types: (Vec<Arc<Node>>, Vec<Option<NodeValueType>>), aggregation, result_matcher: Box<dyn FnOnce(&Result<_, _>) -> bool>| {
            let (nodes, node_types) = nodes_and_types;
            let factory = ValueAggregatorFactoryImpl {};
            let field_descriptor = get_field_descriptor_from_name(field_descriptor_name);
            let context =
                ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);

            let result = factory.create(&aggregation, &context);

            assert!(result_matcher(&result), "Unexpected result: {result:?}");
        },

        no_aggregation_ok: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        vector_aggregation_ok: (
            REPEATED_INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        min_aggregation_ok: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::MinAggregation(create_min_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        max_aggregation_ok: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::MaxAggregation(create_max_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        avg_aggregation_ok: (
            FLOAT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::AvgAggregation(create_avg_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        std_dev_aggregation_ok: (
            FLOAT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::StdDevAggregation(create_std_dev_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        sum_aggregation_with_int_ok: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::SumAggregation(create_sum_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        sum_aggregation_with_float_ok: (
            FLOAT_FIELD_NAME,
            create_float_nodes_of_size(1),
            AggregatedFieldValueProto::SumAggregation(create_sum_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        delta_aggregation_with_int_ok: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::DeltaAggregation(create_delta_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        delta_aggregation_with_float_ok: (
            FLOAT_FIELD_NAME,
            create_float_nodes_of_size(1),
            AggregatedFieldValueProto::DeltaAggregation(create_delta_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        count_aggregation_ok: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(0),
            AggregatedFieldValueProto::CountAggregation(CountAggregation::new()),
            Box::new(Result::is_ok),
        ),
        bool_no_aggregation_ok: (
            BOOL_FIELD_NAME,
            create_bool_nodes_of_size(1),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        bool_vector_aggregation_ok: (
            BOOL_LIST_FIELD_NAME,
            create_bool_nodes_of_size(1),
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        node_output_is_int_but_field_is_float_ok: (
            FLOAT_FIELD_NAME,
            (vec![Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) })], vec![Some(NodeValueType::Int)]),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_is_int_list_but_field_is_float_list_ok: (
            FLOAT_LIST_FIELD_NAME,
            (vec![Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) })], vec![Some(NodeValueType::Int)]),
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),

        node_output_message_type_unknown_success: (
            MESSAGE_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Message { descriptor: None })]),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_message_type_list_unknown_success: (
            MESSAGE_LIST_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Message { descriptor: None })]),
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        node_output_message_type_any_success: (
            ANY_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) })]),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_message_list_type_any_success: (
            ANY_LIST_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) })]),
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),
        node_output_message_type_compatible_success: (
            MESSAGE_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Message {
                descriptor: Some(TestNestedMessage::descriptor()),
            })]),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_message_list_type_compatible_success: (
            MESSAGE_LIST_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Message {
                descriptor: Some(TestNestedMessage::descriptor()),
            })]),
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0), Some(1))),
            Box::new(Result::is_ok),
        ),

        node_output_type_string_success: (
            STRING_FIELD_NAME,
            (vec![Arc::new(Node::ConstantLeaf { value: NodeValue::None })], vec![Some(NodeValueType::String)]),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),
        node_output_type_bytes_success: (
            BYTES_FIELD_NAME,
            (vec![Arc::new(Node::ConstantLeaf { value: NodeValue::None })], vec![Some(NodeValueType::ByteList)]),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(Result::is_ok),
        ),

        node_index_not_set_fails: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            // Expression node index is `None`
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(None)),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::MissingExpressionNodeIndex))),
        ),
        node_index_out_of_bound_fails: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            // Requires node at index `10`
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(10))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IndexOutOfBound { .. }))),
        ),
        node_output_is_incompatible_with_field_fails: (
            FLOAT_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Bool)]),
            AggregatedFieldValueProto::NoAggregation(
                // node at index 0 is bool type, but field type is float
                // cannot assign boolean to float field
                create_no_aggregation_proto(Some(0)),
            ),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
        node_output_message_type_incompatible_fails: (
            MESSAGE_FIELD_NAME,
            (create_node_vec(), vec![Some(NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) })]),
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
        vector_aggregation_with_list_typed_node_fails: (
            REPEATED_INT_FIELD_NAME,
            (vec![Arc::new(Node::ConstantLeaf { value: NodeValue::Int32List(vec![99]) })], vec![Some(NodeValueType::IntList)]),
            AggregatedFieldValueProto::VectorAggregation(
                create_vector_aggregation_proto(Some(0), None),
            ),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleAggregation { .. }))),
        ),
        vector_aggregation_with_zero_max_length_fails: (
            REPEATED_INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::VectorAggregation(
                create_vector_aggregation_proto(Some(0), Some(0)),
            ),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::InvalidMaxLengthForVectorAggregation))),
        ),
        avg_aggregation_incompatible_field_type_fails: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::AvgAggregation(create_avg_aggregation_proto(Some(0))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
        std_dev_aggregation_incompatible_field_type_fails: (
            INT_FIELD_NAME,
            create_int_nodes_of_size(1),
            AggregatedFieldValueProto::StdDevAggregation(create_std_dev_aggregation_proto(Some(0))),
            Box::new(|res| matches!(res, Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }))),
        ),
    );

    // A copy of the `Any` message descriptor, but with slightly adjusted source code info of its
    // file descriptor.
    static ANY_DESCRIPTOR_CLONE: LazyLock<MessageDescriptor> = LazyLock::new(|| {
        let mut file_descriptor_proto = Any::descriptor().file_descriptor_proto().clone();
        file_descriptor_proto.source_code_info.as_mut().unwrap().location.push(
            protobuf::descriptor::source_code_info::Location {
                leading_comments: Some("bla bla".to_owned()),
                ..Default::default()
            },
        );

        let file_descriptor = FileDescriptor::new_dynamic(file_descriptor_proto, &[]).unwrap();
        file_descriptor.message_by_package_relative_name(Any::NAME).unwrap()
    });

    parameterized_test!(
        #[googletest::test]
        #[test]
        is_compatible,

        |base: &NodeValueType, actual: &NodeValueType, expected: bool| {
            let is_compatible = ValueAggregatorFactoryImpl::is_compatible(base, actual);

            expect_that!(is_compatible, eq(expected));
        },

        bool_bool: (&NodeValueType::Bool, &NodeValueType::Bool, true),
        int_int: (&NodeValueType::Int, &NodeValueType::Int, true),
        float_float: (&NodeValueType::Float, &NodeValueType::Float, true),
        string_string: (&NodeValueType::String, &NodeValueType::String, true),
        byte_list_byte_list: (&NodeValueType::ByteList, &NodeValueType::ByteList, true),
        bool_list_bool_list: (&NodeValueType::BoolList, &NodeValueType::BoolList, true),
        int_list_int_list: (&NodeValueType::IntList, &NodeValueType::IntList, true),
        float_list_float_list: (&NodeValueType::FloatList, &NodeValueType::FloatList, true),

        // Compatible numeric conversions
        float_int: (&NodeValueType::Float, &NodeValueType::Int, true),
        float_list_int_list: (&NodeValueType::FloatList, &NodeValueType::IntList, true),

        // Incompatible numeric conversions
        int_float: (&NodeValueType::Int, &NodeValueType::Float, false),
        int_list_float_list: (&NodeValueType::IntList, &NodeValueType::FloatList, false),

        // Incompatible mixed types
        int_bool: (&NodeValueType::Int, &NodeValueType::Bool, false),
        bool_int: (&NodeValueType::Bool, &NodeValueType::Int, false),
        float_string: (&NodeValueType::Float, &NodeValueType::String, false),
        string_float: (&NodeValueType::String, &NodeValueType::Float, false),
        bool_string: (&NodeValueType::Bool, &NodeValueType::String, false),
        string_bool: (&NodeValueType::String, &NodeValueType::Bool, false),

        // List vs. single
        int_list_int: (&NodeValueType::IntList, &NodeValueType::Int, false),
        int_int_list: (&NodeValueType::Int, &NodeValueType::IntList, false),
        float_list_float: (&NodeValueType::FloatList, &NodeValueType::Float, false),
        float_float_list: (&NodeValueType::Float, &NodeValueType::FloatList, false),
        bool_list_bool: (&NodeValueType::BoolList, &NodeValueType::Bool, false),
        bool_bool_list: (&NodeValueType::Bool, &NodeValueType::BoolList, false),
        message_message_list: (&NodeValueType::Message { descriptor: None }, &NodeValueType::MessageList { descriptor: None }, false),
        message_list_message: (&NodeValueType::MessageList { descriptor: None }, &NodeValueType::Message { descriptor: None }, false),

        // Message types
        message_message_none: (&NodeValueType::Message { descriptor: None }, &NodeValueType::Message { descriptor: None }, true),
        message_message_same: (&NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) }, &NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) }, true),
        message_message_different: (&NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) }, &NodeValueType::Message { descriptor: Some(TestMessage::descriptor()) }, false),

        // Check that assigning works, even if the messages are defined in different proto files, as
        // long as they are compatible.
        message_same_definition_but_different_file: (&NodeValueType::Message { descriptor: Some(TestBoolValue1::descriptor()) }, &NodeValueType::Message { descriptor: Some(TestBoolValue2::descriptor()) }, true),
        message_same_definition_but_different_file_different_package: (&NodeValueType::Message { descriptor: Some(BoolValue::descriptor()) }, &NodeValueType::Message { descriptor: Some(TestBoolValue2::descriptor()) }, false),

        message_any_message: (&NodeValueType::Message { descriptor: Some(Any::descriptor()) }, &NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) }, true),
        // You can assign whatever message you want to an Any message.
        message_any_message_any: (&NodeValueType::Message { descriptor: Some(Any::descriptor()) }, &NodeValueType::Message { descriptor: Some(Any::descriptor()) }, true),
        message_any_message_none: (&NodeValueType::Message { descriptor: Some(Any::descriptor()) }, &NodeValueType::Message { descriptor: None }, true),

        message_any_message2: (&NodeValueType::Message { descriptor: Some(ANY_DESCRIPTOR_CLONE.to_owned()) }, &NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) }, true),
        // You can assign whatever message you want to an Any message.
        message_any_message_any2: (&NodeValueType::Message { descriptor: Some(ANY_DESCRIPTOR_CLONE.to_owned()) }, &NodeValueType::Message { descriptor: Some(ANY_DESCRIPTOR_CLONE.to_owned()) }, true),
        message_any_message_none2: (&NodeValueType::Message { descriptor: Some(ANY_DESCRIPTOR_CLONE.to_owned()) }, &NodeValueType::Message { descriptor: None }, true),

        // You cannot assign Any to a field that is not Any itself.
        message_message_any2: (&NodeValueType::Message { descriptor: Some(Timestamp::descriptor()) }, &NodeValueType::Message { descriptor: Some(Any::descriptor()) }, false),

        message_any_message_any3: (&NodeValueType::Message { descriptor: Some(ANY_DESCRIPTOR_CLONE.to_owned()) }, &NodeValueType::Message { descriptor: Some(Any::descriptor()) }, true),
        message_any_message_any4: (&NodeValueType::Message { descriptor: Some(Any::descriptor()) }, &NodeValueType::Message { descriptor: Some(ANY_DESCRIPTOR_CLONE.to_owned()) }, true),

        message_list_message_list_none: (&NodeValueType::MessageList { descriptor: None }, &NodeValueType::MessageList { descriptor: None }, true),
        message_list_message_list_same: (&NodeValueType::MessageList { descriptor: Some(Timestamp::descriptor()) }, &NodeValueType::MessageList { descriptor: Some(Timestamp::descriptor()) }, true),
        message_list_message_list_different: (&NodeValueType::MessageList { descriptor: Some(Timestamp::descriptor()) }, &NodeValueType::MessageList { descriptor: Some(TestMessage::descriptor()) }, false),

        // Check that assigning works, even if the messages are defined in different proto files, as
        // long as they are compatible.
        message_list_same_definition_but_different_file: (&NodeValueType::MessageList { descriptor: Some(TestBoolValue1::descriptor()) }, &NodeValueType::MessageList { descriptor: Some(TestBoolValue2::descriptor()) }, true),
        message_list_same_definition_but_different_file_different_package: (&NodeValueType::MessageList { descriptor: Some(BoolValue::descriptor()) }, &NodeValueType::MessageList { descriptor: Some(TestBoolValue2::descriptor()) }, false),

        message_list_any_message_list: (&NodeValueType::MessageList { descriptor: Some(Any::descriptor()) }, &NodeValueType::MessageList { descriptor: Some(Timestamp::descriptor()) }, true),
        // You can assign whatever message you want to an Any message.
        message_list_any_message_list_any: (&NodeValueType::MessageList { descriptor: Some(Any::descriptor()) }, &NodeValueType::MessageList { descriptor: Some(Any::descriptor()) }, true),
        message_list_any_message_list_none: (&NodeValueType::MessageList { descriptor: Some(Any::descriptor()) }, &NodeValueType::MessageList { descriptor: None }, true),

        // You cannot assign Any to a field that is not Any itself.
        message_list_message_list_any: (&NodeValueType::MessageList { descriptor: Some(Timestamp::descriptor()) }, &NodeValueType::MessageList { descriptor: Some(Any::descriptor()) }, false),
    );
}
