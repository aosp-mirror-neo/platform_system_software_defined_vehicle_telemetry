// Copyright 2024 Google LLC

use protobuf::{reflect::FieldDescriptor, well_known_types::any::Any, MessageFull};
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
        expected == actual
            // MessageBuilder can convert integers to floating points, so assigning an
            // integer to a float field is valid. But the reverse is not true.
            || (matches!(expected, NodeValueType::Float) && matches!(actual, NodeValueType::Int))
            || (matches!(expected, NodeValueType::FloatList) && matches!(actual, NodeValueType::IntList))
            // If the actual type not known yet, it's valid to assign it to any message field.
            || (matches!(expected, NodeValueType::Message { .. }) && matches!(actual, NodeValueType::Message { descriptor } if descriptor.is_none()))
            || (matches!(expected, NodeValueType::MessageList { .. }) && matches!(actual, NodeValueType::MessageList { descriptor } if descriptor.is_none()))
            // If the expected type is protobuf Any, it's valid to assign any message field.
            || (matches!(expected, NodeValueType::Message { descriptor } if descriptor.as_ref().is_some_and(|d| d == &Any::descriptor())) && matches!(actual, NodeValueType::Message { .. }))
            || (matches!(expected, NodeValueType::MessageList { descriptor } if descriptor.as_ref().is_some_and(|d| d == &Any::descriptor())) && matches!(actual, NodeValueType::MessageList { .. }))
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

    use protobuf::well_known_types::timestamp::Timestamp;
    use sdv_telemetry_expr::node_value::NodeValue;
    use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};
    use sdv_telemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::{
        AvgAggregation, CountAggregation, DeltaAggregation, MaxAggregation, MinAggregation,
        NoAggregation, StdDevAggregation, SumAggregation, VectorAggregation,
    };
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
}
