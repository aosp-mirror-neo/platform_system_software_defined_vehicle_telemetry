use protobuf::reflect::FieldDescriptor;
use sdvtelemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::Aggregated_field_value as AggregatedFieldValueProto;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::{
    ast::Node,
    node_value_type::{NodeValueType, NodeValueTypeExtension},
};
use crate::metrics_config_orchestrator::message_builder::{
    value_aggregator::{
        avg_aggregator::AvgAggregator, count_aggregator::CountAggregator,
        max_aggregator::MaxAggregator, min_aggregator::MinAggregator,
        pass_through_aggregator::PassThroughAggregator, vector_aggregator::VectorAggregator,
    },
    ValueAggregator,
};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ValueAggregatorFactoryError {
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
}

pub trait ValueAggregatorFactory: Send + Sync {
    fn create(
        &self,
        aggregated_field_value: &AggregatedFieldValueProto,
        context: &ValueAggregatorFactoryCreateContext,
    ) -> Result<Box<dyn ValueAggregator>, ValueAggregatorFactoryError>;

    fn validate(
        &self,
        aggregated_field_value: &AggregatedFieldValueProto,
        context: &ValueAggregatorFactoryValidateContext,
    ) -> Result<(), ValueAggregatorFactoryError>;
}

pub struct ValueAggregatorFactoryCreateContext<'a> {
    pub field_descriptor: &'a FieldDescriptor,
    pub node_types: &'a [NodeValueType],
    pub nodes: &'a [Arc<Node>],
}

pub struct ValueAggregatorFactoryValidateContext<'a> {
    pub field_descriptor: &'a FieldDescriptor,
    pub node_types: &'a [NodeValueType],
}

pub struct ValueAggregatorFactoryImpl {}

impl ValueAggregatorFactory for ValueAggregatorFactoryImpl {
    fn create(
        &self,
        aggregated_field_value: &AggregatedFieldValueProto,
        context: &ValueAggregatorFactoryCreateContext,
    ) -> Result<Box<dyn ValueAggregator>, ValueAggregatorFactoryError> {
        Self::is_field_assignment_valid(
            context.field_descriptor,
            aggregated_field_value,
            context.node_types,
        )?;
        match aggregated_field_value {
            AggregatedFieldValueProto::NoAggregation(aggr_proto) => {
                Ok(Box::new(PassThroughAggregator::new(
                    &context.nodes[aggr_proto.expression_node_index.unwrap() as usize],
                )))
            }
            AggregatedFieldValueProto::VectorAggregation(aggr_proto) => {
                Ok(Box::new(VectorAggregator::new(
                    &context.nodes[aggr_proto.expression_node_index.unwrap() as usize],
                )))
            }
            AggregatedFieldValueProto::MaxAggregation(aggr_proto) => {
                Ok(Box::new(MaxAggregator::new(
                    &context.nodes[aggr_proto.expression_node_index.unwrap() as usize],
                )))
            }
            AggregatedFieldValueProto::MinAggregation(aggr_proto) => {
                Ok(Box::new(MinAggregator::new(
                    &context.nodes[aggr_proto.expression_node_index.unwrap() as usize],
                )))
            }
            AggregatedFieldValueProto::AvgAggregation(aggr_proto) => {
                Ok(Box::new(AvgAggregator::new(
                    &context.nodes[aggr_proto.expression_node_index.unwrap() as usize],
                )))
            }
            AggregatedFieldValueProto::CountAggregation(_) => Ok(Box::new(CountAggregator::new())),
            _ => Err(ValueAggregatorFactoryError::UnknownAggregationType {
                msg: format!("{:?}", aggregated_field_value),
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
        node_types: &[NodeValueType],
    ) -> Result<(), ValueAggregatorFactoryError> {
        // validate this field descriptor is supported (no strings, maps, bytes)
        // and get the type of the field
        let field_type = field_descriptor
            .runtime_field_type()
            .get_node_value_type()
            .map_err(|_| ValueAggregatorFactoryError::UnsupportedFieldType)?;

        // get aggregated value type
        let value_type = aggregated_field_value.get_node_value_type(node_types)?;

        if !Self::is_compatible(&field_type, &value_type) {
            return Err(ValueAggregatorFactoryError::IncompatibleFieldAssignment {
                field_type: format!("{:?}", field_type),
                aggregated_type: format!("{:?}", value_type),
            });
        }
        Ok(())
    }

    fn is_compatible(expected: &NodeValueType, actual: &NodeValueType) -> bool {
        // MessageBuilder can convert integers to floating points, so assigning an
        // integer to a float field is valid. But the reverse is not true.
        expected == actual
            || (matches!(expected, NodeValueType::Float) && matches!(actual, NodeValueType::Int))
    }
}

impl<'a> ValueAggregatorFactoryCreateContext<'a> {
    /// Constructor for the context used by factory's create() method
    pub(crate) fn new(
        field_descriptor: &'a FieldDescriptor,
        node_types: &'a [NodeValueType],
        nodes: &'a [Arc<Node>],
    ) -> Self {
        ValueAggregatorFactoryCreateContext { field_descriptor, node_types, nodes }
    }
}

impl<'a> ValueAggregatorFactoryValidateContext<'a> {
    /// Constructor for the context used by factory's validate() method
    pub(crate) fn new(
        field_descriptor: &'a FieldDescriptor,
        node_types: &'a [NodeValueType],
    ) -> Self {
        ValueAggregatorFactoryValidateContext { field_descriptor, node_types }
    }
}

trait AggregatedFieldValueExtension {
    /// Get aggregation output's type.
    fn get_node_value_type(
        &self,
        node_types: &[NodeValueType],
    ) -> Result<NodeValueType, ValueAggregatorFactoryError>;
}

impl AggregatedFieldValueExtension for AggregatedFieldValueProto {
    fn get_node_value_type(
        &self,
        node_types: &[NodeValueType],
    ) -> Result<NodeValueType, ValueAggregatorFactoryError> {
        match self {
            AggregatedFieldValueProto::NoAggregation(aggr_proto) => {
                Ok(get_node_type(node_types, &aggr_proto.expression_node_index)?.clone())
            }
            AggregatedFieldValueProto::VectorAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, &aggr_proto.expression_node_index)?;
                // after vector aggregation, it will be a list type
                match node_type {
                    NodeValueType::Bool => Ok(NodeValueType::BoolList),
                    NodeValueType::Int => Ok(NodeValueType::IntList),
                    NodeValueType::Float => Ok(NodeValueType::FloatList),
                    _ => Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply vector aggregation on node output type {:?}",
                            node_type
                        ),
                    }),
                }
            }
            AggregatedFieldValueProto::MaxAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, &aggr_proto.expression_node_index)?;
                if matches!(node_type, NodeValueType::Int | NodeValueType::Float) {
                    Ok(node_type.clone())
                } else {
                    Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply max aggregation on node output type {:?}",
                            node_type
                        ),
                    })
                }
            }
            AggregatedFieldValueProto::MinAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, &aggr_proto.expression_node_index)?;
                if matches!(node_type, NodeValueType::Int | NodeValueType::Float) {
                    Ok(node_type.clone())
                } else {
                    Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply max aggregation on node output type {:?}",
                            node_type
                        ),
                    })
                }
            }
            AggregatedFieldValueProto::AvgAggregation(aggr_proto) => {
                let node_type = get_node_type(node_types, &aggr_proto.expression_node_index)?;
                if matches!(node_type, NodeValueType::Int | NodeValueType::Float) {
                    Ok(NodeValueType::Float)
                } else {
                    Err(ValueAggregatorFactoryError::IncompatibleAggregation {
                        msg: format!(
                            "Cannot apply avg aggregation on node output type {:?}",
                            node_type
                        ),
                    })
                }
            }
            AggregatedFieldValueProto::CountAggregation(_) => Ok(NodeValueType::Int),
            _ => Err(ValueAggregatorFactoryError::UnknownAggregationType {
                msg: format!("{:?}", self),
            }),
        }
    }
}

fn get_node_type<'a>(
    node_types: &'a [NodeValueType],
    index: &'a Option<u32>,
) -> Result<&'a NodeValueType, ValueAggregatorFactoryError> {
    let index = index.ok_or(ValueAggregatorFactoryError::MissingExpressionNodeIndex)?;
    node_types
        .get(index as usize)
        .ok_or(ValueAggregatorFactoryError::IndexOutOfBound { index, size: node_types.len() })
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use protobuf::MessageFull;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdvtelemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::{
        AvgAggregation, NoAggregation, VectorAggregation,
    };

    use super::*;
    use crate::expr::node_value::NodeValue;

    // See all valid field names in src/utils/test_utils/test_message.proto
    const INT_FIELD_NAME: &str = "int_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
    const REPEATED_INT_FIELD_NAME: &str = "repeated_i64s";
    const STRING_FIELD_NAME: &str = "string_field";

    fn get_field_descriptor_from_name(field_name: &str) -> FieldDescriptor {
        let msg_descriptor = TestMessage::descriptor();
        msg_descriptor.field_by_name(field_name).unwrap()
    }

    fn create_int_nodes_of_size(n: usize) -> (Vec<Arc<Node>>, Vec<NodeValueType>) {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(0) });
        let node_type = NodeValueType::Int;
        (vec![node; n], vec![node_type; n])
    }

    fn create_no_aggregation_proto(idx: Option<u32>) -> NoAggregation {
        let mut aggr = NoAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_vector_aggregation_proto(idx: Option<u32>) -> VectorAggregation {
        let mut aggr = VectorAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    fn create_avg_aggregation_proto(idx: Option<u32>) -> AvgAggregation {
        let mut aggr = AvgAggregation::new();
        aggr.expression_node_index = idx;
        aggr
    }

    #[test]
    fn validate_no_aggregation_ok() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let node_types = vec![NodeValueType::Int];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0)));

        assert!(factory.validate(&aggregated_field_value, &context).is_ok());
    }

    #[test]
    fn validate_no_aggregation_node_index_not_set_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let node_types = vec![NodeValueType::Int];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        // expression node index is none
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(None));

        let err = factory.validate(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::MissingExpressionNodeIndex),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_no_aggregation_node_index_out_of_bound_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let node_types = vec![NodeValueType::Int];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        // requires node at index 10
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(10)));

        let err = factory.validate(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IndexOutOfBound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_field_descriptor_has_unsupported_type_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(STRING_FIELD_NAME);
        let node_types = vec![NodeValueType::Int];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0)));

        let err = factory.validate(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::UnsupportedFieldType { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_node_output_is_incompatible_with_field_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(FLOAT_FIELD_NAME);
        let node_types = vec![NodeValueType::Bool];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        let aggregated_field_value = AggregatedFieldValueProto::NoAggregation(
            // node at index 0 is Bool type, but field type is float
            // cannot assign boolean to float field
            create_no_aggregation_proto(Some(0)),
        );

        let err = factory.validate(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_node_output_is_int_but_field_is_float_ok() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(FLOAT_FIELD_NAME);
        let node_types = vec![NodeValueType::Int];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0)));

        assert!(factory.validate(&aggregated_field_value, &context).is_ok());
    }

    #[test]
    fn validate_vector_aggregation_with_list_typed_node_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(REPEATED_INT_FIELD_NAME);
        let node_types = vec![NodeValueType::IntList];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        // cannot apply vector aggregation on IntList types
        let aggregated_field_value =
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0)));

        let err = factory.validate(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IncompatibleAggregation { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_avg_aggregation_incompatible_field_type_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let node_types = vec![NodeValueType::Int];
        let context = ValueAggregatorFactoryValidateContext::new(&field_descriptor, &node_types);
        // avg aggregation will produce a float, cannot assign it to int field
        let aggregated_field_value =
            AggregatedFieldValueProto::AvgAggregation(create_avg_aggregation_proto(Some(0)));

        let err = factory.validate(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_no_aggregation_ok() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let (nodes, node_types) = create_int_nodes_of_size(1);
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0)));

        assert!(factory.create(&aggregated_field_value, &context).is_ok());
    }
    #[test]
    fn create_no_aggregation_node_index_not_set_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let (nodes, node_types) = create_int_nodes_of_size(1);
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        // expression node index is none
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(None));

        let err = factory.create(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::MissingExpressionNodeIndex),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_no_aggregation_node_index_out_of_bound_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let (nodes, node_types) = create_int_nodes_of_size(1);
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        // requires node at index 10
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(10)));

        let err = factory.create(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IndexOutOfBound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_field_descriptor_has_unsupported_type_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(STRING_FIELD_NAME);
        let (nodes, node_types) = create_int_nodes_of_size(1);
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0)));

        let err = factory.create(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::UnsupportedFieldType { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_node_output_is_incompatible_with_field_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(FLOAT_FIELD_NAME);
        let node_types = vec![NodeValueType::Bool];
        let nodes = vec![Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) })];
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        let aggregated_field_value = AggregatedFieldValueProto::NoAggregation(
            // node at index 0 is bool type, but field type is float
            // cannot assign boolean to float field
            create_no_aggregation_proto(Some(0)),
        );

        let err = factory.create(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_node_output_is_int_but_field_is_float_ok() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(FLOAT_FIELD_NAME);
        let node_types = vec![NodeValueType::Int];
        let nodes = vec![Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) })];
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        let aggregated_field_value =
            AggregatedFieldValueProto::NoAggregation(create_no_aggregation_proto(Some(0)));

        assert!(factory.create(&aggregated_field_value, &context).is_ok());
    }

    #[test]
    fn create_vector_aggregation_with_list_typed_node_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(REPEATED_INT_FIELD_NAME);
        let node_types = vec![NodeValueType::IntList];
        let nodes = vec![Arc::new(Node::ConstantLeaf { value: NodeValue::Int32List(vec![99]) })];
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        // cannot apply vector aggregation on IntList types
        let aggregated_field_value =
            AggregatedFieldValueProto::VectorAggregation(create_vector_aggregation_proto(Some(0)));

        let err = factory.create(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IncompatibleAggregation { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_avg_aggregation_incompatible_field_type_fails() {
        let factory = ValueAggregatorFactoryImpl {};
        let field_descriptor = get_field_descriptor_from_name(INT_FIELD_NAME);
        let (nodes, node_types) = create_int_nodes_of_size(1);
        let context =
            ValueAggregatorFactoryCreateContext::new(&field_descriptor, &node_types, &nodes);
        // avg aggregation will produce a float, cannot assign it to int field
        let aggregated_field_value =
            AggregatedFieldValueProto::AvgAggregation(create_avg_aggregation_proto(Some(0)));

        let err = factory.create(&aggregated_field_value, &context).err().unwrap();

        assert!(
            matches!(err, ValueAggregatorFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }
}
