// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for manually building the abstract syntax tree and evaluating
//! the AST.

use lazy_static::lazy_static;
use log::info;
use sdv_ast::{NodeFactory, NodeFactoryImpl};
use sdv_telem_expr_test_proto::test_message::TestMessage;
use sdvtelemetry_proto_rust::expression::{
    combination_node::ArithmeticOperator, CombinationNode, ConstantLeafNode, FieldLeafNode,
    Node as NodeProto,
};

static LOG_TAG: &str = "Ast";

lazy_static! {
    static ref TEST_MESSAGE: TestMessage = {
        let mut test_message = TestMessage::new();
        test_message.int_field = 10;
        test_message
    };
}

/// This function is an argument to `Node#evaluate()`.
fn provide_test_message(_publisher_name: &str) -> &dyn protobuf::MessageDyn {
    &*TEST_MESSAGE
}

fn main() {
    sdv_log::init_logger(LOG_TAG).unwrap_or_else(|error| match &error {
        sdv_log::LoggerError::AlreadyInitializedError(_) => {
            // Only inform error, not panic
            info!("{}", error)
        }
        _ => panic!("{}", error),
    });

    info!("Telemetry AST");

    let mut left_node = NodeProto::new();
    let mut right_node = NodeProto::new();
    let mut root_node = NodeProto::new();

    let mut field_leaf = FieldLeafNode::new();
    field_leaf.publisher_name = "publisher_name".to_string();
    field_leaf.field_names.push("int_field".to_string());
    left_node.set_field_leaf_node(field_leaf); // left = 10
    let mut constant_leaf = ConstantLeafNode::new();
    constant_leaf.set_int32_value(3);
    right_node.set_constant_leaf_node(constant_leaf.clone()); // right = 3

    let mut combination_node = CombinationNode::new();
    combination_node.left_index = Some(0);
    combination_node.right_index = Some(1);
    combination_node.set_arithmetic_operator(ArithmeticOperator::DIVIDE);
    root_node.set_combination_node(combination_node); // root = 10 / 3 = 3.3333

    let all_nodes = vec![left_node, right_node, root_node];

    let node_factory = NodeFactoryImpl::new();
    match node_factory.create_nodes(&all_nodes) {
        Ok(rust_nodes) => {
            info!("All converted nodes = {:#?}", rust_nodes);
            for (i, node_ref) in rust_nodes.iter().enumerate() {
                info!("index {} result = {:?}", i, node_ref.evaluate(&provide_test_message));
            }
        }
        Err(e) => {
            info!("{}", e);
        }
    }
}
