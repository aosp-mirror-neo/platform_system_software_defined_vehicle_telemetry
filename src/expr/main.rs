// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for manually building the abstract syntax tree and evaluating
//! the AST.

use log::info;
use sdv_ast::Node;
use sdvtelemetry_proto_rust::expression::{
    combination_node::ArithmeticOperator, CombinationNode, ConstantLeafNode, Node as NodeProto,
};

static LOG_TAG: &str = "Ast";

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

    let mut constant_leaf = ConstantLeafNode::new();
    constant_leaf.set_int32_value(10);
    left_node.set_constant_leaf_node(constant_leaf); // left = 10
    let mut constant_leaf = ConstantLeafNode::new();
    constant_leaf.set_int64_value(3);
    right_node.set_constant_leaf_node(constant_leaf); // right = 3

    let mut combination_node = CombinationNode::new();
    combination_node.left_index = Some(0);
    combination_node.right_index = Some(1);
    combination_node.set_arithmetic_operator(ArithmeticOperator::DIVIDE);
    root_node.set_combination_node(combination_node); // root = 10 / 3 = 3.3333

    let all_nodes = vec![left_node, right_node, root_node];

    match Node::convert_nodes(&all_nodes) {
        Ok(rust_nodes) => {
            info!("All converted nodes = {:#?}", rust_nodes);
            for (i, node_ref) in rust_nodes.iter().enumerate() {
                info!("index {} result = {:?}", i, node_ref.borrow().evaluate());
            }
        }
        Err(e) => {
            info!("{}", e);
        }
    }
}
