// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for manually building the abstract syntax tree and evaluating
//! the AST.

use lazy_static::lazy_static;
use log::info;
use protobuf::MessageDyn;
use sdv_ast::{MessageProvider, MessageProviderError, NodeFactory, NodeFactoryImpl};
use sdv_telem_expr_test_proto::test_message::TestMessage;
use sdvtelemetry_proto_rust::expression::{
    combination_node::ArithmeticOperator, CombinationNode, ConstantLeafNode, FieldLeafNode,
    Node as NodeProto,
};
use std::sync::Arc;

static LOG_TAG: &str = "Ast";

lazy_static! {
    static ref TEST_MESSAGE: TestMessage = {
        let mut test_message = TestMessage::new();
        test_message.int_field = 10;
        test_message.repeated_doubles =
            vec![std::f64::consts::PI, std::f64::consts::TAU, std::f64::consts::E];
        test_message
    };
}

struct MessageProviderImpl {}

impl MessageProvider for MessageProviderImpl {
    fn get_message(
        &self,
        _publisher_name: &str,
    ) -> Result<Option<Box<dyn MessageDyn>>, MessageProviderError> {
        Ok(Some(Box::new((*TEST_MESSAGE).clone())))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sdv_log::init_logger(LOG_TAG)?;

    info!("Telemetry AST");

    let mut left_node = NodeProto::new();
    let mut right_node = NodeProto::new();
    let mut root_node = NodeProto::new();
    let mut value_node = NodeProto::new();
    let mut message_node = NodeProto::new();

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

    let mut field_leaf = FieldLeafNode::new();
    field_leaf.publisher_name = "publisher_name".to_string();
    message_node.set_field_leaf_node(field_leaf.clone()); // protobuf message

    field_leaf.field_names.push("repeated_doubles".to_string());
    value_node.set_field_leaf_node(field_leaf); // list = [3.14, 6.28, 2.718]

    let all_nodes = vec![left_node, right_node, root_node, value_node, message_node];

    let node_factory = NodeFactoryImpl {};
    let message_provider = Arc::new(MessageProviderImpl {}) as Arc<dyn MessageProvider>;
    match node_factory.create_nodes(&all_nodes, &message_provider) {
        Ok(rust_nodes) => {
            info!("All converted nodes = {:#?}", rust_nodes);
            for (i, node_ref) in rust_nodes.iter().enumerate() {
                info!("index {} result = {:?}", i, node_ref.evaluate());
            }
        }
        Err(e) => {
            info!("{}", e);
        }
    }

    Ok(())
}
