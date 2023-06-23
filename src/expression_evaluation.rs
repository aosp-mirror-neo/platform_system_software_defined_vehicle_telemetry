// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::expression::{NewProtoNode, Node};
use std::ops::Fn;

/// Creates expression based on the root node and the child nodes provided by `node_provider`.
pub trait ExpressionFactory {
    fn create_from_node(&self, node: &Node, other_nodes: &[Node]) -> dyn Expression;
    fn create_from_new_proto(&self, node: &NewProtoNode, other_nodes: &[Node]) -> dyn Expression;
}

/// Evaluates associated expression using publisher messages provided by `message_provider`.
pub trait Expression {
    fn evaluate(
        &self,
        message_provider: &dyn Fn(&str) -> &dyn protobuf::MessageDyn,
    ) -> dyn protobuf::MessageDyn;
}
