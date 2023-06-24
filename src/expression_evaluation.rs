// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::expression::{NewProtoNode as NewProtoNodeProto, Node as NodeProto};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
use std::ops::Fn;

/// Creates expression based on the root node and the child nodes provided by `node_provider`.
pub trait ExpressionFactory {
    fn create_from_node(&self, node: &NodeProto, other_nodes: &[NodeProto]) -> ExpressionImpl;
    fn create_from_new_proto(
        &self,
        node: &NewProtoNodeProto,
        other_nodes: &[NodeProto],
    ) -> ExpressionImpl;
}

/// Evaluates associated expression using publisher messages provided by `message_provider`.
pub trait Expression {
    fn evaluate(
        &self,
        message_provider: &dyn Fn(&str) -> &dyn protobuf::MessageDyn,
    ) -> Box<dyn protobuf::MessageDyn>;
}

pub struct ExpressionFactoryImpl {}

pub struct ExpressionImpl {}

impl ExpressionFactory for ExpressionFactoryImpl {
    #[allow(unused_variables)]
    fn create_from_node(&self, node: &NodeProto, other_nodes: &[NodeProto]) -> ExpressionImpl {
        ExpressionImpl {}
    }

    #[allow(unused_variables)]
    fn create_from_new_proto(
        &self,
        node: &NewProtoNodeProto,
        other_nodes: &[NodeProto],
    ) -> ExpressionImpl {
        ExpressionImpl {}
    }
}

impl Expression for ExpressionImpl {
    #[allow(unused_variables)]
    fn evaluate(
        &self,
        message_provider: &dyn Fn(&str) -> &dyn protobuf::MessageDyn,
    ) -> Box<dyn protobuf::MessageDyn> {
        Box::new(MetricsConfig::new())
    }
}
