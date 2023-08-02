// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::MessageDescriptor;
use sdv_ast::Node;
use sdvtelemetry_proto_rust::{
    expression::NewProtoNode, metrics_configuration::declarative_aggregation::AggregationType,
};
use std::sync::{Arc, Weak};

use crate::metrics_config_orchestrator::Trigger;

pub struct Publisher {
    pub name: String,
    pub publisher_type: PublisherType,
}

pub enum PublisherType {
    Signal {
        signal_name: String,
    },
    Service {
        service_name: String,
    },
    Aggregation {
        triggers: Vec<Weak<Trigger>>,
        message_descriptor: MessageDescriptor,
        reset_on_get: bool,
        declarative_aggregations: Vec<DeclarativeAggregation>,
    },
}

pub struct DeclarativeAggregation {
    pub field_name: String,
    pub selector_expression: Arc<Node>,
    pub aggregation_type: AggregationType,
    // TODO(b/293213911): Create new rust type and remove reference to protobuf message
    pub seed_expression: Option<NewProtoNode>,
    pub aggregation_expression: Option<NewProtoNode>,
}

impl Publisher {
    #[allow(dead_code)]
    fn activate(&self) -> Result<(), String> {
        Ok(())
    }

    #[allow(dead_code)]
    fn dectivate(&self) -> Result<(), String> {
        Ok(())
    }
}
