// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_ast::Node;
use sdvtelemetry_proto_rust::{
    expression::NewProtoNode, metrics_configuration::declarative_aggregation::AggregationType,
};
use std::sync::Arc;

use crate::metrics_config_orchestrator::TriggerImpl;

pub(crate) trait Publisher {
    fn get_name(&self) -> &str;
    fn get_latest_message(&self) -> Box<dyn MessageDyn>;

    fn activate(&self) -> Result<(), String>;
    fn deactivate(&self) -> Result<(), String>;
}

#[allow(dead_code)]
pub(crate) struct PublisherImpl {
    pub name: String,
    pub publisher_type: PublisherType,
}

#[allow(dead_code)]
pub(crate) enum PublisherType {
    Signal {
        signal_name: String,
    },
    Service {
        service_name: String,
    },
    Aggregation {
        triggers: Vec<Arc<TriggerImpl>>,
        message_descriptor: MessageDescriptor,
        reset_on_get: bool,
        declarative_aggregations: Vec<DeclarativeAggregation>,
    },
}

#[allow(dead_code)]
pub(crate) struct DeclarativeAggregation {
    pub field_name: String,
    pub selector_expression: Arc<Node>,
    pub aggregation_type: AggregationType,
    // TODO(b/293213911): Create new rust type and remove reference to protobuf message
    pub seed_expression: Option<NewProtoNode>,
    pub aggregation_expression: Option<NewProtoNode>,
}

impl Publisher for PublisherImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_latest_message(&self) -> Box<dyn MessageDyn> {
        Box::new(sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig::new())
    }

    fn activate(&self) -> Result<(), String> {
        Ok(())
    }

    fn deactivate(&self) -> Result<(), String> {
        Ok(())
    }
}
