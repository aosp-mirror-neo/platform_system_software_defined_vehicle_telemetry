// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::metrics_configuration::declarative_aggregation::AggregationType;
use sdvtelemetry_proto_rust::metrics_configuration::Publisher as PublisherProto;
use std::marker::{Send, Sync};
use std::ops::Fn;

use crate::expression_evaluation::Expression;
use crate::metrics_config_orchestrator::trigger::Trigger;
use crate::metrics_config_orchestrator::MetricsConfig;

pub trait PublisherFactory {
    fn create(&self, publisher_proto: &PublisherProto, context: &MetricsConfig) -> Publisher;
}

#[allow(dead_code)]
pub struct Publisher {
    name: String,
    publisher_type: PublisherType,
}

#[allow(dead_code)]
pub enum PublisherType {
    Signal {},
    Service {},
    Aggregation {
        triggers: Vec<Trigger>,
        message_factory: Box<dyn (Fn() -> dyn protobuf::MessageDyn) + Send + Sync>,
        reset_on_get: bool,
        declarative_aggregations: Vec<DeclarativeAggregation>,
    },
}

#[allow(dead_code)]
pub struct DeclarativeAggregation {
    field_name: String,
    selector_expression: Box<dyn Expression + Send + Sync>,
    aggregation_type: AggregationType,
    seed_expression: Box<dyn Expression + Send + Sync>,
    aggregation_expression: Box<dyn Expression + Send + Sync>,
}

pub struct PublisherFactoryImpl {}

impl PublisherFactory for PublisherFactoryImpl {
    #[allow(unused_variables)]
    fn create(&self, publisher_proto: &PublisherProto, context: &MetricsConfig) -> Publisher {
        Publisher { name: publisher_proto.name.clone(), publisher_type: PublisherType::Service {} }
    }
}
