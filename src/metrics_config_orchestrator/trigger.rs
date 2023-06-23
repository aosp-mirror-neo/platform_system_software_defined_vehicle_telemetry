// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::metrics_configuration::Trigger as TriggerProto;
use std::marker::{Send, Sync};

use crate::expression_evaluation::Expression;
use crate::metrics_config_orchestrator::MetricsConfig;

pub trait TriggerFactory {
    fn create(&self, trigger_proto: &TriggerProto, context: &MetricsConfig) -> Trigger;
}

pub struct Trigger {
    pub name: String,
    pub trigger_type: TriggerType,
}

#[allow(dead_code)]
pub enum TriggerType {
    Data { publisher_name: String },
    Periodic { interval_ms: u32 },
    Conditional { triggers: Vec<Trigger>, selector_expression: Box<dyn Expression + Send + Sync> },
}

pub struct TriggerFactoryImpl {}

impl TriggerFactory for TriggerFactoryImpl {
    #[allow(unused_variables)]
    fn create(&self, trigger_proto: &TriggerProto, context: &MetricsConfig) -> Trigger {
        Trigger {
            name: trigger_proto.name.clone(),
            trigger_type: TriggerType::Periodic { interval_ms: u32::MAX },
        }
    }
}
