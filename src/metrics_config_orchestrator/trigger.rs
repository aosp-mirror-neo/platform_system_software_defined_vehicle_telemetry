// Copyright 2023 Google LLC. All Rights Reserved.

use std::marker::{Send, Sync};

use crate::expression_evaluation::Expression;

#[allow(dead_code)]
pub struct Trigger {
    name: String,
    trigger_type: TriggerType,
}

#[allow(dead_code)]
pub enum TriggerType {
    Data { publisher_name: String },
    Periodic { interval_ms: u32 },
    Conditional { triggers: Vec<Trigger>, selector_expression: Box<dyn Expression + Send + Sync> },
}
