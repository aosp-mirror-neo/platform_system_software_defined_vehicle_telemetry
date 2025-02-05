// Copyright 2025 Google LLC

mod constant;
mod previous_value_relational;

pub(crate) use constant::ConstantConditionChecker;
pub(crate) use previous_value_relational::PreviousValueRelationalConditionChecker;

use crate::expr::node_value::NodeValue;

pub(crate) trait ConditionChecker: Send + Sync {
    fn check(&mut self, value: NodeValue) -> anyhow::Result<bool>;
}
