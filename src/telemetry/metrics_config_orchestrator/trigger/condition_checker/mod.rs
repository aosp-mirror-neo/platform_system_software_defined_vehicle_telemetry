// Copyright 2025 Google LLC

mod constant;
mod edge;
mod previous_value_relational;

pub(crate) use constant::ConstantConditionChecker;
pub(crate) use edge::EdgeConditionChecker;
pub(crate) use previous_value_relational::PreviousValueRelationalConditionChecker;

use anyhow::{bail, Context};
use sdv_telemetry_proto_rust::expression::combination_node::{Operator, RelationalOperator};

use crate::expr::node_value::NodeValue;

pub(crate) trait ConditionChecker: Send + Sync {
    fn check(&mut self, value: NodeValue) -> anyhow::Result<bool>;
}

fn apply_relational_operator(
    operator: RelationalOperator,
    left: NodeValue,
    right: NodeValue,
) -> anyhow::Result<bool> {
    let operator = Operator::RelationalOperator(operator.into());

    let result = left
        .operate_sync(&operator, || Ok(right))
        .with_context(|| format!("Failed to apply operator {operator:?}"))?;
    let NodeValue::Bool(result) = result else {
        bail!("Operator {operator:?} evaluation returned non-bool result: {result:?}");
    };
    Ok(result)
}
