// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod constant;
mod edge;
mod previous_value_relational;

pub(crate) use constant::ConstantConditionChecker;
pub(crate) use edge::EdgeConditionChecker;
pub(crate) use previous_value_relational::PreviousValueRelationalConditionChecker;

use anyhow::{bail, Context};
use sdv_telemetry_expr::node_value::NodeValue;
use sdv_telemetry_proto_rust::expression::combination_node::{Operator, RelationalOperator};

// Represents how the very first value should be handled for condition checkers which compare two
// subsequent values.
#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum EvaluatedFirstValuePolicy {
    Skip,
    FireTrigger,
    CompareTo { value: NodeValue },
}

pub(crate) trait ConditionChecker: Send + Sync {
    fn check(&mut self, value: NodeValue) -> anyhow::Result<bool>;
}

fn apply_relational_operator(
    operator: RelationalOperator,
    left: &NodeValue,
    right: &NodeValue,
) -> anyhow::Result<bool> {
    let operator = Operator::RelationalOperator(operator.into());

    let result = left
        .operate_sync(&operator, || right)
        .with_context(|| format!("Failed to apply operator {operator:?}"))?;
    let NodeValue::Bool(result) = result else {
        bail!("Operator {operator:?} evaluation returned non-bool result: {result:?}");
    };
    Ok(result)
}
