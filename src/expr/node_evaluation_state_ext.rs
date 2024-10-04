// Copyright 2024 Google LLC

use crate::expr::node_evaluation_state::{NodeEvaluationResult, NodeEvaluationState};
use crate::expr::node_value::NodeValue;

pub(super) trait WithNodeEvaluationState {
    type Output;

    fn with_state(self, state: NodeEvaluationState) -> Self::Output;
}

impl<E> WithNodeEvaluationState for Result<NodeValue, E> {
    type Output = Result<NodeEvaluationResult, E>;

    fn with_state(self, state: NodeEvaluationState) -> Self::Output {
        self.map(move |value| (value, state))
    }
}
