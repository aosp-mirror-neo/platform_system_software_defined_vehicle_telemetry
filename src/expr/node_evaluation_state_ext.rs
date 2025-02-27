// Copyright 2024 Google LLC

use crate::node_value::NodeValue;

pub(super) trait WithNodeEvaluationState<S> {
    type Output;

    fn with_state(self, state: S) -> Self::Output;
}

impl<S, E> WithNodeEvaluationState<S> for Result<NodeValue, E> {
    type Output = Result<(NodeValue, S), E>;

    fn with_state(self, state: S) -> Self::Output {
        self.map(move |value| (value, state))
    }
}
