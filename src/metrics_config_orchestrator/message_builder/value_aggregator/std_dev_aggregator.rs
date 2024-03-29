use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where numeric standard deviation is calculated.
pub struct StdDevAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the running average. This is saved for efficient standard deviation computation
    // updates.
    current_avg: NodeValue,
    // Stores the running standard deviation.
    current_std_dev: NodeValue,
    // Stores the number of samples.
    n: u32,
}

// Update the running standard deviation after learning the new sample. The calculation is
// performed using Welford's algorithm.
fn update_running_standard_deviation(
    n: u32,
    prev_std_dev: &mut NodeValue,
    prev_avg: &NodeValue,
    new_avg: &NodeValue,
    new_sample: &NodeValue,
) -> Result<(), AggregationError> {
    if n == 0 {
        // Standard deviation for a single value is always 0 (default value) as the average is
        // the same as the value itself.
        *prev_std_dev = NodeValue::Double(0.0);
        return Ok(());
    }

    let n = n as f64;

    let get_double = |node_value: &NodeValue| -> Result<f64, AggregationError> {
        node_value.get_double(&|| AggregationError::NumericAggregationTypeMismatch {
            value: format!("{:?}", node_value),
        })
    };

    let prev_std_dev_value = get_double(prev_std_dev)?;
    let new_sample_value = get_double(new_sample)?;
    let prev_avg_value = get_double(prev_avg)?;
    let new_avg_value = get_double(new_avg)?;

    // Distances from the previous and new averages
    let prev_delta = new_sample_value - prev_avg_value;
    let new_delta = new_sample_value - new_avg_value;

    // Accumulated squared distance from the previous average
    let acc_squared_dist = (prev_std_dev_value.powi(2) * n) + (prev_delta * new_delta);

    let variance = acc_squared_dist / (n + 1.0);
    *prev_std_dev = NodeValue::Double(variance.sqrt());
    Ok(())
}

impl ValueAggregator for StdDevAggregator {
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        let new_value = self.node.evaluate()?;
        if new_value.is_none() {
            return Ok(new_value);
        }
        if !new_value.is_numeric() {
            panic!(
                "Validation should have ensured that standard deviation aggregation type \
                has numeric expression evaluation results"
            );
        }

        let prev_avg = self.current_avg.clone();
        self.current_avg.update_running_average(self.n, &new_value)?;

        update_running_standard_deviation(
            self.n,
            &mut self.current_std_dev,
            &prev_avg,
            &self.current_avg,
            &new_value,
        )?;
        self.n += 1;
        Ok(self.current_std_dev.clone())
    }
}

impl StdDevAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self {
            node: node.clone(),
            current_avg: NodeValue::Double(0.0),
            current_std_dev: NodeValue::Double(0.0),
            n: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_computed_values_from_aggregator<T>(node_outputs: &[T]) -> Vec<NodeValue>
    where
        NodeValue: std::convert::From<T>,
        T: Copy,
    {
        let mut std_dev_aggregator =
            StdDevAggregator::new(&Arc::new(Node::ConstantLeaf { value: node_outputs[0].into() }));
        let mut result_std_devs = Vec::new();

        for i in 0..node_outputs.len() - 1 {
            let res = std_dev_aggregator.compute().unwrap();
            result_std_devs.push(res);

            // update the next exrp eval result, so we can test correctness of compute()
            std_dev_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: node_outputs[i + 1].into() });
        }

        // To make sure the last node also gets calculated.
        result_std_devs.push(std_dev_aggregator.compute().unwrap());

        result_std_devs
    }

    #[test]
    fn std_dev_aggregator_compute_with_int_ok() {
        let node_outputs: &[i64] = &[2, 4, 4, 4, 5, 5, 7, 9];
        let expected: &[f64] = &[
            0.0,
            1.0,
            (8_f64 / 9_f64).sqrt(),
            0.75_f64.sqrt(),
            0.96_f64.sqrt(),
            1.0,
            (96_f64 / 49_f64).sqrt(),
            2.0,
        ];

        let res = get_computed_values_from_aggregator(node_outputs);
        assert_eq!(res, expected.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn std_dev_aggregator_compute_with_float_ok() {
        let node_outputs: &[f64] = &[2.0, 3.5, 4.25, 4.75, 5.5, 4.5, 6.75, 9.75];
        let expected: &[f64] = &[
            0.0,
            0.75,
            0.875_f64.sqrt(),
            1.078125_f64.sqrt(),
            1.425_f64.sqrt(),
            (11_f64 / 9_f64).sqrt(),
            (94_f64 / 49_f64).sqrt(),
            4.734375_f64.sqrt(),
        ];

        let res = get_computed_values_from_aggregator(node_outputs);
        assert_eq!(res, expected.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn std_dev_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            StdDevAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let res = aggregator.compute().unwrap();
        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn std_dev_aggregator_compute_with_non_numeric_value_panics() {
        let mut bad_std_dev =
            StdDevAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        bad_std_dev.compute().unwrap();
    }
}
