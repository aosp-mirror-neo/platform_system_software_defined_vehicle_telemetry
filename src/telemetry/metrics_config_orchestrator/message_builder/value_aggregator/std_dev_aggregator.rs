// Copyright 2024 Google LLC

use sdv_telemetry_expr::node_value::NodeValue;

use super::node_value_extension::NodeValueExtension;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

const DEFAULT_DOUBLE_VALUE: NodeValue = NodeValue::Double(0.0);

/// Field value type where numeric standard deviation is calculated.
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct StdDevAggregator {
    // Stores the running average. This is saved for efficient standard deviation computation
    // updates.
    current_avg: NodeValue,
    // Stores the running standard deviation.
    current_std_dev: NodeValue,
    // Stores the number of samples.
    n: u32,
}

impl ValueAggregator for StdDevAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        let node_value = node_value.expect("Aggregator should be provided with a value");
        if node_value.is_none() {
            return Ok(NodeValue::None);
        }

        if !node_value.is_numeric() {
            panic!(
                "Validation should have ensured that standard deviation aggregation type \
                has numeric expression evaluation results"
            );
        }

        let prev_avg = self.current_avg.clone();
        self.current_avg.update_running_average(self.n, &node_value)?;

        Self::update_running_standard_deviation(
            self.n,
            &mut self.current_std_dev,
            &prev_avg,
            &self.current_avg,
            &node_value,
        )?;
        self.n += 1;
        Ok(self.current_std_dev.clone())
    }

    fn reset(&mut self) {
        self.current_avg = DEFAULT_DOUBLE_VALUE;
        self.current_std_dev = DEFAULT_DOUBLE_VALUE;
        self.n = 0;
    }
}

impl StdDevAggregator {
    pub(crate) fn new() -> Self {
        Self { current_avg: DEFAULT_DOUBLE_VALUE, current_std_dev: DEFAULT_DOUBLE_VALUE, n: 0 }
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

        let n = f64::from(n);

        let get_double = |node_value: &NodeValue| -> Result<f64, AggregationError> {
            node_value.get_double().ok_or_else(|| {
                AggregationError::NumericAggregationTypeMismatch {
                    value: format!("{node_value:?}"),
                }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_computed_values_from_aggregator<T>(node_outputs: &[T]) -> Vec<NodeValue>
    where
        NodeValue: std::convert::From<T>,
        T: Copy,
    {
        let mut aggregator = StdDevAggregator::new();
        node_outputs
            .iter()
            .map(|value| aggregator.compute(Some((*value).into())).unwrap())
            .collect()
    }

    #[test]
    fn compute_with_int_ok() {
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
    fn compute_with_float_ok() {
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
    fn compute_with_none_value_ok() {
        let mut aggregator = StdDevAggregator::new();
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn compute_with_non_numeric_value_panics() {
        let mut bad_std_dev = StdDevAggregator::new();
        bad_std_dev.compute(Some(NodeValue::Bool(true))).unwrap();
    }

    #[test]
    #[should_panic]
    fn compute_without_node_value_panics() {
        let mut aggregator = StdDevAggregator::new();
        aggregator.compute(None).unwrap();
    }

    #[test]
    fn reset_ok() {
        let mut aggregator = StdDevAggregator::new();
        aggregator.compute(Some(2.into())).unwrap();

        aggregator.reset();

        assert_eq!(aggregator.current_avg, DEFAULT_DOUBLE_VALUE);
        assert_eq!(aggregator.current_std_dev, DEFAULT_DOUBLE_VALUE);
        assert_eq!(aggregator.n, 0);
    }
}
