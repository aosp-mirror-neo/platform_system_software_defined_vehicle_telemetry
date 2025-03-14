// Copyright 2025 Google LLC

use parking_lot::Mutex;
use sdv_telemetry_expr::{node_evaluation_context::NodeEvaluationContext, node_value::NodeValue};
use sdv_telemetry_proto_rust::expression::combination_node::RelationalOperator;
use sdv_telemetry_utils::atomic::{SequenceNumber, SequenceNumberType};
use sdv_telemetry_utils::option_extension::OptionExtension;
use std::sync::{Arc, Weak};
use tokio::task::AbortHandle;
use tokio::time::sleep;

use super::super::conditional::EdgeOptions;
use super::{apply_relational_operator, ConditionChecker};
use crate::metrics_config_orchestrator::node_evaluation_context_factory::NodeEvaluationContextFactory;

#[derive(Copy, Clone)]
#[cfg_attr(test, derive(Debug, Eq, PartialEq))]
enum Edge {
    Rising,
    Falling,
}

impl Edge {
    /// Checks whether there is an edge of type `Self` between `prev` and `cur`.
    fn is_edge(&self, prev: &NodeValue, cur: &NodeValue) -> anyhow::Result<bool> {
        apply_relational_operator(
            match self {
                Self::Rising => RelationalOperator::LT,
                Self::Falling => RelationalOperator::GT,
            },
            prev,
            cur,
        )
    }
}

enum EdgeDetectionState {
    /// Waiting for an edge to occur
    WaitForEdge { previous_value: Option<NodeValue> },
    /// After an edge occurred, keep monitoring whether there are glitches.
    MonitorGlitches {
        /// The type of edge that we are monitoring.
        edge: Edge,
        /// Whether or not all values during monitoring must be equal to each other.
        require_exact: bool,
        /// The "previous_value" of the initial edge that caused us to start monitoring. While
        /// monitoring, all incoming values must also be an edge as compared to this value.
        threshold: NodeValue,
        /// The newest value received during edge monitoring.
        previous_value: NodeValue,

        timeout_id: SequenceNumberType,
        timeout_abort_handle: AbortHandle,
    },
}

impl Default for EdgeDetectionState {
    fn default() -> Self {
        Self::WaitForEdge { previous_value: None }
    }
}

impl Drop for EdgeDetectionState {
    fn drop(&mut self) {
        // If we were monitoring for glitches, abort the async task that is waiting for the timeout
        // to occur.
        let Self::MonitorGlitches { timeout_abort_handle, .. } = self else {
            return;
        };
        timeout_abort_handle.abort();
    }
}

enum EdgeDetectionOptions {
    Rising(EdgeOptions),
    Falling(EdgeOptions),
    Both { rising_options: EdgeOptions, falling_options: EdgeOptions },
}

impl EdgeDetectionOptions {
    /// Check if there is an edge supported by `Self` between the `prev` and `cur` value. If so,
    /// returns `Ok(Some(_))`, otherwise returns `Ok(None)`.
    fn which_edge(
        &self,
        prev: &NodeValue,
        cur: &NodeValue,
    ) -> anyhow::Result<Option<(Edge, &EdgeOptions)>> {
        let to_check = match self {
            Self::Rising(edge_options) => &[(Edge::Rising, edge_options)][..],
            Self::Falling(edge_options) => &[(Edge::Falling, edge_options)][..],
            Self::Both { rising_options, falling_options } => {
                &[(Edge::Rising, rising_options), (Edge::Falling, falling_options)][..]
            }
        };

        for (edge, edge_options) in to_check.iter() {
            if edge.is_edge(prev, cur)? {
                return Ok(Some((*edge, *edge_options)));
            }
        }

        Ok(None)
    }
}

struct EdgeConditionCheckerImpl {
    timeout_id_generator: SequenceNumber,
    state: EdgeDetectionState,
    edge_detection_options: EdgeDetectionOptions,
    node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,

    /// Used to notify the caller that it should trigger without being associated to any specific
    /// call to `Self::check`.
    synthetic_trigger_callback: Box<dyn FnMut(Arc<NodeEvaluationContext>) + Send + Sync>,
}

impl EdgeConditionCheckerImpl {
    fn check(
        &mut self,
        current_value: NodeValue,
        weak_factory: impl Fn() -> Weak<Mutex<Self>>,
    ) -> anyhow::Result<bool> {
        if current_value.is_none() {
            // TODO: b/307547570 - properly handle absent values
            return Ok(false);
        }

        match &mut self.state {
            EdgeDetectionState::WaitForEdge { ref mut previous_value } => {
                // Extract previous value and save current value.
                let (previous_value, current_value) = previous_value.take_and_insert(current_value);
                let Some(previous_value) = previous_value else {
                    // No previous value exists, wait for the next value to arrive.
                    return Ok(false);
                };

                let Some((edge, EdgeOptions { min_duration, require_exact })) =
                    self.edge_detection_options.which_edge(&previous_value, current_value)?
                else {
                    // No edge detected.
                    return Ok(false);
                };

                if min_duration.is_zero() {
                    // No glitch filtering, trigger immediately.
                    return Ok(true);
                }

                // Spawn a task to notify `self` after `min_duration`.
                let node_evaluation_context_factory =
                    Arc::clone(&self.node_evaluation_context_factory);
                let weak_self = weak_factory();
                let timeout_id = self.timeout_id_generator.next();
                let min_duration = min_duration.to_owned();
                let abort_handle = tokio::spawn(async move {
                    sleep(min_duration).await;
                    if let Some(condition_checker) = weak_self.upgrade() {
                        // Create the context as early as possible, before waiting for the lock.
                        let context = Arc::new(node_evaluation_context_factory.create());

                        condition_checker.lock().on_timeout(timeout_id, context);
                    }
                })
                .abort_handle();

                let current_value = std::mem::replace(current_value, NodeValue::None);
                self.state = EdgeDetectionState::MonitorGlitches {
                    threshold: previous_value,
                    previous_value: current_value,
                    timeout_abort_handle: abort_handle,
                    timeout_id,
                    edge,
                    require_exact: *require_exact,
                };

                // Do not trigger now, trigger later.
                Ok(false)
            }
            EdgeDetectionState::MonitorGlitches {
                ref threshold,
                ref mut previous_value,
                timeout_abort_handle: _,
                timeout_id: _,
                ref edge,
                ref require_exact,
            } => {
                let continue_monitoring = if *require_exact {
                    apply_relational_operator(
                        RelationalOperator::EQ,
                        previous_value,
                        &current_value,
                    )?
                } else {
                    edge.is_edge(threshold, &current_value)?
                };

                if continue_monitoring {
                    *previous_value = current_value;
                    Ok(false)
                } else {
                    self.state =
                        EdgeDetectionState::WaitForEdge { previous_value: Some(threshold.clone()) };
                    self.check(current_value, weak_factory)
                }
            }
        }
    }

    fn on_timeout(
        &mut self,
        want_timeout_id: SequenceNumberType,
        context: Arc<NodeEvaluationContext>,
    ) {
        // If we are no longer monitoring glitches or timeout IDs don't match, then we do nothing -
        // this can only be caused by a race between a call to `on_timeout` and `check`, which has
        // made this timeout obsolete.
        let EdgeDetectionState::MonitorGlitches {
            threshold: _,
            ref previous_value,
            timeout_id,
            timeout_abort_handle: _,
            edge: _,
            require_exact: _,
        } = self.state
        else {
            return;
        };
        if want_timeout_id != timeout_id {
            return;
        }

        (self.synthetic_trigger_callback)(context);
        self.state =
            EdgeDetectionState::WaitForEdge { previous_value: Some(previous_value.clone()) };
    }
}

/// Wrapper around `EdgeConditionCheckerImpl`, which hides the fact that it is wrapped in an `Arc`
/// from the caller.
pub(crate) struct EdgeConditionChecker {
    implementation: Arc<Mutex<EdgeConditionCheckerImpl>>,
}

impl ConditionChecker for EdgeConditionChecker {
    fn check(&mut self, value: NodeValue) -> anyhow::Result<bool> {
        self.implementation.lock().check(value, || Arc::downgrade(&self.implementation))
    }
}

impl EdgeConditionChecker {
    pub(crate) fn new_rising_edge(
        rising_options: EdgeOptions,
        synthetic_trigger_callback: impl FnMut(Arc<NodeEvaluationContext>) + Send + Sync + 'static,
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    ) -> Self {
        Self {
            implementation: Arc::new(
                EdgeConditionCheckerImpl {
                    timeout_id_generator: SequenceNumber::new(),
                    edge_detection_options: EdgeDetectionOptions::Rising(rising_options),
                    state: EdgeDetectionState::default(),
                    synthetic_trigger_callback: Box::new(synthetic_trigger_callback),
                    node_evaluation_context_factory,
                }
                .into(),
            ),
        }
    }

    pub(crate) fn new_falling_edge(
        falling_options: EdgeOptions,
        synthetic_trigger_callback: impl FnMut(Arc<NodeEvaluationContext>) + Send + Sync + 'static,
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    ) -> Self {
        Self {
            implementation: Arc::new(
                EdgeConditionCheckerImpl {
                    timeout_id_generator: SequenceNumber::new(),
                    edge_detection_options: EdgeDetectionOptions::Falling(falling_options),
                    state: EdgeDetectionState::default(),
                    synthetic_trigger_callback: Box::new(synthetic_trigger_callback),
                    node_evaluation_context_factory,
                }
                .into(),
            ),
        }
    }

    pub(crate) fn new_both_edges(
        rising_options: EdgeOptions,
        falling_options: EdgeOptions,
        synthetic_trigger_callback: impl FnMut(Arc<NodeEvaluationContext>) + Send + Sync + 'static,
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    ) -> Self {
        Self {
            implementation: Arc::new(
                EdgeConditionCheckerImpl {
                    timeout_id_generator: SequenceNumber::new(),
                    edge_detection_options: EdgeDetectionOptions::Both {
                        rising_options,
                        falling_options,
                    },
                    state: EdgeDetectionState::default(),
                    synthetic_trigger_callback: Box::new(synthetic_trigger_callback),
                    node_evaluation_context_factory,
                }
                .into(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::{self, StreamExt};
    use googletest::matcher::{Matcher, MatcherBase};
    use googletest::matchers::{
        anything, container_eq, eq, err, ge, is_false, is_true, le, matches_pattern, none, ok,
        points_to, pointwise, some,
    };
    use googletest::{assert_that, expect_that};
    use lazy_static::lazy_static;
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_test_utils::parameterized_test;
    use std::future;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;
    use tokio::task::JoinError;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use crate::metrics_config_orchestrator::node_evaluation_context_factory::NodeEvaluationContextFactoryImpl;

    const NONE: NodeValue = NodeValue::None;
    const TRUE: NodeValue = NodeValue::Bool(true);
    const FALSE: NodeValue = NodeValue::Bool(false);
    const INT_0: NodeValue = NodeValue::Int32(0);
    const INT_1: NodeValue = NodeValue::Int32(1);
    const INT_2: NodeValue = NodeValue::Int32(2);
    const INT_3: NodeValue = NodeValue::Int32(3);
    const INT_4: NodeValue = NodeValue::Int32(4);

    lazy_static! {
        static ref STR_0: NodeValue = NodeValue::String("0".to_owned());
        static ref STR_1: NodeValue = NodeValue::String("1".to_owned());
    }

    parameterized_test!(
        #[test]
        edge_is_edge,
        |edge: Edge, prev: &NodeValue, cur: &NodeValue, matcher| {
            assert_that!(edge.is_edge(prev, cur), matcher);
        },

        rising_true_1: (Edge::Rising, &FALSE, &TRUE, ok(is_true())),
        rising_true_2: (Edge::Rising, &INT_1, &INT_3, ok(is_true())),
        rising_false_1: (Edge::Rising, &TRUE, &TRUE, ok(is_false())),
        rising_false_3: (Edge::Rising, &TRUE, &FALSE, ok(is_false())),
        rising_false_2: (Edge::Rising, &FALSE, &FALSE, ok(is_false())),
        rising_unsupported_1: (Edge::Rising, &NONE, &TRUE, err(anything())),
        rising_unsupported_2: (Edge::Rising, &STR_1, &STR_0, err(anything())),
        rising_unsupported_3: (Edge::Rising, &STR_1, &INT_0, err(anything())),

        falling_true_1: (Edge::Falling, &TRUE, &FALSE, ok(is_true())),
        falling_true_2: (Edge::Falling, &INT_3, &INT_1, ok(is_true())),
        falling_false_1: (Edge::Falling, &TRUE, &TRUE, ok(is_false())),
        falling_false_2: (Edge::Falling, &FALSE, &TRUE, ok(is_false())),
        falling_false_3: (Edge::Falling, &FALSE, &FALSE, ok(is_false())),
        falling_unsupported_1: (Edge::Falling, &NONE, &TRUE, err(anything())),
        falling_unsupported_2: (Edge::Falling, &STR_1, &STR_0, err(anything())),
        falling_unsupported_3: (Edge::Falling, &STR_1, &INT_0, err(anything())),
    );

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn edge_detection_state_aborts_timeout_task_when_changed() {
        let task = tokio::spawn(future::pending::<()>());

        let mut _state = EdgeDetectionState::MonitorGlitches {
            timeout_abort_handle: task.abort_handle(),

            // Values of these fields don't matter.
            edge: Edge::Rising,
            threshold: NodeValue::None,
            previous_value: NodeValue::None,
            require_exact: false,
            timeout_id: 0,
        };

        // The task should continue running after passing the `AbortHandle` into `MonitorGlitches`.
        tokio::task::yield_now().await;
        expect_that!(task.is_finished(), is_false());

        // Updating the `_state` to `WaitForEdge` should cancel the task.
        _state = EdgeDetectionState::WaitForEdge { previous_value: None };
        expect_that!(task.await, err(matches_pattern!(JoinError { is_cancelled(): is_true() })));
    }

    // Arbitrary options, just used to verify that `which_edge` returns the corresponding
    // `EdgeOptions`.
    const EO_1: EdgeOptions =
        EdgeOptions { min_duration: Duration::from_secs(123), require_exact: true };
    const EO_2: EdgeOptions = EdgeOptions { min_duration: Duration::ZERO, require_exact: false };

    parameterized_test!(
        #[test]
        edge_detection_options_which_edge,
        |options: EdgeDetectionOptions, prev: &NodeValue, cur: &NodeValue, matcher| {
            assert_that!(options.which_edge(prev, cur), matcher);
        },

        rising_rising: (
            EdgeDetectionOptions::Rising(EO_1.clone()),
            &FALSE, &TRUE, ok(some((eq(&Edge::Rising), eq(&&EO_1)))),
        ),
        rising_falling: (
            EdgeDetectionOptions::Rising(EO_1.clone()),
            &TRUE, &FALSE, ok(none()),
        ),
        rising_equal: (
            EdgeDetectionOptions::Rising(EO_1.clone()),
            &TRUE, &TRUE, ok(none()),
        ),

        falling_rising: (
            EdgeDetectionOptions::Falling(EO_2.clone()),
            &FALSE, &TRUE, ok(none()),
        ),
        falling_falling: (
            EdgeDetectionOptions::Falling(EO_2.clone()),
            &TRUE, &FALSE, ok(some((eq(&Edge::Falling), eq(&&EO_2)))),
        ),
        falling_equal: (
            EdgeDetectionOptions::Falling(EO_2.clone()),
            &TRUE, &TRUE, ok(none()),
        ),

        both_rising: (
            EdgeDetectionOptions::Both { rising_options: EO_1.clone(), falling_options: EO_2.clone() },
            &FALSE, &TRUE, ok(some((eq(&Edge::Rising), eq(&&EO_1)))),
        ),
        both_falling: (
            EdgeDetectionOptions::Both { rising_options: EO_1.clone(), falling_options: EO_2.clone() },
            &TRUE, &FALSE, ok(some((eq(&Edge::Falling), eq(&&EO_2)))),
        ),
        both_equal: (
            EdgeDetectionOptions::Both { rising_options: EO_1.clone(), falling_options: EO_2.clone() },
            &TRUE, &TRUE, ok(none()),
        ),
    );

    #[sdv_telemetry_test_utils::timeout]
    async fn run_test(
        condition_checker_factory: impl FnOnce(
            Box<dyn FnMut(Arc<NodeEvaluationContext>) + Send + Sync>,
            Arc<dyn NodeEvaluationContextFactory>,
        ) -> EdgeConditionChecker,
        input: &[(Duration, &NodeValue)],
        want_sync: &[Result<bool, ()>],
        want_async: &[Duration],
    ) {
        fn approx_eq(expected: Duration) -> impl Matcher<Duration> {
            // 20% tolerance.
            const TIME_TOLERANCE: Duration = Duration::from_millis(d(1).as_millis() as u64 / 5);

            ge(expected - TIME_TOLERANCE).and(le(expected + TIME_TOLERANCE))
        }

        let (tx, rx) = mpsc::unbounded_channel();

        let mut checker = condition_checker_factory(
            Box::new(move |_context| tx.send(Instant::now()).unwrap()),
            Arc::new(NodeEvaluationContextFactoryImpl::new(new_stub_pal())),
        );

        let start = Instant::now();
        let got_sync = stream::iter(
            input
                .iter()
                // Sum up relative `wait_before`s into absolute `wait_until`s.
                .scan(Duration::ZERO, |wait_until, (wait_before, node_value)| {
                    *wait_until += *wait_before;
                    Some((*wait_until, node_value))
                }),
        )
        .then(|(wait_until, node_value)| async move {
            sleep(wait_until.saturating_sub(start.elapsed())).await;
            node_value
        })
        .map(|node_value| checker.check(node_value.to_owned().clone()).map_err(|_| ()))
        .collect::<Vec<_>>()
        .await;

        // Wait for the condition checker to be idle (=waiting for the next edge, as opposed to
        // monitoring for glitches).
        while !matches!(checker.implementation.lock().state, EdgeDetectionState::WaitForEdge { .. })
        {
            sleep(Duration::from_millis(50)).await;
        }

        // Drop the condition checker to close the `(tx, rx)` channel and therefore allow collection
        // of the results.
        drop(checker);

        // Collect all received synthetic trigger timestamps and convert them into `Duration`s
        // relative to `start`.
        let got_async = UnboundedReceiverStream::new(rx)
            .map(|triggered_at| triggered_at.duration_since(start))
            .collect::<Vec<_>>()
            .await;

        expect_that!(got_sync, container_eq(want_sync.to_vec()));
        expect_that!(got_async, pointwise!(|want| points_to(approx_eq(*want)), want_async));
    }

    // Helper constants to make specifying the expected result more succinct.
    const T: Result<bool, ()> = Ok(true);
    const F: Result<bool, ()> = Ok(false);
    const E: Result<bool, ()> = Err(());

    /// Helper function that abstracts and shortens the creation of a `Duration`.
    const fn d(units: u64) -> Duration {
        Duration::from_millis(units * 100)
    }

    parameterized_test!(
        #[googletest::test]
        // Timeout is applied to `run_test`, so no need for #[sdv_telemetry_test_utils::timeout].
        #[tokio::test]
        edge_condition_checker_with_default_edge_options,
        |condition_checker_factory,
         input: &[(Duration, &NodeValue)],
         want_sync: &[Result<bool, ()>]| async {
            run_test(
                condition_checker_factory,
                input,
                want_sync,
                // no async triggers expected for default edge options.
                &[],
            ).await;
        },

        both_edges: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions::default(), EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &INT_1), (d(0), &INT_2), (d(0), &INT_2), (d(0), &INT_0)],
            &[F, T, F, T],
        ),
        both_edges_string: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions::default(), EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &STR_0), (d(0), &STR_0)],
            &[F, E],
        ),
        both_edges_none: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions::default(), EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &NONE), (d(0), &NONE)],
            &[F, F],
        ),

        rising_edge: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &INT_1), (d(0), &INT_2), (d(0), &INT_1)],
            &[F, T, F],
        ),
        rising_edge_string: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &STR_0), (d(0), &STR_0)],
            &[F, E],
        ),
        rising_edge_none: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &NONE), (d(0), &NONE)],
            &[F, F],
        ),

        falling_edge: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &INT_2), (d(0), &INT_1), (d(0), &INT_2)],
            &[F, T, F],
        ),
        falling_edge_string: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &STR_0), (d(0), &STR_0)],
            &[F, E],
        ),
        falling_edge_none: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions::default(), callback, node_evaluation_context_factory),
            &[(d(0), &NONE), (d(0), &NONE)],
            &[F, F],
        ),
    );

    parameterized_test!(
        #[googletest::test]
        // Timeout is applied to `run_test`, so no need for #[sdv_telemetry_test_utils::timeout].
        #[tokio::test]
        edge_condition_checker_with_edge_options_for_booleans,
        |condition_checker_factory_factory,
         input: &[(Duration, &NodeValue)],
         want_sync: &[Result<bool, ()>],
         want_async: &[Duration]| async {
            // Run the test twice, once with `require_exact = true`, and once with `require_exact =
            // false`. For booleans, the `EdgeConditionChecker` should behave the same. Ideally,
            // we'd create separate tests, but that would be very verbose. To ensure that test
            // failures can be more easily attributed to each of the test runs, we print a message
            // before each run.

            eprintln!("Running test with require_exact = true");
            let condition_checker_factory = condition_checker_factory_factory(true);
            run_test(condition_checker_factory, input, want_sync, want_async).await;

            eprintln!("Running test with require_exact = false");
            let condition_checker_factory = condition_checker_factory_factory(false);
            run_test(condition_checker_factory, input, want_sync, want_async).await;
        },

        // Rising edge, then falling edge after `min_duration` has passed.
        both_edges_min_duration_1: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact }, EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE), /* trigger */ (d(4), &FALSE)],
            &[F, F, F],
            &[d(3), d(7)],
        ),
        // Rising edge, repeated `TRUE` messages, then falling edge after `min_duration` has passed,
        // then repeated `FALSE` messages.
        both_edges_min_duration_2: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact }, EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE), (d(0), &TRUE), (d(1), &TRUE), /* trigger */ (d(3), &TRUE), (d(0), &FALSE), (d(0), &FALSE), (d(1), &FALSE), /* trigger */ (d(4), &FALSE)],
            &[F, F, F, F, F, F, F, F, F],
            &[d(3), d(7)],
        ),
        // Rising edge, but then back to `FALSE` during `min_duration`.
        both_edges_min_duration_3: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact }, EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE), (d(1), &FALSE), (d(4), &FALSE)],
            &[F, F, F, F],
            &[],
        ),
        // Rising edge, then falling edge after `min_duration` has passed, then another immediate
        // rising edge. The falling edge options have no `min_duration`, hence the falling edge
        // should trigger immediately.
        both_edges_mixed_min_duration_1: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact }, EdgeOptions { min_duration: d(0), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE), /* trigger */ (d(4), &FALSE), /* trigger */ (d(0), &TRUE) /* trigger */],
            &[F, F, /* sync trigger */ T, F],
            &[d(3), d(7)],
        ),
        // Rising edge, but then back to `FALSE` during `min_duration`. Even though the falling edge
        // options have no `min_duration`, the falling edge should not trigger, since the rising
        // edge before it is discarded.
        both_edges_mixed_min_duration_2: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact }, EdgeOptions { min_duration: d(0), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE), (d(1), &FALSE)],
            &[F, F, F],
            &[],
        ),

        // Rising edge
        rising_edge_min_duration_1: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE)],
            &[F, F],
            &[d(3)],
        ),
        // Rising edge, then repeated `TRUE` messages.
        rising_edge_min_duration_2: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE), (d(0), &TRUE), (d(0), &TRUE), (d(1), &TRUE), (d(4), &TRUE)],
            &[F, F, F, F, F, F],
            &[d(3)],
        ),
        // Rising edge, but then back to `FALSE` during `min_duration`.
        rising_edge_min_duration_3: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &FALSE), (d(0), &TRUE), (d(1), &FALSE), (d(4), &FALSE)],
            &[F, F, F, F],
            &[],
        ),

        // Falling edge
        falling_edge_min_duration_1: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &TRUE), (d(0), &FALSE)],
            &[F, F],
            &[d(3)],
        ),
        // Falling edge, then repeated `FALSE` messages.
        falling_edge_min_duration_2: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &TRUE), (d(0), &FALSE), (d(0), &FALSE), (d(0), &FALSE), (d(1), &FALSE), (d(4), &FALSE)],
            &[F, F, F, F, F, F],
            &[d(3)],
        ),
        // Falling edge, but then back to `TRUE` during `min_duration`.
        falling_edge_min_duration_3: (
            |require_exact|
            move |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact }, callback, node_evaluation_context_factory),
            &[(d(0), &TRUE), (d(0), &FALSE), (d(1), &TRUE), (d(4), &TRUE)],
            &[F, F, F, F],
            &[],
        ),
    );

    parameterized_test!(
        #[googletest::test]
        // Timeout is applied to `run_test`, so no need for #[sdv_telemetry_test_utils::timeout].
        #[tokio::test]
        edge_condition_checker_with_edge_options,
        |condition_checker_factory,
            input: &[(Duration, &NodeValue)],
            want_sync: &[Result<bool, ()>],
            want_async: &[Duration]| async {
            run_test(condition_checker_factory, input, want_sync, want_async).await;
        },

        // Rising edge, with oscillating values during glitch monitoring, then falling edge, also
        // with oscillating values during glitch monitoring.
        both_edges_oscillating_values: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact: false }, EdgeOptions { min_duration: d(3), require_exact: false }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_0), (d(0), &INT_3), (d(1), &INT_1), (d(1), &INT_2), /* trigger */ (d(2), &INT_1), (d(0), &INT_0), (d(1), &INT_1), (d(0), &INT_0), /* trigger */ (d(3), &INT_0)],
            &[F, F, F, F, F, F, F, F, F],
            &[d(3), d(7)],
        ),
        // Rising edge, with oscillating values during glitch monitoring.
        rising_edge_oscillating_values: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact: false }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_0), (d(0), &INT_3), (d(1), &INT_1), (d(1), &INT_2)],
            &[F, F, F, F],
            &[d(3)],
        ),
        // Falling edge, with oscillating values during glitch monitoring.
        falling_edge_oscillating_values: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact: false }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_3), (d(0), &INT_0), (d(1), &INT_2), (d(1), &INT_1)],
            &[F, F, F, F],
            &[d(3)],
        ),

        // Rising edge that passes glitch filtering, then falling edge with rising edge immediately after.
        rising_edge_no_pause_before_next_edge: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact: false }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_0), (d(0), &INT_3), (d(4), &INT_1), (d(0), &INT_2)],
            &[F, F, F, F],
            &[d(3), d(7)],
        ),
        // Falling edge that passes glitch filtering, then rising edge with falling edge immediately after.
        falling_edge_no_pause_before_next_edge: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact: false }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_3), (d(0), &INT_0), (d(4), &INT_2), (d(0), &INT_1)],
            &[F, F, F, F],
            &[d(3), d(7)],
        ),

        // Rising edge with `require_exact` and all values match exactly, then falling edge, also
        // with all values matching exactly.
        both_edges_require_exact_all_match: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact: true }, EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_0), (d(0), &INT_3), (d(1), &INT_3), (d(3), &INT_1), (d(0), &INT_1), (d(1), &INT_1)],
            &[F, F, F, F, F, F],
            &[d(3), d(7)],
        ),
        // Rising edge with `require_exact` and all values match exactly.
        rising_edge_require_exact_all_match: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_0), (d(0), &INT_3), (d(1), &INT_3)],
            &[F, F, F],
            &[d(3)],
        ),
        // Falling edge with `require_exact` and all values match exactly.
        falling_edge_require_exact_all_match: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_3), (d(0), &INT_0), (d(1), &INT_0)],
            &[F, F, F],
            &[d(3)],
        ),

        // Rising edge with `require_exact` and not all values match exactly, then falling edge, also
        // with not all values matching exactly.
        both_edges_require_exact_not_all_match: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact: true }, EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_2), (d(0), &INT_3), (d(2), &INT_4), (d(0), &INT_2), (d(0), &INT_1), (d(1), &INT_0), (d(1), &INT_2)],
            &[F, F, F, F, F, F, F],
            &[],
        ),
        // Rising edge with `require_exact` and not all values match exactly.
        rising_edge_require_exact_not_all_match: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_0), (d(0), &INT_2), (d(2), &INT_3), (d(2), &INT_0)],
            &[F, F, F, F],
            &[],
        ),
        // Falling edge with `require_exact` and not all values match exactly.
        falling_edge_require_exact_not_all_match: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_3), (d(0), &INT_2), (d(2), &INT_0), (d(2), &INT_3)],
            &[F, F, F, F],
            &[],
        ),

        // Rising edge with `require_exact` and not all values match exactly, then a pause, then
        // falling edge, also with not all values matching exactly, then another pause.
        //
        // This tests that edge detection immediately "restarts" after a non-equal value is
        // detected.
        both_edges_require_exact_not_all_match_then_pause: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_both_edges(EdgeOptions { min_duration: d(3), require_exact: true }, EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_2), (d(0), &INT_3), (d(1), &INT_4), /* trigger (2 -> 4) */ (d(4), &INT_1), (d(0), &INT_0), (d(1), &INT_2) /* trigger (4 -> 2) */],
            &[F, F, F, F, F, F],
            &[d(4), d(9)],
        ),
        // Rising edge with `require_exact` and not all values match exactly, then a pause.
        //
        // This tests that edge detection immediately "restarts" after a non-equal value is
        // detected.
        rising_edge_require_exact_not_all_match_then_pause: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_rising_edge(EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_0), (d(0), &INT_2), (d(2), &INT_3) /* trigger (0 -> 3) */],
            &[F, F, F],
            &[d(5)],
        ),
        // Falling edge with `require_exact` and not all values match exactly, then a pause.
        //
        // This tests that edge detection immediately "restarts" after a non-equal value is
        // detected.
        falling_edge_require_exact_not_all_match_then_pause: (
            |callback, node_evaluation_context_factory|
            EdgeConditionChecker::new_falling_edge(EdgeOptions { min_duration: d(3), require_exact: true }, callback, node_evaluation_context_factory),
            &[(d(0), &INT_3), (d(0), &INT_2), (d(2), &INT_0) /* trigger (3 -> 2) */],
            &[F, F, F],
            &[d(5)],
        ),
    );
}
