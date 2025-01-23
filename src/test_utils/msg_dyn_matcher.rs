// Copyright 2025 Google LLC

use googletest::description::Description;
use googletest::matcher::{Matcher, MatcherBase, MatcherResult};
use protobuf::{reflect::ReflectEqMode, MessageDyn};
use std::fmt::Debug;
use std::ops::Deref;

/// Googletest matcher for values dereferencing to `dyn MessageDyn` (e.g., `&dyn MessageDyn`,
/// `&Arc<dyn MessageDyn>`, ...).
pub fn msg_dyn_eq<'e>(
    expected: &'e dyn MessageDyn,
    eq_mode: &'e ReflectEqMode,
) -> MessageDynMatcher<'e> {
    MessageDynMatcher { expected, eq_mode }
}

/// Googletest matcher for values dereferencing to `dyn MessageDyn` (e.g., `&dyn MessageDyn`,
/// `&Arc<dyn MessageDyn>`, ...).
#[derive(MatcherBase)]
pub struct MessageDynMatcher<'e> {
    expected: &'e dyn MessageDyn,
    eq_mode: &'e ReflectEqMode,
}

impl<'a, 'e, A: Deref<Target = dyn MessageDyn> + Debug> Matcher<&'a A> for MessageDynMatcher<'e> {
    fn matches(&self, actual: &'a A) -> MatcherResult {
        actual.reflect_eq_dyn(self.expected, self.eq_mode).into()
    }

    fn describe(&self, matcher_result: MatcherResult) -> Description {
        match matcher_result {
            MatcherResult::Match => {
                format!("is equal (using {:?}) to {:?}", self.eq_mode, self.expected).into()
            }
            MatcherResult::NoMatch => {
                format!("isn't equal (using {:?}) to {:?}", self.eq_mode, self.expected).into()
            }
        }
    }
}
