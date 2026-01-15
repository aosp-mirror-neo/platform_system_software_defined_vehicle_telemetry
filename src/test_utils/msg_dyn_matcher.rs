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

impl<'a, A: Deref<Target = dyn MessageDyn> + Debug> Matcher<&'a A> for MessageDynMatcher<'_> {
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
