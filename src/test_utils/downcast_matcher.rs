// Copyright 2025 Google LLC

use downcast_rs::Downcast;
use googletest::description::Description;
use googletest::matcher::{Matcher, MatcherBase, MatcherResult};
use std::any::type_name;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;

/// Matches a value that dereferences to `BaseT` and downcasts to `ConcreteT` matching the provided
/// `inner_matcher`.
///
/// Example:
/// ```
/// trait Base: Downcast + Debug {}
///
/// #[derive(Debug)]
/// struct Concrete {}
///
/// impl Base for Concrete {}
///
/// let value = Box::new(Concrete {}) as Box<dyn Base>;
///
/// assert_that!(value, downcast_ref::<dyn Base, Concrete, _>(pat!(&Concrete { .. })));
/// ```
pub fn downcast_ref<BaseT: ?Sized, ConcreteT, InnerMatcherT>(
    inner_matcher: InnerMatcherT,
) -> DowncastMatcher<BaseT, ConcreteT, InnerMatcherT> {
    DowncastMatcher { _base: PhantomData, _concrete: PhantomData, inner_matcher }
}

/// A matcher which matches a value that dereferences to `BaseT` and downcasts to `ConcreteT`
/// matching a given `inner_matcher`.
#[derive(MatcherBase)]
pub struct DowncastMatcher<BaseT: ?Sized, ConcreteT, InnerMatcherT> {
    _base: PhantomData<BaseT>,
    _concrete: PhantomData<ConcreteT>,
    inner_matcher: InnerMatcherT,
}

impl<
        'i,
        InputT: Deref<Target = BaseT> + Debug,
        BaseT: Downcast + Debug + ?Sized,
        ConcreteT: 'static + Debug,
        InnerMatcherT: Matcher<&'i ConcreteT>,
    > Matcher<&'i InputT> for DowncastMatcher<BaseT, ConcreteT, InnerMatcherT>
{
    fn matches(&self, actual: &'i InputT) -> MatcherResult {
        let Some(concrete) = self.downcast_ref(actual) else {
            return MatcherResult::NoMatch;
        };
        self.inner_matcher.matches(concrete)
    }

    fn describe(&self, matcher_result: MatcherResult) -> Description {
        match matcher_result {
            MatcherResult::Match => format!(
                "can be downcast from {} to {}, which {}",
                type_name::<BaseT>(),
                type_name::<ConcreteT>(),
                self.inner_matcher.describe(MatcherResult::Match)
            )
            .into(),
            MatcherResult::NoMatch => format!(
                "cannot be downcast from {} to {}, or which {}",
                type_name::<BaseT>(),
                type_name::<ConcreteT>(),
                self.inner_matcher.describe(MatcherResult::NoMatch)
            )
            .into(),
        }
    }

    fn explain_match(&self, actual: &'i InputT) -> Description {
        match self.downcast_ref(actual) {
            Some(actual) => Description::new()
                .text(format!(
                    "which can be downcast from {} to {}",
                    type_name::<BaseT>(),
                    type_name::<ConcreteT>(),
                ))
                .nested(self.inner_matcher.explain_match(actual)),
            None => Description::new().text(format!(
                "which cannot be downcast from {} to {}",
                type_name::<BaseT>(),
                type_name::<ConcreteT>(),
            )),
        }
    }
}

impl<BaseT: Downcast + Debug + ?Sized, ConcreteT: 'static + Debug, InnerMatcherT>
    DowncastMatcher<BaseT, ConcreteT, InnerMatcherT>
{
    fn downcast_ref<'i, InputT: Deref<Target = BaseT> + Debug>(
        &self,
        actual: &'i InputT,
    ) -> Option<&'i ConcreteT> {
        actual.deref().as_any().downcast_ref::<ConcreteT>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use downcast_rs::impl_downcast;
    use googletest::matchers::{anything, displays_as, err, not, pat, starts_with};
    use googletest::{expect_that, verify_that};

    trait Trait: Debug + Downcast {}

    impl_downcast!(Trait);

    #[derive(Debug)]
    struct ImplA {
        n: i32,
    }

    impl Trait for ImplA {}

    #[derive(Debug)]
    struct ImplB {}

    impl Trait for ImplB {}

    #[googletest::test]
    #[test]
    fn downcast_ref_works() {
        let a_123 = Box::new(ImplA { n: 123 }) as Box<dyn Trait>;

        expect_that!(a_123, downcast_ref::<dyn Trait, ImplA, _>(anything()));
        expect_that!(a_123, not(downcast_ref::<dyn Trait, ImplB, _>(anything())));

        expect_that!(a_123, downcast_ref::<dyn Trait, _, _>(pat!(ImplA { n: &123 })));
        expect_that!(a_123, not(downcast_ref::<dyn Trait, _, _>(pat!(ImplA { n: &77 }))));
    }

    #[googletest::test]
    #[test]
    fn downcast_ref_prints_good_description_on_failure() {
        let a_123 = Box::new(ImplA { n: 123 }) as Box<dyn Trait>;

        let trait_tn = type_name::<dyn Trait>();
        let impla_tn = type_name::<ImplA>();
        let implb_tn = type_name::<ImplB>();

        let result =
            verify_that!(a_123, downcast_ref::<dyn Trait, ImplA, _>(pat!(ImplA { n: &77 })));
        expect_that!(result, err(displays_as(starts_with(format!("Value of: a_123
Expected: can be downcast from {trait_tn} to {impla_tn}, which is ImplA which has field `n`, which is equal to 77
Actual: ImplA {{ n: 123 }},
  which can be downcast from {trait_tn} to {impla_tn}
    which has field `n`, which isn't equal to 77
")))));

        let result = verify_that!(a_123, downcast_ref::<dyn Trait, ImplB, _>(pat!(ImplB {})));
        expect_that!(
            result,
            err(displays_as(starts_with(format!(
                "Value of: a_123
Expected: can be downcast from {trait_tn} to {implb_tn}, which is ImplB {{}}
Actual: ImplA {{ n: 123 }},
  which cannot be downcast from {trait_tn} to {implb_tn}
"
            ))))
        );
    }
}
