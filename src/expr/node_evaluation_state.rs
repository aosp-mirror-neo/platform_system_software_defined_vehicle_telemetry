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

use dashmap::{mapref::entry::Entry as DashMapEntry, DashMap};
use futures::future::{self, BoxFuture};
use futures::FutureExt;
use sdv_telemetry_utils::{operation_result::OperationResult, value_holder::ValueHolder};
use std::sync::Arc;

use crate::dynamic_message::DynamicMessage;
use crate::node_error::MessageProviderError;
use crate::node_evaluation_context::NodeEvaluationContext;

/// Stores expression evaluation state to ensure expression evaluation uses the same message
/// in different branches.
pub struct NodeEvaluationState {
    context: Arc<NodeEvaluationContext>,
    sync_message_cache: DashMap<String, Option<Arc<dyn DynamicMessage>>>,
    async_message_cache: DashMap<
        String,
        future::Shared<
            BoxFuture<'static, Result<Option<Arc<dyn DynamicMessage>>, Arc<MessageProviderError>>>,
        >,
    >,
}

impl NodeEvaluationState {
    /// Creates a new instance of `NodeEvaluationState`.
    pub fn new(context: Arc<NodeEvaluationContext>) -> Self {
        Self { context, sync_message_cache: DashMap::new(), async_message_cache: DashMap::new() }
    }

    /// Returns a reference to the underlying node evaluation context.
    pub(super) fn get_context(&self) -> &Arc<NodeEvaluationContext> {
        &self.context
    }

    /// Retrieves publisher message from cache, or obtains it from the message factory
    /// and adds it to the cache if it's not present.
    pub(super) fn get_or_add(
        &self,
        publisher_name: &str,
        message_factory: impl FnOnce(
            &str,
        ) -> OperationResult<
            'static,
            Option<Arc<dyn DynamicMessage>>,
            MessageProviderError,
        >,
    ) -> OperationResult<'static, Option<Arc<dyn DynamicMessage>>, Arc<MessageProviderError>> {
        if let Some(msg) = self.context.get_message(publisher_name) {
            return Ok(Some(Arc::clone(msg))).into();
        }

        match self.sync_message_cache.entry(publisher_name.to_owned()) {
            DashMapEntry::Occupied(o) => Ok(o.get().as_ref().map(Arc::clone)).into(),
            DashMapEntry::Vacant(v_sync) => {
                match self.async_message_cache.entry(publisher_name.to_owned()) {
                    DashMapEntry::Occupied(o) => ValueHolder::Asynchronous(o.get().clone().boxed()),
                    DashMapEntry::Vacant(v_async) => {
                        match message_factory(publisher_name).map_err(Arc::new) {
                            ValueHolder::Synchronous(res) => {
                                if let Ok(maybe_msg) = &res {
                                    // Put message into synchronous message cache.
                                    v_sync.insert(maybe_msg.to_owned());
                                }
                                ValueHolder::Synchronous(res)
                            }
                            ValueHolder::Asynchronous(future) => {
                                let shared_future = future.shared();
                                // Put message into async message cache.
                                v_async.insert(shared_future.clone());
                                ValueHolder::Asynchronous(shared_future.boxed())
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::MessageFull;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::assert_matches;
    use std::future;

    use crate::mock::MockDynamicMessage;

    const KNOWN_PUBLISHER_NAME: &str = "publisher_foo";

    #[test]
    fn get_or_add_sync_message_from_context_success() {
        let msg =
            Arc::new(MockDynamicMessage::new(TestMessage::descriptor())) as Arc<dyn DynamicMessage>;
        let context = Arc::new(NodeEvaluationContext::with_publisher_message(
            None,
            KNOWN_PUBLISHER_NAME.to_owned(),
            Arc::clone(&msg),
        ));
        let state = NodeEvaluationState::new(context);

        let result =
            state.get_or_add(KNOWN_PUBLISHER_NAME, |_| panic!("Should not call message factory"));
        let res_msg = result.unwrap_sync().unwrap().unwrap();

        assert!(Arc::ptr_eq(&res_msg, &msg), "Unexpected message: {res_msg:?}");
    }

    #[test]
    fn get_or_add_sync_message_from_cache_success() {
        let msg =
            Arc::new(MockDynamicMessage::new(TestMessage::descriptor())) as Arc<dyn DynamicMessage>;
        let context = Arc::new(NodeEvaluationContext::new(None));
        let state = NodeEvaluationState::new(Arc::clone(&context));
        state.sync_message_cache.insert(KNOWN_PUBLISHER_NAME.to_owned(), Some(Arc::clone(&msg)));

        let result =
            state.get_or_add(KNOWN_PUBLISHER_NAME, |_| panic!("Should not call message factory"));
        let res_msg = result.unwrap_sync().unwrap().unwrap();

        assert!(Arc::ptr_eq(&res_msg, &msg), "Unexpected message: {res_msg:?}");
    }

    #[test]
    fn get_or_add_sync_message_from_message_factory_success() {
        let msg =
            Arc::new(MockDynamicMessage::new(TestMessage::descriptor())) as Arc<dyn DynamicMessage>;
        let context = Arc::new(NodeEvaluationContext::new(None));
        let state = NodeEvaluationState::new(Arc::clone(&context));

        let result = state.get_or_add(KNOWN_PUBLISHER_NAME, |_| Ok(Some(Arc::clone(&msg))).into());
        let res_msg = result.unwrap_sync().unwrap().unwrap();
        let cached_value = state.sync_message_cache.get(KNOWN_PUBLISHER_NAME).unwrap();

        assert!(Arc::ptr_eq(&res_msg, &msg), "Unexpected message: {res_msg:?}");
        assert!(
            Arc::ptr_eq(cached_value.as_ref().unwrap(), &msg),
            "Unexpected message: {:?}",
            cached_value.as_ref().unwrap()
        )
    }

    #[test]
    fn get_or_add_sync_message_from_message_factory_fail() {
        let context = Arc::new(NodeEvaluationContext::new(None));
        let state = NodeEvaluationState::new(Arc::clone(&context));

        let result = state.get_or_add(KNOWN_PUBLISHER_NAME, |_| {
            Err(MessageProviderError::InvalidPublisherName).into()
        });
        let err = result.unwrap_sync().unwrap_err();

        assert_matches!(err.as_ref(), MessageProviderError::InvalidPublisherName);
        assert!(state.sync_message_cache.get(KNOWN_PUBLISHER_NAME).is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_or_add_async_message_from_cache_success() {
        let msg =
            Arc::new(MockDynamicMessage::new(TestMessage::descriptor())) as Arc<dyn DynamicMessage>;
        let context = Arc::new(NodeEvaluationContext::new(None));
        let state = NodeEvaluationState::new(Arc::clone(&context));
        let msg_clone = Arc::clone(&msg);
        let future = future::ready(Ok(Some(msg_clone))).boxed().shared();
        state.async_message_cache.insert(KNOWN_PUBLISHER_NAME.to_owned(), future);

        let result =
            state.get_or_add(KNOWN_PUBLISHER_NAME, |_| panic!("Should not call message factory"));
        let res_msg = result.unwrap_async().await.unwrap().unwrap();

        assert!(Arc::ptr_eq(&res_msg, &msg), "Unexpected message: {res_msg:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_or_add_async_message_from_message_factory_success() {
        let msg =
            Arc::new(MockDynamicMessage::new(TestMessage::descriptor())) as Arc<dyn DynamicMessage>;
        let context = Arc::new(NodeEvaluationContext::new(None));
        let state = NodeEvaluationState::new(Arc::clone(&context));

        let msg_clone = Arc::clone(&msg);
        let result = state.get_or_add(KNOWN_PUBLISHER_NAME, |_| {
            future::ready(Ok(Some(msg_clone))).boxed().into()
        });
        let res_msg = result.unwrap_async().await.unwrap().unwrap();
        let cached_future = state.async_message_cache.get(KNOWN_PUBLISHER_NAME).unwrap();
        let cached_msg = cached_future.value().clone().await.unwrap().unwrap();

        assert!(Arc::ptr_eq(&res_msg, &msg), "Unexpected message: {res_msg:?}");
        assert!(Arc::ptr_eq(&cached_msg, &msg), "Unexpected message: {cached_msg:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_or_add_async_message_from_message_factory_fail() {
        let context = Arc::new(NodeEvaluationContext::new(None));
        let state = NodeEvaluationState::new(Arc::clone(&context));

        let err = state
            .get_or_add(KNOWN_PUBLISHER_NAME, |_| {
                future::ready(Err(MessageProviderError::InvalidPublisherName)).boxed().into()
            })
            .unwrap_async()
            .await
            .unwrap_err();
        let cached_err =
            state.async_message_cache.get(KNOWN_PUBLISHER_NAME).unwrap().clone().await.unwrap_err();

        assert_matches!(err.as_ref(), MessageProviderError::InvalidPublisherName);
        assert_matches!(cached_err.as_ref(), MessageProviderError::InvalidPublisherName);
    }

    #[test]
    fn get_or_add_sync_message_not_found_returns_none() {
        let context = Arc::new(NodeEvaluationContext::new(None));
        let state = NodeEvaluationState::new(context);

        let result = state.get_or_add(KNOWN_PUBLISHER_NAME, |_| Ok(None).into());
        let res = result.unwrap_sync().unwrap();

        assert!(res.is_none());
    }
}
