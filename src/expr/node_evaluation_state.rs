// Copyright 2024 Google LLC

use futures::future::{self, BoxFuture};
use protobuf::MessageDyn;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::sync::Arc;

use crate::expr::node_error::MessageProviderError;
use crate::expr::node_evaluation_context::NodeEvaluationContext;

/// Stores expression evaluation state to ensure expression evaluation uses the same message
/// in different branches.
pub(super) struct NodeEvaluationState {
    context: Arc<NodeEvaluationContext>,
    sync_message_cache: HashMap<String, Option<Arc<dyn MessageDyn>>>,
    async_message_cache: HashMap<
        String,
        future::Shared<
            BoxFuture<'static, Result<Option<Arc<dyn MessageDyn>>, Arc<MessageProviderError>>>,
        >,
    >,
}

impl Default for NodeEvaluationState {
    fn default() -> Self {
        NodeEvaluationState::new(NodeEvaluationContext::new().into())
    }
}

impl NodeEvaluationState {
    /// Creates a new instance of `NodeEvaluationState`.
    pub(super) fn new(context: Arc<NodeEvaluationContext>) -> Self {
        Self { context, sync_message_cache: HashMap::new(), async_message_cache: HashMap::new() }
    }

    /// Returns a reference to the underlying node evaluation context.
    pub(super) fn get_context(&self) -> &Arc<NodeEvaluationContext> {
        &self.context
    }

    /// Returns a synchronously available message if found in the state.
    pub(super) fn get_sync_message(
        &self,
        publisher_name: &str,
    ) -> Option<Option<&Arc<dyn MessageDyn>>> {
        let context_message = self.context.get_message(publisher_name);
        if context_message.is_some() {
            return Some(context_message);
        }

        self.sync_message_cache.get(publisher_name).map(Option::as_ref)
    }

    /// Returns a future to an asynchronously available message if found in the state.
    pub(super) fn get_async_message(
        &self,
        publisher_name: &str,
    ) -> Option<
        &future::Shared<
            BoxFuture<'static, Result<Option<Arc<dyn MessageDyn>>, Arc<MessageProviderError>>>,
        >,
    > {
        self.async_message_cache.get(publisher_name)
    }

    /// Caches a synchronously available message in the state.
    ///
    /// The cache is expected to contain only one message per publisher.
    pub(super) fn cache_sync_message(
        &mut self,
        publisher_name: String,
        message: Option<Arc<dyn MessageDyn>>,
    ) {
        self.sync_message_cache
            .try_insert_value(publisher_name, message)
            .expect("Only one message per publisher should be added into sync cache");
    }

    /// Caches an asynchronously available message in the state.
    ///
    /// The cache is expected to contain only one message per publisher.
    pub(super) fn cache_async_message(
        &mut self,
        publisher_name: String,
        message: future::Shared<
            BoxFuture<'static, Result<Option<Arc<dyn MessageDyn>>, Arc<MessageProviderError>>>,
        >,
    ) {
        self.async_message_cache
            .try_insert_value(publisher_name, message)
            .expect("Only one message per publisher should be added into async cache");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::FutureExt;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageDyn;

    const KNOWN_PUBLISHER_NAME: &str = "publisher_foo";

    #[test]
    fn get_sync_message_from_context_success() {
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() });
        let context = Arc::new(NodeEvaluationContext::with_publisher_message(
            KNOWN_PUBLISHER_NAME.to_owned(),
            Arc::<Int32Value>::clone(&msg),
        ));
        let state = NodeEvaluationState::new(context);

        let res_msg = state.get_sync_message(KNOWN_PUBLISHER_NAME).unwrap().unwrap();

        assert!(Arc::ptr_eq(res_msg, &(msg as Arc<dyn MessageDyn>)));
    }

    #[test]
    fn get_sync_message_from_cache_success() {
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() });
        let mut state = NodeEvaluationState::new(NodeEvaluationContext::new().into());
        state.cache_sync_message(
            KNOWN_PUBLISHER_NAME.to_owned(),
            Some(Arc::<Int32Value>::clone(&msg)),
        );

        let res_msg = state.get_sync_message(KNOWN_PUBLISHER_NAME).unwrap().unwrap();

        assert!(Arc::ptr_eq(res_msg, &(msg as Arc<dyn MessageDyn>)));
    }

    #[test]
    fn get_sync_message_not_found_returns_none() {
        let state = NodeEvaluationState::new(NodeEvaluationContext::new().into());

        let res = state.get_sync_message(KNOWN_PUBLISHER_NAME);

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_async_message_from_cache_success() {
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() });
        let mut state = NodeEvaluationState::new(NodeEvaluationContext::new().into());
        let msg_clone = Arc::clone(&msg);
        let future = async move { Ok(Some(msg_clone as Arc<dyn MessageDyn>)) }.boxed().shared();
        state.cache_async_message(KNOWN_PUBLISHER_NAME.to_owned(), future);

        let res_msg =
            state.get_async_message(KNOWN_PUBLISHER_NAME).unwrap().clone().await.unwrap().unwrap();

        assert!(Arc::ptr_eq(&res_msg, &(msg as Arc<dyn MessageDyn>)));
    }

    #[test]
    fn get_async_message_not_found_returns_none() {
        let state = NodeEvaluationState::new(NodeEvaluationContext::new().into());

        let res = state.get_async_message(KNOWN_PUBLISHER_NAME);

        assert!(res.is_none());
    }
}
