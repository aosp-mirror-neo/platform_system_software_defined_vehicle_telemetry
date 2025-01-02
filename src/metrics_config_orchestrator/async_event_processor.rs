// Copyright 2024 Google LLC

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;

pub(super) enum AsyncReceiver<T> {
    Bounded(Receiver<T>),
    Unbounded(UnboundedReceiver<T>),
}

impl<T> AsyncReceiver<T> {
    async fn recv(&mut self) -> Option<T> {
        match self {
            AsyncReceiver::Bounded(ref mut receiver) => receiver.recv().await,
            AsyncReceiver::Unbounded(ref mut receiver) => receiver.recv().await,
        }
    }
}

/// Receives and processes events asynchronously in a background task.
pub(super) struct AsyncEventProcessor {
    _cancellation_drop_guard: tokio_util::sync::DropGuard,
}

#[async_trait]
pub(super) trait AsyncEventHandler<TEvent> {
    async fn handle(&mut self, event: TEvent);
}

impl AsyncEventProcessor {
    /// Creates a processor that asynchronously processes incoming events in a background task
    /// using the provided callback.
    ///
    /// The background task is cancelled and the receiver is dropped when the processor is dropped.
    pub(super) fn new<TEvent: Send + 'static>(
        mut receiver: AsyncReceiver<TEvent>,
        mut handler: impl AsyncEventHandler<TEvent> + Send + 'static,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Discard pending messages if the processor was dropped.
                    biased;
                    _ = cancellation_token_clone.cancelled() => break,
                    maybe_event = receiver.recv() => {
                        if let Some(event) = maybe_event {
                            handler.handle(event).await;
                        } else {
                            // Sender was dropped, so there will be no more events.
                            break;
                        }
                    }
                }
            }
        });

        Self { _cancellation_drop_guard: cancellation_token.drop_guard() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct TestEventHandler {
        event_count: Arc<AtomicU32>,
    }

    #[async_trait]
    impl AsyncEventHandler<()> for TestEventHandler {
        async fn handle(&mut self, _event: ()) {
            self.event_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl TestEventHandler {
        fn new() -> Self {
            Self { event_count: Arc::new(0.into()) }
        }

        fn get_event_count(&self) -> u32 {
            self.event_count.load(Ordering::Acquire)
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn process_events_success() {
        let (tx, rx) = mpsc::unbounded_channel();
        let handler = TestEventHandler::new();
        let _processor = AsyncEventProcessor::new(AsyncReceiver::Unbounded(rx), handler.clone());

        tx.send(()).unwrap();
        tx.send(()).unwrap();
        tx.send(()).unwrap();
        tokio::task::yield_now().await;

        assert_eq!(handler.get_event_count(), 3);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn process_events_with_bounded_receiver_success() {
        let (tx, rx) = mpsc::channel(2);
        let handler = TestEventHandler::new();
        let _processor = AsyncEventProcessor::new(AsyncReceiver::Bounded(rx), handler.clone());

        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        tokio::task::yield_now().await;

        assert_eq!(handler.get_event_count(), 3);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn drop_success() {
        let (tx, rx) = mpsc::unbounded_channel();
        let handler = TestEventHandler::new();

        {
            let _processor =
                AsyncEventProcessor::new(AsyncReceiver::Unbounded(rx), handler.clone());
            // `processor` is dropped here, so the background task should be cancelled.
        }
        // Before task is cancelled, send should be successful, but the event should not get processed.
        tx.send(()).unwrap();
        // Yield to process task cancellation.
        tokio::task::yield_now().await;

        // After task cancellation send should fail.
        assert!(tx.send(()).is_err());
        assert_eq!(handler.get_event_count(), 0);
    }
}
