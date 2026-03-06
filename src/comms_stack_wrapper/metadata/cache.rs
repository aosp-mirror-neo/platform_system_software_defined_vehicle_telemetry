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

use anyhow::Context;
use futures::future;
use futures::future::BoxFuture;
use futures::future::OptionFuture;
use futures::future::Shared;
use futures::stream::BoxStream;
use futures::Stream;
use futures::StreamExt;
use log::error;
use log::log_enabled;
use log::trace;
use sdv_comms::sd::ServiceDiscoveryManager;
use sdv_comms::sd::ServiceUnitChangeEvent;
use sdv_comms::sd::ServiceUnitDefinition;
use sdv_comms::sd::UnitNameDiscoveryArgs;
use sdv_comms::ContextRef;
use sdv_config_provider::vsidl_provider_api::VsidlConfigProvider;
use sdv_telemetry_comms_utils::debug_format_service_unit_definition;
use sdv_telemetry_comms_utils::ServiceName;
use sdv_telemetry_utils::async_lazy_map::AsyncLazyMap;
use std::pin::pin;
use std::sync::Arc;
use tokio::select;
use tokio::sync::watch;
use tokio::sync::OwnedMappedMutexGuard;
use tokio_stream::wrappers::WatchStream;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;

use super::fetch_metadata;
use super::CurrentMetadataGenerations;
use super::MetadataGeneration;
use super::MetadataGenerationEvent;
use super::MetadataGenerationId;
use super::MetadataServiceType;
use crate::comms_stack_accessor::CommsStackAccessor;

type VsidlProviderFuture =
    Shared<BoxFuture<'static, Option<Arc<dyn VsidlConfigProvider + Send + Sync>>>>;

/// A mutex guard that holds a reference to the current metadata generation for a service.
#[clippy::has_significant_drop]
#[must_use = "if unused the Mutex will immediately unlock"]
pub(crate) struct MetadataMutexGuard(
    OwnedMappedMutexGuard<
        Option<(watch::Sender<CurrentMetadataGenerations>, DropGuard)>,
        watch::Sender<CurrentMetadataGenerations>,
    >,
);

impl MetadataMutexGuard {
    fn new(
        mutex_guard: OwnedMappedMutexGuard<
            Option<(watch::Sender<CurrentMetadataGenerations>, DropGuard)>,
            watch::Sender<CurrentMetadataGenerations>,
        >,
    ) -> Self {
        Self(mutex_guard)
    }

    /// Returns a stream of `MetadataGeneration`s that are updated whenever the metadata generation
    /// for the service changes. The stream always immediately yields the current metadata
    /// generation (if any).
    pub(crate) fn subscribe(&self) -> impl Stream<Item = Option<MetadataGeneration>> {
        WatchStream::new(self.0.subscribe()).map(|generations| generations.get_owned())
    }

    /// Borrows the metadata generation for the service, and, if it is `Some(_)` applies the given
    /// function to it and returns the result. If the metadata generation is `None`, returns `None`.
    pub(crate) fn borrow_map<T>(&self, f: impl FnOnce(&MetadataGeneration) -> T) -> Option<T> {
        // Ideally, we'd just return `self.0.borrow().get()` here, but that doesn't work for
        // lifetime reasons.
        self.0.borrow().get().map(f)
    }
}

/// A cache for `MetadataGeneration`s that automatically keeps them up-to-date by monitoring the
/// service unit change stream and re-fetching metadata when necessary.
#[derive(Debug)]
pub(crate) struct MetadataCache {
    cache: Arc<
        AsyncLazyMap<
            ServiceName,
            (
                // `watch::Sender`, effectively, allows us to:
                // - set the latest value
                // - get the latest value
                // - subscribe to changes
                watch::Sender<CurrentMetadataGenerations>,
                // Guards the cancellation of the unit change stream, which is spawned in a
                // background task to update the above sender on changes.
                DropGuard,
            ),
        >,
    >,
    vsidl_provider: VsidlProviderFuture,
}

impl MetadataCache {
    /// Creates a new `MetadataCache`.
    pub(crate) fn new(vsidl_provider: VsidlProviderFuture) -> Self {
        Self { cache: Arc::new(AsyncLazyMap::new()), vsidl_provider }
    }

    /// Checks if the cache contains an entry for the given `service_name` that is initialized and
    /// contains a metadata generation.
    #[cfg(all(test, integration_tests))]
    pub(crate) async fn has_generation(&self, service_name: &ServiceName) -> bool {
        self.cache.get(service_name).await.is_some_and(|mutex_guard| {
            let (ref tx, ref _unit_change_stream_drop_guard) = *mutex_guard;
            tx.borrow().get().is_some()
        })
    }

    /// Returns the current generation for the given `service_name`, fetching it if it's
    /// not already cached.
    pub(crate) async fn get_or_fetch_current(
        &self,
        ctx: ContextRef,
        service_name: ServiceName,
    ) -> anyhow::Result<MetadataMutexGuard> {
        self.cache
            .get_or_try_init(service_name.clone(), || async move {
                // Step 1: Create a stream of metadata generations for the given service name.
                let metadata_generations_stream =
                    self.create_metadata_generations_stream(ctx, service_name.clone()).await?;

                // Step 2: Try to fetch the initial metadata generation from service discovery. This
                // will be the first value sent on the metadata generation stream.
                let maybe_initial_metadata_generation = OptionFuture::from(
                    CommsStackAccessor::discover_service(ctx, &service_name).await.map(
                        |service_unit_definition| {
                            Self::try_fetch_metadata_generation(
                                &service_name,
                                service_unit_definition,
                                self.vsidl_provider.clone(),
                            )
                        },
                    ),
                )
                .await
                .transpose()?;

                // Step 3: Ensure we ignore the first metadata generation received from
                // `metadata_generations_stream` if it is equal to the one queried via
                // service discovery in step 2.
                //
                // If the publisher is already available, creating the stream in step 1
                // immediately yields a `CurrentMetadataGenerations(vec![metadata_generation])` due
                // to service discovery sending us a `Registered` event for the service. Step 2 then
                // fetches that same metadata generation via service discovery. Since step 4
                // initializes the `watch::channel` with the generation from step 2, this step
                // deduplicates the stream's first item to avoid processing it twice.
                let mut maybe_initial_metadata_generation_clone =
                    maybe_initial_metadata_generation.clone();
                let metadata_generations_stream =
                    metadata_generations_stream.filter_map(move |metadata_generations| {
                        let Some(initial_metadata_generation) =
                            maybe_initial_metadata_generation_clone.take()
                        else {
                            return future::ready(Some(metadata_generations));
                        };
                        // If we reach this point, then:
                        // 1. `initial_metadata_generation` was discovered through service
                        //    discovery.
                        // 2. We are in the very first iteration of the stream (because
                        //    `Option::take` above guarantees that `initial_metadata_generation` is
                        //    `None` after the first iteration).

                        // If the stream contains exactly one metadata generation (it cannot contain
                        // more than 1 because this is the first iteration)...
                        let Some(metadata_generation) = metadata_generations.get() else {
                            return future::ready(Some(metadata_generations));
                        };
                        // ...and the metadata generation in the stream is equal to the initial
                        // metadata generation...
                        if metadata_generation.eq_ignoring_id(&initial_metadata_generation) {
                            // ... then drop the stream's item without notifying subscribers.
                            return future::ready(None);
                        }
                        future::ready(Some(metadata_generations))
                    });

                // Step 4: Initialize the watch channel with the initial metadata generation.
                let (tx, _) = watch::channel(CurrentMetadataGenerations::new(
                    maybe_initial_metadata_generation
                        .map_or_else(Vec::new, |metadata_generation| vec![metadata_generation]),
                ));

                // Step 5: Spawn a background task to monitor the unit change stream and update the
                // cached metadata generations.
                let cancellation_token = CancellationToken::new();
                let unit_change_stream_drop_guard = cancellation_token.clone().drop_guard();
                let cache_clone = Arc::clone(&self.cache);
                tokio::spawn(async move {
                    let mut metadata_generations_stream = pin!(metadata_generations_stream);
                    while let Some(new_generations) = select! {
                        biased;
                        _ = cancellation_token.cancelled() => None,
                        new_generations = metadata_generations_stream.next() => new_generations,
                    } {
                        let Some(mut cache_entry_mutex_guard) =
                            cache_clone.get(&service_name).await
                        else {
                            // This should never happen, since we never clear the cache. And even if we
                            // did, the cancellation token should fire imminently, and the loop should
                            // end that way.
                            continue;
                        };
                        let (ref mut tx, ref _unit_change_stream_drop_guard) =
                            *cache_entry_mutex_guard;
                        tx.send_replace(new_generations);
                        drop(cache_entry_mutex_guard);
                    }
                });

                Ok((tx, unit_change_stream_drop_guard))
            })
            .await
            .map(|mutex_guard| {
                MetadataMutexGuard::new(OwnedMappedMutexGuard::map(
                    mutex_guard,
                    |(tx, _unit_change_stream_drop_guard)| tx,
                ))
            })
    }

    /// Removes the given `generation_id` for `service_name` from the cache. The cached generation
    /// is only removed if it matches the given id. Does _not_ stop monitoring the unit change
    /// stream for metadata updates.
    pub(crate) async fn remove_generation(
        &self,
        service_name: &ServiceName,
        generation_id: MetadataGenerationId,
    ) -> bool {
        let Some(mut mutex_guard) = self.cache.get(service_name).await else {
            return false;
        };
        let (ref mut tx, ref _unit_change_stream_drop_guard) = *mutex_guard;
        let was_removed = tx.send_if_modified(|generations| {
            // Only remove the generation if it matches the given id. If it does, remove it, and
            // return `true` to indicate that the value has changed, which will notify any
            // subscribers.
            generations.handle(MetadataGenerationEvent::RemoveById(generation_id))
        });
        drop(mutex_guard);

        was_removed
    }

    async fn try_fetch_metadata_generation(
        service_name: &ServiceName,
        service_unit_definition: ServiceUnitDefinition,
        vsidl_provider: VsidlProviderFuture,
    ) -> anyhow::Result<MetadataGeneration> {
        let metadata =
            fetch_metadata(service_name, &service_unit_definition, move || vsidl_provider).await?;

        Ok(MetadataGeneration::new(
            MetadataServiceType::new(&service_unit_definition.get_unit_metadata()),
            service_unit_definition.get_unit_identifier(),
            metadata,
        ))
    }

    async fn create_metadata_generations_stream(
        &self,
        ctx: ContextRef,
        service_name: ServiceName,
    ) -> anyhow::Result<impl Stream<Item = CurrentMetadataGenerations>> {
        let vsidl_provider_clone = self.vsidl_provider.clone();
        let service_name_clone = service_name.clone();

        let unit_change_stream = Self::create_unit_change_stream(ctx, service_name.clone()).await?;
        let metadata_generation_stream = unit_change_stream
            .inspect(move |event| Self::trace_service_discovery_event("passed-through", &service_name, event))
            .filter_map(move |event| {
                let service_name_clone = service_name_clone.clone();
                let vsidl_provider_clone = vsidl_provider_clone.clone();
                async move {
                    match event {
                        ServiceUnitChangeEvent::Changed(service_unit_definition)
                        | ServiceUnitChangeEvent::Registered(service_unit_definition) => {
                            let Ok(metadata_generation) = Self::try_fetch_metadata_generation(
                                &service_name_clone,
                                service_unit_definition,
                                vsidl_provider_clone.clone(),
                            )
                            .await
                            .inspect_err(
                                |err| error!("Failed to re-fetch metadata for '{service_name_clone}'. Continuing with possibly stale metadata. Error: {err:#?}")
                            ) else {
                                // If we fail to fetch the metadata, do not notify the subscribers. They
                                // will continue using the last known good metadata generation.
                                return None;
                            };
                            Some(MetadataGenerationEvent::Update(metadata_generation))
                        }
                        ServiceUnitChangeEvent::Unregistered(service_unit_definition) => {
                            Some(MetadataGenerationEvent::RemoveByUnitIdentifierAndServiceType {
                                unit_identifier: service_unit_definition.get_unit_identifier(),
                                service_type: MetadataServiceType::new(&service_unit_definition.get_unit_metadata()),
                            })
                        },
                    }
                }
            })
            .scan(CurrentMetadataGenerations::new(vec![]), move |metadata_generations, event| {
                // Update the metadata generations based on the event.
                let was_updated = metadata_generations.handle(event);

                // Return `Some(Some(_))` if metadata generations were updated. Return `Some(None)`
                // if they were not updated. The wrapping in the outer `Some` is necessary, because
                // `StreamExt::scan` stops if we return `None`. The inner `Some/None` is necessary
                // so that we can ignore events that didn't update anything in the `filter_map`
                // below.
                future::ready(Some(if was_updated { Some(metadata_generations.clone()) } else { None }))
            })
            // Ignore `None`s produced above when the metadata generations didn't change.
            .filter_map(future::ready);
        Ok(metadata_generation_stream)
    }

    async fn create_unit_change_stream(
        ctx: ContextRef,
        service_name: ServiceName,
    ) -> anyhow::Result<BoxStream<'static, ServiceUnitChangeEvent>> {
        let sd = ServiceDiscoveryManager::new(ctx);

        let service_name_clone = service_name.clone();
        Ok(match service_name {
            ServiceName::UnitTypeBased { unit_type, unit_name } => sd
                .subscribe_service_unit_change_by_type(&unit_type)
                .await
                .with_context(|| {
                    format!("Failed to subscribe to changes for unit type {unit_type}")
                })?
                .inspect(move |event| {
                    Self::trace_service_discovery_event("incoming", &service_name_clone, event)
                })
                .filter(move |event| {
                    future::ready(match event {
                        ServiceUnitChangeEvent::Registered(service_unit)
                        | ServiceUnitChangeEvent::Unregistered(service_unit)
                        | ServiceUnitChangeEvent::Changed(service_unit) => {
                            unit_name.as_ref().is_none_or(|unit_name| {
                                service_unit.get_unit_identifier().get_unit_name() == unit_name
                            })
                        }
                    })
                })
                .boxed(),
            ServiceName::FqinBased { fqin, unit_name } => {
                let name_args = UnitNameDiscoveryArgs::new_builder()
                    .set_sdv_package_name(fqin.get_sdv_package_name())
                    .set_service_bundle_name(fqin.get_service_bundle_name())
                    .set_service_unit_name(unit_name.clone())
                    .set_sdv_vm_name(fqin.get_sdv_vm_name())
                    .build()
                    .with_context(|| {
                        format!("Failed to build UnitNameDiscoveryArgs for {fqin}|{unit_name}")
                    })?;
                sd.subscribe_service_unit_change_by_name(&name_args)
                    .await
                    .with_context(|| {
                        format!("Failed to subscribe to changes for service {fqin}|{unit_name}")
                    })?
                    .inspect(move |event| {
                        Self::trace_service_discovery_event("incoming", &service_name_clone, event)
                    })
                    .filter(move |event| {
                        future::ready(match event {
                            ServiceUnitChangeEvent::Registered(service_unit)
                            | ServiceUnitChangeEvent::Unregistered(service_unit)
                            | ServiceUnitChangeEvent::Changed(service_unit) => {
                                service_unit
                                    .get_service_bundle_identity()
                                    .get_fqin()
                                    .get_service_instance_name()
                                    == fqin.get_service_instance_name()
                            }
                        })
                    })
                    .boxed()
            }
        })
    }

    fn trace_service_discovery_event(
        processing_step: &'static str,
        service_name: &ServiceName,
        event: &ServiceUnitChangeEvent,
    ) {
        if log_enabled!(log::Level::Trace) {
            let event = match event {
                ServiceUnitChangeEvent::Changed(service_unit_definition) => format!(
                    "Changed({})",
                    debug_format_service_unit_definition!(service_unit_definition)
                ),
                ServiceUnitChangeEvent::Registered(service_unit_definition) => format!(
                    "Registered({})",
                    debug_format_service_unit_definition!(service_unit_definition)
                ),
                ServiceUnitChangeEvent::Unregistered(service_unit_definition) => format!(
                    "Unregistered({})",
                    debug_format_service_unit_definition!(service_unit_definition)
                ),
            };
            trace!(
            "Received {processing_step} service unit change event for '{service_name}': {event}"
        );
        }
    }
}

#[cfg(all(test, integration_tests))]
mod tests {
    use super::*;

    use futures::future;
    use futures::future::FutureExt;
    use googletest::expect_that;
    use googletest::matchers::{anything, eq, is_false, is_true, none, not, pat, some};
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageFull;
    use sdv_comms::dt;
    use sdv_comms::id::UnitIdentifier;
    use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
    use std::pin::pin;

    use crate::test_utils;

    fn empty_vsidl_provider() -> VsidlProviderFuture {
        futures::future::ready(None).boxed().shared()
    }

    async fn register_test_topic(ctx: ContextRef, service_name: &str) -> dt::Publisher {
        test_utils::register_topic(
            ctx,
            service_name,
            MessageMetadata {
                file_descriptors: vec![Int32Value::descriptor().file_descriptor().proto().clone()],
                message_full_name: Int32Value::descriptor().full_name().to_owned(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn get_or_fetch_current_success() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let cache = MetadataCache::new(empty_vsidl_provider());
        expect_that!(cache.has_generation(&service_name).await, is_false());

        let _publisher = register_test_topic(ctx, &service_name_string).await;

        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let stream = mutex_guard.subscribe();
        drop(mutex_guard);

        let generation =
            pin!(stream.filter_map(|maybe_generation| async move { maybe_generation }))
                .next()
                .await
                .unwrap();

        expect_that!(
            generation.metadata.message_descriptor.full_name(),
            eq(Int32Value::descriptor().full_name())
        );
        expect_that!(generation.service_type, eq(MetadataServiceType::Dt));

        expect_that!(cache.has_generation(&service_name).await, is_true());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn get_or_fetch_current_removes_on_unregistration() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let publisher = register_test_topic(ctx, &service_name_string).await;

        let cache = MetadataCache::new(empty_vsidl_provider());

        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let mut stream = mutex_guard.subscribe();
        drop(mutex_guard);

        // Wait for the generation to be added to the cache.
        pin!(stream.by_ref().filter(|maybe_generation| future::ready(maybe_generation.is_some())))
            .next()
            .await
            .unwrap();
        expect_that!(cache.has_generation(&service_name).await, is_true());

        // Drop the publisher to simulate unregistration
        drop(publisher);
        // Wait for the generation to be removed from the cache.
        pin!(stream.filter(|maybe_generation| future::ready(maybe_generation.is_none())))
            .next()
            .await
            .unwrap();
        expect_that!(cache.has_generation(&service_name).await, is_false());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn get_or_fetch_current_updates_on_service_change() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let publisher1 = register_test_topic(ctx, &service_name_string).await;

        let cache = MetadataCache::new(empty_vsidl_provider());

        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let mut stream = mutex_guard.subscribe();
        drop(mutex_guard);

        let generation1 =
            pin!(stream.by_ref().filter_map(|maybe_generation| async move { maybe_generation }))
                .next()
                .await
                .unwrap();

        // Drop the publisher to simulate unregistration
        drop(publisher1);
        // Wait for the generation to be removed from the cache.
        pin!(stream.by_ref().filter(|maybe_generation| future::ready(maybe_generation.is_none())))
            .next()
            .await
            .unwrap();

        let _publisher2 = register_test_topic(ctx, &service_name_string).await;

        let generation2 =
            pin!(stream.filter_map(|maybe_generation| async move { maybe_generation }))
                .next()
                .await
                .unwrap();

        expect_that!(generation2.id, not(eq(generation1.id)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn remove_generation_success() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let _publisher = register_test_topic(ctx, &service_name_string).await;

        let cache = MetadataCache::new(empty_vsidl_provider());

        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let stream = mutex_guard.subscribe();
        drop(mutex_guard);

        let generation =
            pin!(stream.filter_map(|maybe_generation| async move { maybe_generation }))
                .next()
                .await
                .unwrap();

        expect_that!(cache.remove_generation(&service_name, generation.id).await, is_true());
        expect_that!(cache.has_generation(&service_name).await, is_false());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn remove_generation_wrong_id() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let _publisher = register_test_topic(ctx, &service_name_string).await;

        let cache = MetadataCache::new(empty_vsidl_provider());
        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let stream = mutex_guard.subscribe();
        drop(mutex_guard);

        // Wait for the generation to be added to the cache.
        pin!(stream.filter(|maybe_generation| future::ready(maybe_generation.is_some())))
            .next()
            .await
            .unwrap();

        use sdv_telemetry_utils::atomic::SequenceNumberType;
        let wrong_id = MetadataGenerationId::from(999999 as SequenceNumberType);

        let was_removed = cache.remove_generation(&service_name, wrong_id).await;
        expect_that!(was_removed, is_false());

        expect_that!(cache.has_generation(&service_name).await, is_true());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn get_or_fetch_current_initially_none_updates_on_registration() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let cache = MetadataCache::new(empty_vsidl_provider());

        // Call cache *before* publisher is registered
        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let mut stream = mutex_guard.subscribe();
        drop(mutex_guard);

        expect_that!(stream.next().await.unwrap().is_none(), is_true());

        let _publisher = register_test_topic(ctx, &service_name_string).await;

        // Wait for the generation to be added to the cache.
        let generation =
            pin!(stream.filter_map(|maybe_generation| async move { maybe_generation }))
                .next()
                .await
                .unwrap();

        expect_that!(
            generation.metadata.message_descriptor.full_name(),
            eq(Int32Value::descriptor().full_name())
        );
        expect_that!(generation.service_type, eq(MetadataServiceType::Dt));

        expect_that!(cache.has_generation(&service_name).await, is_true());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn get_or_fetch_current_concurrent_calls() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let _publisher = register_test_topic(ctx, &service_name_string).await;

        let cache = Arc::new(MetadataCache::new(empty_vsidl_provider()));

        let cache_clone1 = Arc::clone(&cache);
        let service_name_clone1 = service_name.clone();
        let cache_clone2 = Arc::clone(&cache);
        let service_name_clone2 = service_name.clone();

        let t1 = tokio::spawn(async move {
            let mutex_guard =
                cache_clone1.get_or_fetch_current(ctx, service_name_clone1).await.unwrap();
            let stream = mutex_guard.subscribe();
            drop(mutex_guard);
            stream
        });
        let t2 = tokio::spawn(async move {
            let mutex_guard =
                cache_clone2.get_or_fetch_current(ctx, service_name_clone2).await.unwrap();
            let stream = mutex_guard.subscribe();
            drop(mutex_guard);
            stream
        });

        let (res1, res2) = tokio::join!(t1, t2);

        let stream1 = res1.unwrap();
        let stream2 = res2.unwrap();

        let generation1 =
            pin!(stream1.filter_map(|maybe_generation| async move { maybe_generation }))
                .next()
                .await
                .unwrap();
        let generation2 =
            pin!(stream2.filter_map(|maybe_generation| async move { maybe_generation }))
                .next()
                .await
                .unwrap();

        expect_that!(generation1.id, eq(generation2.id));
        expect_that!(generation1.eq_ignoring_id(&generation2), is_true());
        expect_that!(cache.has_generation(&service_name).await, is_true());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn stream_updates_when_publisher_vanishes_and_reappears() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let service_name_string = test_utils::make_unique_service_name();
        let service_name = ServiceName::parse(&service_name_string).unwrap();

        let cache = MetadataCache::new(empty_vsidl_provider());
        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let mut stream = mutex_guard.subscribe();
        drop(mutex_guard);
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), none());

        let publisher = register_test_topic(ctx, &service_name_string).await;
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), some(anything()));

        drop(publisher);
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), none());

        let publisher = register_test_topic(ctx, &service_name_string).await;
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), some(anything()));

        drop(publisher);
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn stream_updates_when_multiple_publishers_appear() {
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context().await;
        let unit_type = test_utils::make_unique_unit_type();
        let service_name = ServiceName::parse(&format!("{unit_type}")).unwrap();

        let service_name_string1 = format!("{unit_type}|unit1");
        let service_name_string2 = format!("{unit_type}|unit2");

        let cache = MetadataCache::new(empty_vsidl_provider());
        let mutex_guard = cache.get_or_fetch_current(ctx, service_name.clone()).await.unwrap();
        let mut stream = mutex_guard.subscribe();
        drop(mutex_guard);
        // 0 publishers -> None
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), none());

        let publisher1 = test_utils::create_publisher_in_new_process(
            service_name_string1,
            test_utils::PublisherType::DataTunnel,
        );
        // 1 publisher -> Some
        expect_that!(
            pin!(stream.by_ref()).next().await.unwrap(),
            some(pat!(MetadataGeneration {
                unit_identifier: pat!(&UnitIdentifier {
                    get_unit_name(): eq("unit1")
                })
            }))
        );

        let publisher2 = test_utils::create_publisher_in_new_process(
            service_name_string2,
            test_utils::PublisherType::DataTunnel,
        );
        // 2 publishers -> None
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), none());

        drop(publisher1);
        // 1 publisher -> Some
        expect_that!(
            pin!(stream.by_ref()).next().await.unwrap(),
            some(pat!(MetadataGeneration {
                unit_identifier: pat!(&UnitIdentifier {
                    get_unit_name(): eq("unit2")
                })
            }))
        );

        drop(publisher2);
        // 0 publishers -> None
        expect_that!(pin!(stream.by_ref()).next().await.unwrap(), none());
    }
}
