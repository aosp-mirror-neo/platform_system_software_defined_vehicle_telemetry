// Copyright 2025 Google LLC

//! This crate contains a `PublishingStrategyFactory` implementation for SDV Comms.

mod data_tunnel;
mod rpc;

use anyhow::bail;
use protobuf::descriptor::FileDescriptorProto;
use rand::{rng, Rng};
use sdv_comms::{sd::UnitType, ContextRef};
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_utils::ServiceName;
use sdv_telemetry_simulator_publishing_strategy::{
    PublishingStrategy, PublishingStrategyFactory, PublishingStrategyType,
};
use std::future::Future;

use crate::data_tunnel::PublishingStrategyImpl as DataTunnelPublishingStrategy;
use crate::rpc::PublishingStrategyImpl as RpcPublishingStrategy;

/// Implements `PublishingStrategyFactory` for publishers using SDV Comms Data Tunnel
pub struct PublishingStrategyFactoryImpl<T: Fn() -> F + Send, F: Future<Output = ContextRef> + Send>
{
    ctx_factory: T,
}

#[async_trait::async_trait]
impl<T: Fn() -> F + Send, F: Future<Output = ContextRef> + Send> PublishingStrategyFactory
    for PublishingStrategyFactoryImpl<T, F>
{
    async fn create(
        &mut self,
        publishing_strategy_type: &PublishingStrategyType,
    ) -> Option<anyhow::Result<Box<dyn PublishingStrategy>>> {
        Some(Ok(match publishing_strategy_type {
            PublishingStrategyType::SdvCommsDataTunnel { message_count, message_size } => {
                let ctx = (self.ctx_factory)().await;

                Box::new(DataTunnelPublishingStrategy::new(ctx, *message_count, *message_size))
                    as Box<dyn PublishingStrategy>
            }
            PublishingStrategyType::SdvCommsRpc => {
                let ctx = (self.ctx_factory)().await;

                Box::new(RpcPublishingStrategy::new(ctx)) as Box<dyn PublishingStrategy>
            }
            PublishingStrategyType::ConfigurablePublisherRegistry => return None,
        }))
    }
}

impl<T: Fn() -> F + Send, F: Future<Output = ContextRef> + Send>
    PublishingStrategyFactoryImpl<T, F>
{
    /// Creates a new `PublishingStrategyFactoryImpl` instance.
    pub fn new(ctx_factory: T) -> Self {
        Self { ctx_factory }
    }
}

/// Returns a tuple of `UnitType`, unit name, and metadata
fn prepare_registration_args(
    service_name: &str,
    message_name: &str,
    file_descriptors: Vec<FileDescriptorProto>,
) -> anyhow::Result<(UnitType, String, MessageMetadata)> {
    match ServiceName::parse(service_name)? {
        ServiceName::UnitTypeBased { unit_type, unit_name } => {
            let metadata = MessageMetadata {
                file_descriptors,
                message_full_name: format!(".{}", message_name),
                ..Default::default()
            };

            Ok((unit_type, unit_name.unwrap_or_else(|| random_unit_name(&mut rng())), metadata))
        }
        ServiceName::FqinBased { .. } => {
            bail!("Unsupported service name format. Only Unit Type-based service names are supported by the simulator.");
        }
    }
}

fn random_unit_name(rng: &mut (impl Rng + ?Sized)) -> String {
    format!("random-{:016x}", rng.gen::<u64>())
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{future, StreamExt};
    use googletest::expect_that;
    use googletest::matchers::{
        all, container_eq, contains_substring, displays_as, eq, err, none, ok, pat, predicate,
        some, starts_with,
    };
    use mockall::mock;
    use protobuf::well_known_types::empty::Empty;
    use protobuf::MessageFull;
    use rand::RngCore;
    use sdv_comms::sd::{ServiceDiscoveryManager, ServiceUnitDefinition, UnitRegistrationArgs};
    use sdv_telemetry_test_utils::{downcast_ref, parameterized_test};
    use std::str::FromStr;

    pub(crate) async fn make_context() -> ContextRef {
        use sdv_comms::id::ServiceFqin;
        use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
        use tokio::sync::OnceCell;

        static CONTEXT_REF: OnceCell<ContextRef> = OnceCell::const_new();
        *CONTEXT_REF
            .get_or_init(|| async {
                // Tests may run in multiple parallel processes, therefore we need to ensure that
                // the FQINs created by the processes do not conflict with each other. This is
                // achieved by embedding the process ID into the FQIN.
                let fqin = ServiceFqin::from_str(&format!(
                    "vm:com.google.sdv.telemetry.tests.SdvCommsPublishingStrategyFactory{}/instance",
                    std::process::id(),
                ))
                .unwrap();
                // The FQIN of the created context is actually silently updated with the correct VM
                // name, so we must not rely on `fqin`, but instead query it from the Context via
                // `ctx.get_self_fqin()`.
                wait_for_sdv_agents_and_create_context(fqin).await
            })
            .await
    }

    pub(crate) async fn discover_service(
        ctx: ContextRef,
        unit_type: &UnitType,
        unit_name: &str,
    ) -> ServiceUnitDefinition {
        let sd = ServiceDiscoveryManager::new(ctx);
        let service_units = sd
            .list_units_by_type(unit_type)
            .filter(|service_unit| {
                future::ready(service_unit.get_unit_identifier().get_unit_name() == unit_name)
            })
            .collect::<Vec<_>>()
            .await;

        let [ref service_unit] = service_units[..] else {
            panic!("Expected exactly one service unit, but got: {service_units:?}");
        };
        service_unit.to_owned()
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn factory_create_sdv_comms_data_tunnel_returns_some() {
        let mut factory = PublishingStrategyFactoryImpl::new(make_context);

        let result = factory
            .create(&PublishingStrategyType::SdvCommsDataTunnel {
                message_count: 1,
                message_size: 2,
            })
            .await;

        expect_that!(
            result,
            some(ok(downcast_ref::<dyn PublishingStrategy, DataTunnelPublishingStrategy, _>(
                pat!(&DataTunnelPublishingStrategy { message_count: 1, message_size: 2 })
            )))
        );
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn factory_create_sdv_comms_rpc_returns_some() {
        let mut factory = PublishingStrategyFactoryImpl::new(make_context);

        let result = factory.create(&PublishingStrategyType::SdvCommsRpc).await;

        expect_that!(
            result,
            some(ok(downcast_ref::<dyn PublishingStrategy, RpcPublishingStrategy, _>(pat!(
                &RpcPublishingStrategy { .. }
            ))))
        );
    }

    #[googletest::test]
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn factory_create_configurable_publisher_registry_returns_none() {
        let mut factory = PublishingStrategyFactoryImpl::new(make_context);

        let result = factory.create(&PublishingStrategyType::ConfigurablePublisherRegistry).await;

        expect_that!(result, none());
    }

    parameterized_test! {
        #[googletest::test]
        prepare_registration_args_valid,

        |service_name, expected_unit_type, unit_name_matcher| {
            let file_descriptors = vec![Empty::descriptor().file_descriptor_proto().to_owned()];

            let result = prepare_registration_args(
                service_name,
                "my.package.MyMessage",
                file_descriptors.clone(),
            );

            expect_that!(
                result,
                ok((
                    predicate(|unit_type: &UnitType| *unit_type == expected_unit_type),
                    unit_name_matcher,
                    pat!(&MessageMetadata {
                        file_descriptors: ref container_eq(file_descriptors),
                        message_full_name: ref eq(".my.package.MyMessage"),
                    }),
                ))
            );
        },

        with_unit_name: ("com.google.sdv.telemetry.tests.Blah|unit-name", UnitType::from_str("com.google.sdv.telemetry.tests.Blah").unwrap(), eq("unit-name")),
        without_unit_name: ("com.google.sdv.telemetry.tests.Blah", UnitType::from_str("com.google.sdv.telemetry.tests.Blah").unwrap(), starts_with("random-")),
    }

    parameterized_test! {
        #[googletest::test]
        prepare_registration_args_invalid,

        |service_name, expected_error| {
            let file_descriptors = vec![Empty::descriptor().file_descriptor_proto().to_owned()];

            let result =
                prepare_registration_args(service_name, "my.package.MyMessage", file_descriptors.clone());

            expect_that!(result, err(displays_as(contains_substring(expected_error))));
        },

        fqin: ("vm:com.google.sdv.telemetry.tests.Blah/instance|unit-name", "Only Unit Type-based service names are supported by the simulator"),
        invalid: ("???", "Failed to parse Unit Type"),
    }

    #[test]
    fn random_unit_name_is_random() {
        let unit_name1 = random_unit_name(&mut rng());
        let unit_name2 = random_unit_name(&mut rng());

        assert_ne!(unit_name1, unit_name2);
    }

    mock! {
        Rng {}

        impl RngCore for Rng {
            fn next_u32(&mut self) -> u32;
            fn next_u64(&mut self) -> u64;
            fn fill_bytes(&mut self, dst: &mut [u8]);
        }
    }

    parameterized_test! {
        #[googletest::test]
        random_unit_name_is_valid,

        |rng: &mut dyn RngCore, unit_name_matcher| {
            let unit_name = random_unit_name(rng);

            // We use `UnitRegistrationArgs::set_unit_name` as a means to validate the unit name, as
            // there is no dedicated `UnitName` struct or similar exposed by the Comms Stack SDK.
            let mut args = UnitRegistrationArgs::new();
            args.set_unit_name(&unit_name);

            expect_that!(args.get_unit_name(), all!(
                eq(&unit_name),
                unit_name_matcher,
            ));
        },

        random: (&mut rng(), starts_with("random-")),
        // These tests are here because the unit name format has certain requirements, like max
        // length and allowed characters. As such, this test tries to generate the edge cases.
        max: (&mut {let mut mock = MockRng::new(); mock.expect_next_u64().once().return_const(u64::MAX); mock}, eq("random-ffffffffffffffff")),
        min: (&mut {let mut mock = MockRng::new(); mock.expect_next_u64().once().return_const(u64::MIN); mock}, eq("random-0000000000000000")),
    }
}
