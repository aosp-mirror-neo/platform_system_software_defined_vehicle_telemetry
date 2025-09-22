// Copyright 2025 Google LLC

use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use protobuf::EnumOrUnknown;
use sdv_comms::id::ServiceFqin;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use sdv_telemetry_sample_middleware_proto::tire::{
    GetTireStatusRequest, GetTireStatusResponse, Location,
};
use sdv_telemetry_utils::observable_value::ObservableValue;
use std::pin::Pin;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

macro_rules! impl_get_pressure_observable {
    ($fn_name:ident, $($ref_mut:tt)?) => {
        fn $fn_name(&$($ref_mut)? self, location: &Location) -> &$($ref_mut)? ObservableValue<u32, 100> {
            match location {
                Location::FRONT_LEFT => & $($ref_mut)? self.pressure_front_left,
                Location::FRONT_RIGHT => & $($ref_mut)? self.pressure_front_right,
                Location::REAR_LEFT => & $($ref_mut)? self.pressure_rear_left,
                Location::REAR_RIGHT => & $($ref_mut)? self.pressure_rear_right,
            }
        }
    };
}

struct TireStatusServerImpl {
    pressure_front_left: ObservableValue<u32, 100>,
    pressure_front_right: ObservableValue<u32, 100>,
    pressure_rear_left: ObservableValue<u32, 100>,
    pressure_rear_right: ObservableValue<u32, 100>,
}

impl TireStatusServerImpl {
    pub(super) fn new() -> Self {
        Self {
            pressure_front_left: ObservableValue::new(),
            pressure_front_right: ObservableValue::new(),
            pressure_rear_left: ObservableValue::new(),
            pressure_rear_right: ObservableValue::new(),
        }
    }

    impl_get_pressure_observable!(get_pressure_observable,);
    impl_get_pressure_observable!(get_pressure_observable_mut, mut);
}

pub(super) struct TireStatusServer {
    implementation: Mutex<TireStatusServerImpl>,
}

impl TireStatusServer {
    pub(super) fn new() -> Self {
        Self { implementation: TireStatusServerImpl::new().into() }
    }

    pub(super) fn set_pressure(&self, location: &Location, pressure: u32) {
        self.implementation.lock().get_pressure_observable_mut(location).set(pressure)
    }

    fn get_pressure(&self, location: &Location) -> Option<u32> {
        self.implementation.lock().get_pressure_observable(location).get().copied()
    }

    fn observe_pressure(&self, location: &Location) -> BroadcastStream<u32> {
        self.implementation.lock().get_pressure_observable(location).observe()
    }

    fn get_location(location: &EnumOrUnknown<Location>) -> SdvResult<Location> {
        location.enum_value().map_err(|err| {
            SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                format!("Invalid {} enum value: {err:?}", std::any::type_name::<Location>()),
            )
        })
    }
}

#[async_trait::async_trait]
impl com_sdv_telemetry_sample_tire_status_rpc::Interface for TireStatusServer {
    async fn CreateSubscription(
        &self,
        _caller_id: ServiceFqin,
        request: &GetTireStatusRequest,
    ) -> SdvResult<Pin<Box<dyn Stream<Item = SdvResult<GetTireStatusResponse>> + Send>>> {
        Ok(self
            .observe_pressure(&Self::get_location(&request.tire)?)
            .map_ok(|pressure| GetTireStatusResponse { pressure, ..Default::default() })
            .map_err(|err| match err {
                BroadcastStreamRecvError::Lagged(skipped_count) => {
                    SdvStatus::with_cause_and_message(
                        SdvStatusCode::DataLoss,
                        err,
                        format!(
                            "Failed to observe changes fast enough (discarded {skipped_count} changes)."
                        ),
                    )
                }
            })
            .boxed())
    }

    async fn GetLatestMessage(
        &self,
        _caller_id: ServiceFqin,
        request: &GetTireStatusRequest,
    ) -> SdvResult<GetTireStatusResponse> {
        self.get_pressure(&Self::get_location(&request.tire)?).map_or_else(
            || Err(SdvStatus::new(SdvStatusCode::NotFound)),
            |pressure| Ok(GetTireStatusResponse { pressure, ..Default::default() }),
        )
    }
}
