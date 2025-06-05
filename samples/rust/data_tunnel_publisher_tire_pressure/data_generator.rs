// Copyright 2024 Google LLC

use log::trace;
use rand::Rng;
use tire_pressure_publisher_proto::tire_pressure_publisher::TirePressureData;

#[derive(Debug)]
pub(crate) struct TirePressureDataGenerator {
    front_left: f64,
    front_right: f64,
    rear_left: f64,
    rear_right: f64,
}

impl TirePressureDataGenerator {
    pub(crate) fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            front_left: rng.random_range(1.3..4.5),
            front_right: rng.random_range(1.3..4.5),
            rear_left: rng.random_range(1.3..4.5),
            rear_right: rng.random_range(1.3..4.5),
        }
    }

    pub(crate) fn generate(&mut self) -> TirePressureData {
        Self::next_pressure(&mut self.front_left);
        Self::next_pressure(&mut self.front_right);
        Self::next_pressure(&mut self.rear_left);
        Self::next_pressure(&mut self.rear_right);

        trace!("TirePressure generator state: {self:?}");
        let mut message = TirePressureData::new();
        message.front_left = self.front_left;
        message.front_right = self.front_right;
        message.rear_left = self.rear_left;
        message.rear_right = self.rear_right;
        message
    }

    fn next_pressure(pressure: &mut f64) {
        const MIN: f64 = 0.0;
        const MAX: f64 = 5.0;
        let mut rng = rand::thread_rng();
        *pressure = (*pressure + rng.random_range(-0.1..=0.1)).clamp(MIN, MAX);
    }
}
