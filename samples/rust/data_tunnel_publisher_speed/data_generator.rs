// Copyright 2024 Google LLC

use log::trace;
use rand::Rng;
use speed_publisher_proto::speed_publisher::SpeedData;

#[derive(Debug)]
enum State {
    Stopped(Stopped),
    Driving,
}

#[derive(Debug)]
struct Stopped {
    ticks: u64,
}

#[derive(Debug)]
pub(crate) struct SpeedDataGenerator {
    speed: i32,
    road_speed_limit: i32,
    state: State,
}

impl SpeedDataGenerator {
    pub(crate) fn new() -> Self {
        Self { speed: 0, road_speed_limit: 5, state: State::Stopped(Stopped { ticks: 0 }) }
    }

    pub(crate) fn generate(&mut self) -> SpeedData {
        const MIN: i32 = -40;
        const MAX: i32 = 250;

        match self.state {
            State::Stopped(ref mut stopped) => {
                if stopped.ticks == 0 {
                    self.state = State::Driving;
                } else {
                    stopped.ticks -= 1;
                }
            }
            State::Driving => {
                let mut rng = rand::thread_rng();
                // Generate the new speed randomly within `MIN` and `MAX` bounds, with a bigger
                // probability to keep the same speed than for it to change.
                self.speed += rng
                    .gen_range::<f32, _>(if self.speed <= MIN {
                        -0.0..=0.7
                    } else if self.speed >= MAX {
                        -0.7..=0.0
                    } else {
                        -0.7..=0.7
                    })
                    .round() as i32;

                if self.speed == 0 {
                    self.state = State::Stopped(Stopped { ticks: rng.random_range(5..30) });
                } else if rng.random_bool(0.005) {
                    self.road_speed_limit = if self.road_speed_limit == 5 { 30 } else { 5 };
                }
            }
        }

        trace!("Speed generator state: {self:?}");
        let mut message = SpeedData::new();
        message.speed = self.speed;
        message.above_speed_limit = self.speed.abs() > self.road_speed_limit;
        message
    }
}
