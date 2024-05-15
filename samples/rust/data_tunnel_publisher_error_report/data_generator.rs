// Copyright 2024 Google LLC. All Rights Reserved.

use error_report_publisher_proto::error_report_publisher::{ErrorInfo, ErrorReportData, Severity};
use protobuf::Enum;
use rand::distributions::{Alphanumeric, DistString};
use rand::seq::IteratorRandom;
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub(crate) struct ErrorReportDataGenerator {}

impl ErrorReportDataGenerator {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn generate(&mut self) -> ErrorReportData {
        let mut rng = rand::thread_rng();

        let mut error_info = ErrorInfo::new();
        error_info.code = rng.gen_range(-1000..=1000);
        error_info.severity = (*Severity::VALUES.iter().choose(&mut rng).unwrap()).into();

        let title_len = rng.gen_range(0..=50);
        error_info.title = Alphanumeric.sample_string(&mut rng, title_len);

        let description_len = rng.gen_range(0..=1_000_000);
        error_info.description = Alphanumeric.sample_string(&mut rng, description_len);

        let mut memory_dump = vec![0u8; rng.gen_range(0..=2_000_000)];
        rng.fill(&mut memory_dump[..]);
        error_info.memory_dump = memory_dump;

        ErrorReportData {
            timestamp: u64::try_from(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis(),
            )
            .unwrap(),
            error_info: Some(error_info).into(),
            ..Default::default()
        }
    }
}
