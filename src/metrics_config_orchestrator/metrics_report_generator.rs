// Copyright 2023 Google LLC. All Rights Reserved.

use std::sync::Arc;

use crate::metrics_config_orchestrator::{Trigger, TriggerImpl};

#[allow(dead_code)]
pub(crate) struct MetricsReportGenerator<TTrigger: Trigger = TriggerImpl> {
    pub(super) report_incomplete: bool,
    pub(super) triggers: Vec<Arc<TTrigger>>,
}
