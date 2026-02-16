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

//! This module includes all the utility functions used in telemetry service

pub mod async_lazy_map;
pub mod atomic;
pub mod collection_extension;
pub mod conversions;
pub mod countdown_latch;
pub mod descriptor_utils;
pub mod duration_utils;
pub mod file_io_utils;
pub mod logging;
pub mod observable_value;
pub mod operation_result;
pub mod option_extension;
pub mod proto_reader;
pub mod value_holder;

pub use sdv_telemetry_utils_macros::with_super_trait;
