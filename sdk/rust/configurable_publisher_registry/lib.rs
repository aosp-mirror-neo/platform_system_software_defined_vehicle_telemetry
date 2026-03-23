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

//! This module implements an SDK for Telemetry's Configurable Publisher Registry.

mod publisher;
mod publisher_metadata;
mod publisher_name;
mod registration;
mod registry;

pub mod codec;

pub use publisher::Publisher;
pub use publisher_metadata::PublisherMetadata;
pub use publisher_name::PublisherName;
pub use publisher_name::PublisherNameError;
pub use registration::Registration;
pub use registry::Registry;
