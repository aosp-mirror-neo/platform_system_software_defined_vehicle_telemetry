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

use protobuf::reflect::MessageDescriptor;

mod fetch;

pub(crate) use fetch::fetch_metadata;

/// The metadata retrieved for a service, containing the necessary descriptors.
#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    pub(crate) config_descriptor: Option<MessageDescriptor>,
    pub(crate) message_descriptor: MessageDescriptor,
}
