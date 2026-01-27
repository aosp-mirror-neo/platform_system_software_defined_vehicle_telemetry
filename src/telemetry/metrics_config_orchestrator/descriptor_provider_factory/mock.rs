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

use super::*;

use sdv_telemetry_expr::mock::MockDescriptorProvider;

pub(crate) struct MockDescriptorProviderFactory {}

#[async_trait]
impl DescriptorProviderFactory for MockDescriptorProviderFactory {
    async fn create(
        &self,
        _fds: &FileDescriptorsAndWkts,
        _publishers: &[SourceProto],
        _service_publisher_manager: &Arc<dyn ServicePublisherAccessor>,
    ) -> Result<Box<dyn DescriptorProvider>, DescriptorProviderFactoryError> {
        Ok(Box::new(MockDescriptorProvider::new()))
    }
}

impl MockDescriptorProviderFactory {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
