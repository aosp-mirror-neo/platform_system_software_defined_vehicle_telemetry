// Copyright 2023 Google LLC. All Rights Reserved.

use super::*;
use crate::expr::mock::MockDescriptorProvider;

pub(crate) struct MockDescriptorProviderFactory {}

impl DescriptorProviderFactory for MockDescriptorProviderFactory {
    fn create(
        &self,
        _fds: &[FileDescriptor],
        _publishers: &[PublisherProto],
        _service_publisher_manager: &Arc<dyn ServicePublisherManager>,
    ) -> Result<Box<dyn DescriptorProvider>, DescriptorProviderFactoryError> {
        Ok(Box::new(MockDescriptorProvider::new()))
    }
}

impl MockDescriptorProviderFactory {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
