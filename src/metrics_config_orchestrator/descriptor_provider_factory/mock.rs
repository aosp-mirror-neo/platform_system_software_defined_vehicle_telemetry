// Copyright 2023 Google LLC

use super::*;

use crate::expr::mock::MockDescriptorProvider;

pub(crate) struct MockDescriptorProviderFactory {}

#[async_trait]
impl DescriptorProviderFactory for MockDescriptorProviderFactory {
    async fn create(
        &self,
        _fds: &[FileDescriptor],
        _publishers: &[PublisherProto],
        _service_publisher_manager: &Arc<dyn ServicePublisherAccessor>,
        _error_callback: Arc<dyn Fn(String, ServiceDiscoveryError) + Send + Sync>,
    ) -> Result<Box<dyn DescriptorProvider>, DescriptorProviderFactoryError> {
        Ok(Box::new(MockDescriptorProvider::new()))
    }
}

impl MockDescriptorProviderFactory {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
