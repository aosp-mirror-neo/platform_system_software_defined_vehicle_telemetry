// Copyright 2023 Google LLC

#[cfg(test)]
mod tests {

    extern crate protobuf;

    use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryReportListener::ITelemetryReportListener;
    use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryReportReadyListener::ITelemetryReportReadyListener;
    use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryService::ITelemetryServiceAsyncServer;
    use android_sdv_telemetry::binder::{
        Interface, ParcelFileDescriptor, Result as BinderResult, Strong,
    };
    use log::info;
    use protobuf::Message;
    use sdv_telemetry::SdvTelemetryService;
    use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
    use tokio::runtime::Runtime;

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    pub struct TelemetryReportListener;

    impl Interface for TelemetryReportListener {}

    impl ITelemetryReportListener for TelemetryReportListener {
        #[allow(unused_variables)]
        fn onResult(
            &self,
            metrics_config_name: &str,
            parcel: &ParcelFileDescriptor,
            err: &[u8],
            status: i32,
        ) -> BinderResult<()> {
            info!("onResult");
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit testing purposes
    pub struct TelemetryReportReadyListener;

    impl Interface for TelemetryReportReadyListener {}

    impl ITelemetryReportReadyListener for TelemetryReportReadyListener {
        #[allow(unused_variables)]
        fn onReady(&self, metrics_config_name: &str) -> BinderResult<()> {
            info!("onResult");
            Ok(())
        }
    }

    #[test]
    fn add_metrics_config() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService) as Box<dyn ITelemetryServiceAsyncServer>;

        // block on async block
        rt.block_on(async {
            let mut config = MetricsConfig::new();
            config.uuid = "01bf0754-df95-46bb-bb35-522b6af66600".to_string();
            config.version = 2;
            config.script = "Sample Script".to_string();

            info!("Serializing metric config");
            let serialized_config: &[u8] = &config.write_to_bytes().unwrap();

            let result = service.addMetricsConfig("TestConfig", serialized_config).await;

            assert!(result.is_ok());
        });
    }

    #[test]
    fn remove_metrics_config() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService) as Box<dyn ITelemetryServiceAsyncServer>;

        // block on async block
        rt.block_on(async {
            let result = service.removeMetricsConfig("TestConfig").await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn remove_all_metrics_configs() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService) as Box<dyn ITelemetryServiceAsyncServer>;

        // block on async block
        rt.block_on(async {
            let result = service.removeAllMetricsConfigs().await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn clear_report_ready_listener() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService) as Box<dyn ITelemetryServiceAsyncServer>;

        // block on async block
        rt.block_on(async {
            let result = service.clearReportReadyListener().await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn set_report_ready_listener() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService) as Box<dyn ITelemetryServiceAsyncServer>;

        let listener: Strong<dyn ITelemetryReportReadyListener> =
            Strong::new(Box::new(TelemetryReportReadyListener));

        // block on async block
        rt.block_on(async {
            let result = service.setReportReadyListener(&listener).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn get_finished_report() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService) as Box<dyn ITelemetryServiceAsyncServer>;

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        // block on async block
        rt.block_on(async {
            let result = service.getFinishedReport("TestConfig", &listener).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn get_all_finished_report() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService) as Box<dyn ITelemetryServiceAsyncServer>;

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        // block on async block
        rt.block_on(async {
            let result = service.getAllFinishedReports(&listener).await;
            assert!(result.is_ok());
        });
    }
}
