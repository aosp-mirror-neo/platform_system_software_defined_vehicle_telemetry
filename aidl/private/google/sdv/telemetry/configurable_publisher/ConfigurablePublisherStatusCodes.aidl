// Copyright 2025 Google LLC

package google.sdv.telemetry.configurable_publisher;

/**
 * Exhaustive list of all status codes supported by the Configurable Publisher Service.
 */
@Backing(type="int")
@VintfStability
enum ConfigurablePublisherStatusCodes {

    /**
     * Status code for successful API calls
     */
    CONFIGURABLE_PUBLISHER_REGISTRY_SUCCESS = 200,

    /**
     * Common error messages during API calls
     */

    /**
     * Unable to deserialize the data sent to ISubscriber service
     */
    SEND_DATA_DESERIALIZATION_FAILED = 400,

    /**
     * Binder error when trying to register IConfigurablePublisher with
     * the IConfigurablePublisherRegistry service
     */
    REGISTER_PUBLISHER_FAILED_BINDER_ERROR = 401,

    /**
     * Unable to parse publisher file descriptor proto when trying to register
     * IConfigurablePublisher with the IConfigurablePublisherRegistry service
     */
    REGISTER_PUBLISHER_FAILED_FILE_DESCRIPTOR_PROTO_PARSE_ERROR = 402,

    /*
     * Message type is not defined in the file descriptor passed when trying to register
     * IConfigurablePublisher with the IConfigurablePublisherRegistry service
     */
    REGISTER_PUBLISHER_FAILED_INVALID_MESSAGE_TYPE_NAME = 403,

    /*
     * Service with the same name is already registered
     */
    REGISTER_PUBLISHER_FAILED_SERVICE_ALREADY_REGISTERED = 404,

    /**
     * Arbitrary Error used as a placeholder for errors not yet defined
     * for ISubscriber
     */
    SUBSCRIBER_UNKNOWN_ERROR = 498,

    /**
     * Arbitrary Error used as a placeholder for errors not yet defined
     * for IConfigurablePublisherRegistry
     */
    CONFIGURABLE_PUBLISHER_REGISTRY_UNKNOWN_ERROR = 499,
}
