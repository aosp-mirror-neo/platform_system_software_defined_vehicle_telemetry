// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import android.util.Log;
import com.google.protobuf.InvalidProtocolBufferException;
import google.sdv.telemetry.configurable_publisher.ISubscriber;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto.StatsPublisherConfiguration;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.StatsLogReport;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.ConfigMetricsReportList;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.ConfigMetricsReport;

/**
 * Publisher for Statsd data. StatsPublisher is responsible for publishing data from Statsd to the
 * Configurable Publisher Registry.
 */
public class StatsPublisher {
    private static final String TAG = StatsPublisher.class.getSimpleName();
    private long pollingIntervalMs;
    private StatsPublisherConfiguration config;
    private Thread publisherThread;
    private long configId;
    private ISubscriber subscriber;

    /**
     * Private Constructor for StatsPublisher
     *
     * <p>Expected to be called by factory method {@link #createPublisherFromConfig}
     *
     * @param config The configuration for the StatsPublisher
     * @param subscriber The subscriber to publish data to
     */
    private StatsPublisher(StatsPublisherConfiguration config, ISubscriber subscriber) {
        this.config = config;
        this.pollingIntervalMs = config.getPollingIntervalMs();
        this.configId = config.getConfigId();
        this.subscriber = subscriber;

        Log.i(TAG, this.config.toString());
        Log.i(TAG, this.pollingIntervalMs + " ms");
        Log.i(TAG, this.configId + " config_id");
        Log.i(TAG, this.subscriber.toString());
    }

    /**
     * Factory method to create a new StatsPublisher from a configuration and subscriber
     *
     * @param config The serialized protobuf configuration for the StatsPublisher
     * @param subscriber The subscriber to publish data to
     * @return A new StatsPublisher object
     */
    public static StatsPublisher createPublisherFromConfig(byte[] config, ISubscriber subscriber)
            throws InvalidProtocolBufferException {
        Log.i(TAG, "createPublisherFromConfig. Creating a new StatsPublisher using raw config");
        StatsPublisherConfiguration configProto = StatsPublisherConfiguration.parseFrom(config);
        Log.i(TAG, "Successfully parsed config");

        // TODO: validate config, make sure fields are reasonable: b/432091569

        return new StatsPublisher(configProto, subscriber);
    }

    public ConfigMetricsReportList pullConfigMetricsReport() {
        // TODO: pull config metrics report from Statsd: b/432091451
        return null;
    }

    public long getConfigId() {
        return configId;
    }

    public Thread start() {
        Log.i(TAG, "start. Starting a new StatsPublisher thread");
        return publisherThread;
    }

    public void stop() {
        publisherThread.interrupt();
    }
}
