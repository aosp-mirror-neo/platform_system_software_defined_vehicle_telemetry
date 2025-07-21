// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import android.util.Log;
import com.google.protobuf.InvalidProtocolBufferException;
import google.sdv.telemetry.configurable_publisher.ISubscriber;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto.StatsPublisherConfiguration;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.StatsLogReport;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.ConfigMetricsReportList;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.ConfigMetricsReport;
import google.sdv.telemetry.java.statspublisher.StatsdConfigProto.StatsdConfig;
import android.app.StatsManager.StatsUnavailableException;
import android.app.StatsManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Publisher for Statsd data. StatsPublisher is responsible for publishing data from Statsd to the
 * Configurable Publisher Registry. Each StatsPublisher corresponds to a single configuration
 * received from the telemetry service and may cater to multiple atoms.
 */
public class StatsPublisher {
    private static final String TAG = StatsPublisher.class.getSimpleName();
    private long pollingIntervalMs;
    private StatsPublisherConfiguration config;
    private long configId;
    private ISubscriber subscriber;
    private StatsManager mStatsManager;
    private ScheduledExecutorService scheduler;

    /**
     * Private Constructor for StatsPublisher
     *
     * <p>Expected to be called by factory method {@link #createPublisherFromConfig}
     *
     * @param config The configuration for the StatsPublisher
     * @param subscriber The subscriber to publish data to
     */
    private StatsPublisher(
            StatsPublisherConfiguration config, ISubscriber subscriber, StatsManager statsManager) {
        this.config = config;
        this.pollingIntervalMs = config.getPollingIntervalMs();
        this.configId = config.getConfigId();
        this.subscriber = subscriber;
        this.mStatsManager = statsManager;

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
    public static StatsPublisher createPublisherFromConfig(
            byte[] config, ISubscriber subscriber, StatsManager statsManager)
            throws InvalidProtocolBufferException {
        Log.i(TAG, "createPublisherFromConfig. Creating a new StatsPublisher using raw config");
        StatsPublisherConfiguration configProto = StatsPublisherConfiguration.parseFrom(config);
        Log.i(TAG, "Successfully parsed config");

        // TODO: validate config, make sure fields are reasonable: b/432091569

        StatsPublisher publisher = new StatsPublisher(configProto, subscriber, statsManager);

        StatsdConfig statsdConfig =
                getStatsdConfig(StatsdConfig.newBuilder().setId(publisher.configId), configProto);

        Log.i(TAG, "Created a new StatsdConfig: " + statsdConfig.toString());
        publisher.addConfigToStatsd(statsdConfig);

        // Begin publishing data on a set interval
        publisher.start();

        return publisher;
    }

    /**
     * Create a StatsdConfig from a StatsPublisherConfiguration. Performs a 1:1 mapping between the
     * proto definitions. Relies on StatsPublisherAtom
     *
     * @param builder The builder for the StatsdConfig with an already-set config key
     * @param config The StatsPublisherConfiguration object
     * @return The StatsdConfig object.
     */
    private static StatsdConfig getStatsdConfig(
            StatsdConfig.Builder builder,
            StatsPublisherConfigProto.StatsPublisherConfiguration config) {
        // Iterate through each atom configuration and create a statsd config
        // Does not require manually specifying atom matchers, they are automatically
        // created from the atom configurations
        for (StatsPublisherConfigProto.AtomConfig atomConfig : config.getConfigList()) {
            builder.addAtomMatcher(
                    StatsdConfigProto.AtomMatcher.newBuilder()
                            .setId(atomConfig.getAtomConfigured().getNumber())
                            .setSimpleAtomMatcher(
                                    StatsdConfigProto.SimpleAtomMatcher.newBuilder()
                                            .setAtomId(
                                                    atomConfig.getAtomConfigured().getNumber())));
            builder.addEventMetric(
                    StatsdConfigProto.EventMetric.newBuilder()
                            .setId(atomConfig.getAtomConfigured().getNumber())
                            .setWhat(atomConfig.getAtomConfigured().getNumber()));
        }

        Log.i(TAG, "Created statsd config from metrics configuration: " + config);
        return builder.build();
    }

    /** Adds a Statsd configuration to Statsd */
    private void addConfigToStatsd(StatsdConfig statsdConfig) {
        try {
            mStatsManager.addConfig(configId, statsdConfig.toByteArray());
            Log.i(TAG, "Successfully added metrics config to statsManager");
        } catch (StatsUnavailableException e) {
            Log.i(TAG, "Error occurred" + e);
        }
    }

    public long getConfigId() {
        return configId;
    }

    public void pullReports() {
        Log.i(TAG, "Pulling reports for conig " + configId);

        // TODO: complete implementation of accessing statsmanager
        // and pulling reports. b/424625099
        try {
            StatsLogProto.ConfigMetricsReportList reportList =
                    StatsLogProto.ConfigMetricsReportList.parseFrom(
                            mStatsManager.getReports(configId));
            Log.i(TAG, "Successfully pulled reports: " + reportList.toString());
        } catch (Exception e) {
            Log.i(TAG, "Error occurred while pulling reports: " + e);
        }
    }

    /** Starts the periodic polling of statsd reports */
    public void start() {
        Log.i(TAG, "Starting periodic polling every " + pollingIntervalMs + " ms");

        // This thread is already scheduled, should not need to do this again
        // Ensures that this method is safe even if incorrectly called multiple times
        if (scheduler != null && !scheduler.isShutdown()) {
            Log.w(TAG, "Publisher is already running.");
            return;
        }

        Log.i(TAG, "Creating scheduler and starting polling");

        scheduler = Executors.newSingleThreadScheduledExecutor();

        // Get a reference to the periodic method
        final Runnable poller = this::pullReports;

        // Schedule the thread to run at the specified polling interval
        scheduler.scheduleAtFixedRate(poller, 0, pollingIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** Stops the periodic polling. */
    public void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            Log.i(TAG, "Stopping publisher polling.");

            // Shut down the scheduler, essentially an interrupt
            scheduler.shutdownNow();
        } else {
            Log.i(TAG, "Scheduler is null or already shutdown");
        }
    }
}
