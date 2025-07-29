// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import android.util.Log;
import com.google.protobuf.InvalidProtocolBufferException;
import google.sdv.telemetry.configurable_publisher.ISubscriber;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto.StatsPublisherConfiguration;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto.AtomConfig;
import google.sdv.telemetry.java.statspublisher.StatsPublisherReportProto.AtomReportList;
import google.sdv.telemetry.java.statspublisher.StatsPublisherReportProto.AtomReport;
import google.sdv.telemetry.java.statspublisher.StatsPublisherReportProto.AtomReportWrapper;
import google.sdv.telemetry.java.statspublisher.AtomsProto.Atom;
import google.sdv.telemetry.java.statspublisher.AnyProto.Any;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.StatsLogReport;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.ConfigMetricsReportList;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.ConfigMetricsReport;
import google.sdv.telemetry.java.statspublisher.StatsdConfigProto.StatsdConfig;
import android.app.StatsManager.StatsUnavailableException;
import android.app.StatsManager;
import google.sdv.telemetry.java.statspublisher.AtomsProto.AppStartOccurred;
import google.sdv.telemetry.java.statspublisher.StatsLogProto.EventMetricData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    private StatsPublisherSubscription subscription;
    private ConcurrentMap<Long, StatsPublisher> configIdToPublisherMap;
    private ScheduledExecutorService scheduler;

    private static final String TYPE_URL_PREFIX = "type.googleapis.com/";
    private static final String STATS_PUBLISHER_CONFIG_TYPE_URL =
           TYPE_URL_PREFIX + "google.sdv.telemetry.java.statspublisher.StatsPublisherConfiguration";

    // Lookup table for package name filtering
    // If empty for a configured atom, all packages are allowed
    private HashMap<Integer, HashSet<String>> atomPackageFilterSetMap = new HashMap<>();

    /**
     * Private Constructor for StatsPublisher
     *
     * <p>Expected to be called by factory method {@link #createPublisherFromConfig}
     *
     * @param config The configuration for the StatsPublisher
     * @param subscriber The subscriber to publish data to
     */
    private StatsPublisher(
            StatsPublisherConfiguration config,
            ISubscriber subscriber,
            StatsManager statsManager,
            ConcurrentMap<Long, StatsPublisher> configIdToPublisherMap) {
        this.config = config;
        this.pollingIntervalMs = config.getPollingIntervalMs();
        this.configId = config.getConfigId();
        this.subscriber = subscriber;
        this.mStatsManager = statsManager;
        this.configIdToPublisherMap = configIdToPublisherMap;
        this.subscription = new StatsPublisherSubscription(this);

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
            byte[] config,
            ISubscriber subscriber,
            StatsManager statsManager,
            ConcurrentMap<Long, StatsPublisher> configIdToPublisherMap)
            throws InvalidProtocolBufferException {
        Log.i(TAG, "createPublisherFromConfig. Creating a new StatsPublisher using raw config");

        // Parse the Any proto and get the StatsPublisherConfiguration proto
        Any anyParsed = Any.parseFrom(config);

        if (!anyParsed.getTypeUrl().equals(STATS_PUBLISHER_CONFIG_TYPE_URL)) {
            throw new IllegalArgumentException(
                    "Invalid type url: "
                            + anyParsed.getTypeUrl()
                            + " but expected: "
                            + STATS_PUBLISHER_CONFIG_TYPE_URL);
        }

        StatsPublisherConfiguration configProto =
                StatsPublisherConfiguration.parseFrom(anyParsed.getValue());

        Log.i(TAG, "Successfully parsed config");

        // TODO: validate config, make sure fields are reasonable: b/432091569

        StatsPublisher publisher =
                new StatsPublisher(configProto, subscriber, statsManager, configIdToPublisherMap);

        StatsdConfig statsdConfig =
                getStatsdConfig(StatsdConfig.newBuilder().setId(publisher.configId), configProto);

        Log.i(TAG, "Created a new StatsdConfig: " + statsdConfig.toString());
        publisher.addConfigToStatsd(statsdConfig);

        // Add package filters to the config
        for (AtomConfig atomConfig : configProto.getConfigList()) {
            if (atomConfig.getPackageNameFilterCount() > 0) {
                HashSet<String> packageSet = new HashSet<>();

                for (String packageName : atomConfig.getPackageNameFilterList()) {
                    packageSet.add(packageName);
                }

                publisher.atomPackageFilterSetMap.put(
                        atomConfig.getAtomConfigured().getNumber(), packageSet);
            }
        }

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

        try {
            StatsLogProto.ConfigMetricsReportList report =
                    StatsLogProto.ConfigMetricsReportList.parseFrom(
                            mStatsManager.getReports(configId));

            Log.i(TAG, "Pulled " + report.getReportsCount() + " reports");

            // Convert the ConfigMetricsReportList to an AtomReportList and filter the atoms based
            // on package name
            AtomReportList atomReportList = convertToAtomReportList(report);

            if(atomReportList.getReportCount() == 0) {
                Log.i(TAG, "No reports found for config " + configId);
                return;
            }

            AtomReportWrapper atomReportWrapper =
                    AtomReportWrapper.newBuilder().setData(atomReportList.toByteString()).build();

            subscriber.sendData(atomReportWrapper.toByteArray());
        } catch (Exception e) {
            Log.i(TAG, "Error occurred while pulling reports: " + e);
        }
    }

    /**
     * Converts a ConfigMetricsReportList to an AtomReportList Also performs the filtering of atoms
     * based on package name
     *
     * @param reports The ConfigMetricsReportList to convert
     * @return The AtomReportList
     */
    private AtomReportList convertToAtomReportList(ConfigMetricsReportList reports) {
        AtomReportList.Builder atomReportList = AtomReportList.newBuilder();

        // Get all atoms and aggregate them into a single report that can be given to the telemetry
        // service. This nesting is necessary because the data is stored in a nested structure in
        // Statsd. However, the metrics list is limited by the amount of atoms that are configured
        // and only the number of atoms is variable. Most of the time, there will only be one
        // atom or entry.
        for (ConfigMetricsReport report : reports.getReportsList()) {
            for (StatsLogReport metric : report.getMetricsList()) {
                for (StatsLogProto.EventMetricData data : metric.getEventMetrics().getDataList()) {
                    Atom currentAtom = null;
                    // Statsd can return either an Atom or an AggregatedAtomInfo
                    // AggregatedAtomInfo contains an Atom
                    // StatsPublisher should handle both cases for future atom types
                    if (data.hasAtom()) {
                        currentAtom = data.getAtom();
                    } else if (data.hasAggregatedAtomInfo()) {
                        currentAtom = data.getAggregatedAtomInfo().getAtom();
                    } else {
                        // No atom found in the data, we can skip this iteration
                        continue;
                    }

                    // Check if the atom is in the filter set, if not, skip it
                    // TODO: investigate if Atom can be sent without serialization b/435444175
                    if (isAtomInFilterSet(currentAtom)) {
                        atomReportList.addReport(
                                AtomReport.newBuilder()
                                        .setData(currentAtom.toByteString())
                                        .setElapsedTimestampNanos(data.getElapsedTimestampNanos())
                                        .build());
                    }
                }
            }
        }

        return atomReportList.build();
    }

    /**
     * Helper method for finding if an atom is in the filter set.
     *
     * @param atom The atom to check
     * @return True if the atom is in the filter set, false otherwise
     */
    private boolean isAtomInFilterSet(Atom atom) {
        if (atomPackageFilterSetMap.containsKey(atom.getPushedCase().getNumber())) {
            // If there is a filter defined for this atom, check if the package is in
            // the set
            HashSet<String> packageSet =
                    atomPackageFilterSetMap.get(atom.getPushedCase().getNumber());

            String packageName =
                    switch (atom.getPushedCase()) {
                        case ACTIVITY_FOREGROUND_STATE_CHANGED ->
                                atom.getActivityForegroundStateChanged().getPkgName();

                        case APP_START_OCCURRED -> atom.getAppStartOccurred().getPkgName();
                        case APP_CRASH_OCCURRED -> atom.getAppCrashOccurred().getPackageName();
                        // MediaPlaybackStateChanged does not have a filter
                        default -> null;
                    };

            // Package name should be null for atoms that cannot be filtered by name
            if (packageName == null || packageSet.isEmpty() || packageSet.contains(packageName)) {
                Log.i(TAG, packageName + " in filter set for atom");
                return true;
            } else {
                Log.i(TAG, packageName + " filtered out " + atom.getPushedCase().getNumber());
                return false;
            }
        } else {
            // No filter defined for this atom, add everything to the filtered report
            return true;
        }
    }

    public StatsPublisherSubscription getSubscription() {
        return subscription;
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

        // Track this publisher and add a reference to it on the map
        configIdToPublisherMap.put(configId, this);
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

        // Remove this publisher from the map of publishers
        configIdToPublisherMap.remove(configId);
    }
}
