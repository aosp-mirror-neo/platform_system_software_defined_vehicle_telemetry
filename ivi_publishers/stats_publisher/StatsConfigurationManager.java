// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import android.os.RemoteException;
import android.util.Log;

import google.sdv.telemetry.java.statspublisher.StatsPublisherSubscription;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto.StatsPublisherConfiguration;
import google.sdv.telemetry.configurable_publisher.ISubscriber;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.nio.ByteBuffer;
import java.time.Duration;
import android.app.StatsManager;

/**
 * Manager for configurations for StatsPublisher. StatsConfigurationManager is responsible for
 * handling configurations for StatsPublisher
 */
public class StatsConfigurationManager extends IConfigurablePublisher.Stub {
    private static final String TAG = "StatsPublisher.StatsConfigurationManager";

    // Subscribers must be stored by this manager so that they can be managed together during
    // shutdown. This also allows the manager to keep a mapping of configuration to
    private List<ISubscriber> subscribers;
    private StatsManager mStatsManager;

    public StatsConfigurationManager(StatsManager statsManager) {
        super();
        this.subscribers = Collections.synchronizedList(new ArrayList<>());
        this.mStatsManager = statsManager;
    }

    @Override
    public String getInterfaceHash() {
        return "google.sdv.telemetry.java.configurable_publisher";
    }

    @Override
    public int getInterfaceVersion() {
        return 1;
    }

    @Override
    public byte[] getLatestMessage(byte[] config) {
        // Unimplemented. StatsPublisher publishes messages directly to subscribers and does not
        // support the getLatestMessage function. Enabling getLatestMessage will require additional
        // information about the subscriber and configuration during runtime.
        return null;
    }

    /**
     * Creates a new publisher thread using the subscription and configuration that reads from
     * StatsManager and publishes based on the configuration
     */
    @Override
    public IConfigurablePublisherSubscription createSubscription(
            ISubscriber subscriber, byte[] configuration) {
        StatsPublisherSubscription subInfo =
                new StatsPublisherSubscription(subscriber, this.subscribers);
        this.subscribers.add(subscriber);

        // TODO: create new thread for each subscriber: b/432049838
        try {
            StatsPublisher.createPublisherFromConfig(
                            configuration, subscriber, mStatsManager);
        } catch (Exception e) {
            Log.i(TAG, "Error occurred when creating publisher from config: " + e);
        }

        return subInfo;
    }
}
