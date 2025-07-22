// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import java.util.List;
import google.sdv.telemetry.configurable_publisher.ISubscriber;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StatsPublisherSubscription extends IConfigurablePublisherSubscription.Stub {

    private StatsPublisher publisher;

    public StatsPublisherSubscription(StatsPublisher publisher) {
        super();

        this.publisher = publisher;
    }

    @Override
    public int getInterfaceVersion() {
        return 1;
    }

    @Override
    public String getInterfaceHash() {
        return "google.sdv.telemetry.java.configurable_publisher_subscription";
    }

    @Override
    public void unsubscribe() {
        // Stops the publisher thread and removes the publisher from the map
        publisher.stop();
    }
}
