// Copyright 2025 Google LLC

package google.sdv.telemetry.java.statspublisher;

import java.util.List;
import google.sdv.telemetry.configurable_publisher.ISubscriber;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;

public class StatsPublisherSubscription extends IConfigurablePublisherSubscription.Stub {

    private List<ISubscriber> subscriptions;
    private ISubscriber subscriber;

    public StatsPublisherSubscription(ISubscriber subscriber, List<ISubscriber> subscriptions) {
        super();

        // Shallow copy, points to the list from the manager thread
        this.subscriptions = subscriptions;
        this.subscriber = subscriber;
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
        subscriptions.remove(subscriber);
    }
}
