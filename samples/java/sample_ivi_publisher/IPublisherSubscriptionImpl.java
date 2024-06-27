// Copyright 2024 Google LLC

package google.sdv.telemetry.java.JavaSampleIviPublisher;

import java.util.List;
import google.sdv.telemetry.configurable_publisher.*;

public class IPublisherSubscriptionImpl extends IConfigurablePublisherSubscription.Stub {

    private List<ISubscriber> subscriptions;
    private ISubscriber sub;

    public IPublisherSubscriptionImpl(ISubscriber sub, List<ISubscriber> subscriptions) {
        super();
        this.subscriptions = subscriptions;
        this.sub = sub;
    }

    @Override
    public String getInterfaceHash() {
        return "google.sdv.telemetry.java.configurable_publisher_subscription";
    }

    @Override
    public int getInterfaceVersion() {
        return 1;
    }

    @Override
    public void unsubscribe() {
        subscriptions.remove(sub);
    }

}