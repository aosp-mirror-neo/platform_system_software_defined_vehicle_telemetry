// Copyright 2025 Google LLC

package google.sdv.telemetry.java.JavaSdkSampleIviPublisher;

import google.sdv.telemetry.configurable_publisher_sdk.*;
import java.util.List;

public class ConfigurablePublisherSubscriptionImpl implements ConfigurablePublisherSubscription {
  private List<Subscriber> subscriptions;
  private Subscriber sub;

  public ConfigurablePublisherSubscriptionImpl(Subscriber sub, List<Subscriber> subscriptions) {
    super();
    this.subscriptions = subscriptions;
    this.sub = sub;
  }

  @Override
  public void unsubscribe() {
    subscriptions.remove(sub);
  }
}