// Copyright 2025 Google LLC

package google.sdv.telemetry.java.JavaSdkSampleIviPublisher;

import android.os.RemoteException;
import android.util.Log;
import com.google.sdv.telemetry.samples.ErrorReportPublisher;
import google.sdv.telemetry.configurable_publisher_sdk.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class PublisherImpl implements ConfigurablePublisher {
  private static final String SERVICE_NAME = "JavaSdkSampleIviPublisher";

  private byte[] latestMessage;
  private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
  private Runnable randomPublisherThread;

  public PublisherImpl() {
    super();
  }

  @Override
  public Optional<byte[]> getLatestMessage(Optional<byte[]> configuration)
      throws ConfigurablePublisherException {
    return Optional.ofNullable(latestMessage);
  }

  @Override
  public ConfigurablePublisherSubscription createSubscription(
      Subscriber subscriber, Optional<byte[]> configuration) throws ConfigurablePublisherException {
    this.subscribers.add(subscriber);
    ConfigurablePublisherSubscriptionImpl subInfo =
        new ConfigurablePublisherSubscriptionImpl(subscriber, this.subscribers);
    return subInfo;
  }

  public Thread publishData() {
    randomPublisherThread = new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          Log.i(SERVICE_NAME, "Publishing data to " + subscribers.size() + " subscribers");

          ErrorReportPublisher.ErrorReportData.Builder err =
              ErrorReportPublisher.ErrorReportData.newBuilder();
          err.setTimestamp(System.currentTimeMillis());
          latestMessage = err.build().toByteArray();

          for (Subscriber sub : subscribers) {
            try {
              sub.sendData(latestMessage);
            } catch (SubscriberException e) {
              System.err.println(e);
            }
          }

          try {
            Thread.sleep(10000);
          } catch (Exception e) {
            System.err.println(e);
          }
        }
      }
    };
    Thread thread = new Thread(randomPublisherThread);
    thread.start();
    return thread;
  }
}