// Copyright 2024 Google LLC

package google.sdv.telemetry.java.JavaSampleIviPublisher;

import android.os.RemoteException;
import google.sdv.telemetry.configurable_publisher.*;
import android.util.Log;
import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.time.Duration;
import com.google.sdv.telemetry.samples.ErrorReportPublisher;

public class IPublisherImpl extends IConfigurablePublisher.Stub {
    private static final String SERVICE_NAME = "JavaSampleIviPublisher";

    private byte[] latestMessage;
    private List<ISubscriber> subscriber = new ArrayList();
    private Runnable randomPublisherThread;

    public IPublisherImpl() {
        super();
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
        return latestMessage;
    }

    @Override
    public IConfigurablePublisherSubscription createSubscription(
            ISubscriber subscriber,
            byte[] configuration) {
        this.subscriber.add(subscriber);
        IPublisherSubscriptionImpl subInfo = new IPublisherSubscriptionImpl(subscriber,
                this.subscriber);
        return subInfo;
    }

    public Thread publishData() {
        randomPublisherThread = new Runnable() {
            @Override
            public void run() {

                while (!Thread.interrupted()) {
                    Log.i(SERVICE_NAME, "Publishing data to " + subscriber.size()
                            + " subscribers");

                    ErrorReportPublisher.ErrorReportData.Builder err =
                        ErrorReportPublisher.ErrorReportData.newBuilder();
                    err.setTimestamp(System.currentTimeMillis());
                    latestMessage = err.build().toByteArray();

                    for (ISubscriber sub : subscriber) {
                        try {
                            sub.sendData(latestMessage);
                        } catch (Exception e) {
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