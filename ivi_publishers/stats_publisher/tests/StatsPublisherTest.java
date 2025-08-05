// Copyright 2025 Google LLC
package google.sdv.telemetry.java.statspublisher.tests;

import androidx.test.filters.LargeTest;
import androidx.test.filters.MediumTest;
import androidx.test.filters.SmallTest;
import android.util.Log;

import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.assertThrows;
import androidx.test.core.app.ApplicationProvider;
import android.content.Context;
import android.app.StatsManager;
import google.sdv.telemetry.java.statspublisher.StatsPublisher;
import google.sdv.telemetry.configurable_publisher.ISubscriber;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto.StatsPublisherConfiguration;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto.StatsPublisherAtom;
import google.sdv.telemetry.java.statspublisher.StatsPublisherConfigProto;
import google.sdv.telemetry.java.statspublisher.AnyProto.Any;

@RunWith(JUnit4.class)
public class StatsPublisherTest {
    private static final String TAG = StatsPublisherTest.class.getSimpleName();

    // To grab system services
    private Context context;
    private StatsManager statsManager;
    private ISubscriber mockSubscriber;
    private ConcurrentHashMap<Long, StatsPublisher> map;
    private static final String TYPE_URL =
        "type.googleapis.com/google.sdv.telemetry.java.statspublisher.StatsPublisherConfiguration";

    @BeforeClass
    public static void beforeClass() {
        Log.d(TAG, "beforeClass()");
    }

    @AfterClass
    public static void afterClass() {
        Log.d(TAG, "afterClass()");
    }

    @Before
    public void before() {
        Log.d(TAG, "before()");

        // Get Access to the System Services
        context = ApplicationProvider.getApplicationContext();
        statsManager = context.getSystemService(StatsManager.class);

        // Instantiate method parameters
        map = new ConcurrentHashMap<>();
        mockSubscriber =
                new ISubscriber.Stub() {
                    @Override
                    public String getInterfaceHash() {
                        return "google.sdv.telemetry.java.configurable_publisher.ISubscriber";
                    }

                    @Override
                    public int getInterfaceVersion() {
                        return 1;
                    }

                    @Override
                    public void sendData(byte[] message) {
                        Log.i(TAG, "Got message from service");
                    }
                };
    }

    @After
    public void after() {
        Log.d(TAG, "after()");
    }

    @Test
    public void createPublisherFromConfig_invalidConfig() {
        byte[] byteConfig = {};

        // invalid config so the creation should fail and an error should be thrown
        assertThrows(
                Exception.class,
                () ->
                        StatsPublisher.createPublisherFromConfig(
                                byteConfig, mockSubscriber, statsManager, map));
    }

    @Test
    public void createPublisherFromConfig_validConfig() {
        StatsPublisherConfiguration config =
                StatsPublisherConfiguration.newBuilder()
                        .setPollingIntervalMs(1000)
                        .setConfigId(System.currentTimeMillis())
                        .addConfig(
                                StatsPublisherConfigProto.AtomConfig.newBuilder()
                                        .setAtomConfigured(StatsPublisherAtom.APP_START_OCCURRED)
                                        .build())
                        .build();

        try {
            StatsPublisher publisher =
                    StatsPublisher.createPublisherFromConfig(
                            Any.newBuilder()
                                    .setValue(config.toByteString())
                                    .setTypeUrl(TYPE_URL)
                                    .build()
                                    .toByteArray(),
                            mockSubscriber,
                            statsManager,
                            map);

            if(publisher == null) {
                Assert.fail("Publisher should not be null");
            } else {
                Log.i(TAG, "Publisher created successfully, test pass.");
            }
        } catch (Exception e) {
            Log.i(TAG, "Error occurred when creating publisher from config: " + e);
            Assert.fail("Exception should not be thrown" + e);
        }
    }
}
