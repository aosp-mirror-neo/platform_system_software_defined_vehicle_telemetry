// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry;
import google.sdv.telemetry.configurable_publisher.PublisherInfo;
/**
 * Unit tests for the {@link PublisherInfoJava} constructor.
 */
@RunWith(AndroidJUnit4.class)
public class PublisherInfoJavaTest {
    private static final String TEST_SERVICE_NAME = "TestService";
    private static final String TEST_MESSAGE_TYPE = ".test.MyMessage";
    private static final byte[] SET_BYTES = new byte[] {1, 2, 3};
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] NULL_BYTES = null;

    @Test
    public void constructor_withSet_succeeds() {
        // Test the "modern" path: proto is null, set is set.
        PublisherInfoJava info = null;
        try {
            info = new PublisherInfoJava(TEST_SERVICE_NAME, SET_BYTES, TEST_MESSAGE_TYPE, false);
        } catch (Throwable t) {
            fail("Should not have thrown an exception: " + t.getMessage());
        }

        assertNotNull(info);
        assertEquals(TEST_SERVICE_NAME, info.getServiceName());
    }

    @Test
    public void constructor_withSetEmpty_succeeds() {
        PublisherInfoJava info = null;
        try {
            info = new PublisherInfoJava(TEST_SERVICE_NAME, EMPTY_BYTES, TEST_MESSAGE_TYPE, true);
        } catch (Throwable t) {
            fail("Should not have thrown an exception: " + t.getMessage());
        }

        assertNotNull(info);
        assertEquals(TEST_SERVICE_NAME, info.getServiceName());
    }

    @Test
    public void constructor_withSetNull_succeeds() {
        PublisherInfoJava info = null;
        try {
            info = new PublisherInfoJava(TEST_SERVICE_NAME, NULL_BYTES, TEST_MESSAGE_TYPE, true);
        } catch (Throwable t) {
            fail("Should not have thrown an exception: " + t.getMessage());
        }

        assertNotNull(info);
        assertEquals(TEST_SERVICE_NAME, info.getServiceName());
    }
}
