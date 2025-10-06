// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import android.os.RemoteException;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry;
import google.sdv.telemetry.configurable_publisher.PublisherInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * On-device unit tests for the {@link ConfigurablePublisherRegistry}. This test class uses the
 * AndroidJUnit4 runner to execute tests on an Android device or emulator.
 */
@RunWith(AndroidJUnit4.class)
public class ConfigurablePublisherRegistryTest {
    private static final String TEST_SERVICE_NAME = "testPublisher";
    private static final String TEST_MESSAGE_TYPE = "test.message";

    private ConfigurablePublisher mFakePublisher;
    private FakeConfigurablePublisherRegistry mFakeRegistryService;
    private PublisherInfoJava mPublisherInfo;

    /** A fake implementation of ConfigurablePublisher for testing */
    private static class FakeConfigurablePublisher implements ConfigurablePublisher {
        @Override
        public ConfigurablePublisherSubscription createSubscription(
                Subscriber subscriber, Optional<byte[]> configuration)
                throws ConfigurablePublisherException {
            return null;
        }

        @Override
        public Optional<byte[]> getLatestMessage(Optional<byte[]> configuration)
                throws ConfigurablePublisherException {
            return Optional.empty();
        }
    }

    /** A fake implementation of the IConfigurablePublisherRegistry for testing. */
    private static class FakeConfigurablePublisherRegistry
            extends IConfigurablePublisherRegistry.Stub {
        private boolean mShouldThrowException = false;
        private boolean mRegisterCalled = false;
        private String mUnregisteredServiceName = null;

        void setShouldThrowException(boolean shouldThrow) {
            this.mShouldThrowException = shouldThrow;
        }

        boolean wasRegisterCalled() {
            return mRegisterCalled;
        }

        String getUnregisteredServiceName() {
            return mUnregisteredServiceName;
        }

        @Override
        public void registerPublisher(PublisherInfo publisherInfo, IConfigurablePublisher publisher)
                throws RemoteException {
            if (mShouldThrowException) {
                throw new RemoteException("Test Exception");
            }
            mRegisterCalled = true;
        }

        @Override
        public void unregisterPublisher(String serviceName) throws RemoteException {
            if (mShouldThrowException) {
                throw new RemoteException("Test Exception");
            }
            this.mUnregisteredServiceName = serviceName;
        }

        @Override
        public String getInterfaceHash() {
            return IConfigurablePublisherRegistry.HASH;
        }

        @Override
        public int getInterfaceVersion() {
            return IConfigurablePublisherRegistry.VERSION;
        }
    }

    @Before
    public void setUp() throws Exception {
        mFakeRegistryService = new FakeConfigurablePublisherRegistry();
        mFakePublisher = new FakeConfigurablePublisher();
        mPublisherInfo =
                new PublisherInfoJava(TEST_SERVICE_NAME, new byte[0], TEST_MESSAGE_TYPE, true);
    }

    @Test
    public void testRegisterPublisher_successfulRegistration() throws Exception {

        ConfigurablePublisherRegistry registry =
                new ConfigurablePublisherRegistry(mFakeRegistryService);
        registry.registerPublisher(mPublisherInfo, mFakePublisher);

        assertTrue(mFakeRegistryService.wasRegisterCalled());

        Field field = ConfigurablePublisherRegistry.class.getDeclaredField("registeredPublishers");
        field.setAccessible(true);
        Set<String> publishers = (Set<String>) field.get(registry);
        assertTrue(publishers.contains(TEST_SERVICE_NAME));
    }

    @Test
    public void testRegisterPublisher_remoteException_throws() throws Exception {

        mFakeRegistryService.setShouldThrowException(true);

        ConfigurablePublisherRegistry registry =
                new ConfigurablePublisherRegistry(mFakeRegistryService);

        assertThrows(
                ConfigurablePublisherRegistryException.class,
                () -> registry.registerPublisher(mPublisherInfo, mFakePublisher));
    }

    @Test
    public void testUnregisterPublisher_successfulUnregistration() throws Exception {
        ConfigurablePublisherRegistry registry =
                new ConfigurablePublisherRegistry(mFakeRegistryService);

        registry.registerPublisher(mPublisherInfo, mFakePublisher);
        registry.unregisterPublisher(TEST_SERVICE_NAME);

        assertEquals(TEST_SERVICE_NAME, mFakeRegistryService.getUnregisteredServiceName());

        Field field = ConfigurablePublisherRegistry.class.getDeclaredField("registeredPublishers");
        field.setAccessible(true);
        Set<String> publishers = (Set<String>) field.get(registry);
        assertFalse(publishers.contains(TEST_SERVICE_NAME));
    }

    @Test
    public void testRegisterPublisher_alreadyRegistered_throwsException() throws Exception {
        ConfigurablePublisherRegistry registry =
                new ConfigurablePublisherRegistry(mFakeRegistryService);

        // First registration should succeed
        registry.registerPublisher(mPublisherInfo, mFakePublisher);

        // Second registration should fail
        ConfigurablePublisherRegistryException exception =
                assertThrows(
                        ConfigurablePublisherRegistryException.class,
                        () -> registry.registerPublisher(mPublisherInfo, mFakePublisher));

        // Verify the exception message
        String expectedMessage =
                "Publisher with service name '" + TEST_SERVICE_NAME + "' is already registered.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testUnregisterPublisher_notRegistered_throwsException() throws Exception {
        ConfigurablePublisherRegistry registry =
                new ConfigurablePublisherRegistry(mFakeRegistryService);

        // Attempt to unregister a publisher that was never registered
        ConfigurablePublisherRegistryException exception =
                assertThrows(
                        ConfigurablePublisherRegistryException.class,
                        () -> registry.unregisterPublisher(TEST_SERVICE_NAME));

        // Verify the exception message
        String expectedMessage =
                "Publisher with service name '" + TEST_SERVICE_NAME + "' was not found.";
        assertEquals(expectedMessage, exception.getMessage());
    }
}
