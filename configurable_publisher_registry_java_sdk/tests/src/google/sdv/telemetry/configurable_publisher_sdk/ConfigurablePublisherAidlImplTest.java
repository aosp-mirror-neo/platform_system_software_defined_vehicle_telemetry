// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import android.os.IBinder;
import android.os.RemoteException;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import google.sdv.telemetry.configurable_publisher.ISubscriber;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Optional;

/** On-device unit tests for {@link ConfigurablePublisherAidlImpl}. */
@RunWith(AndroidJUnit4.class)
public class ConfigurablePublisherAidlImplTest {

    private static final byte[] TEST_DATA = new byte[] {1, 2, 3};
    private static final byte[] TEST_CONFIG = new byte[] {4, 5, 6};

    private FakeConfigurablePublisher mFakePublisher;
    private ConfigurablePublisherAidlImpl mPublisherAidl;

    /**
     * A fake implementation of the {@link ConfigurablePublisher} interface for controlling test
     * behavior.
     */
    private static class FakeConfigurablePublisher implements ConfigurablePublisher {
        private boolean mShouldThrowException = false;
        private boolean mCreateSubscriptionCalled = false;
        private Optional<byte[]> mLatestMessage = Optional.empty();

        void setShouldThrowException(boolean shouldThrow) {
            this.mShouldThrowException = shouldThrow;
        }

        void setLatestMessage(byte[] message) {
            this.mLatestMessage = Optional.ofNullable(message);
        }

        boolean wasCreateSubscriptionCalled() {
            return mCreateSubscriptionCalled;
        }

        @Override
        public ConfigurablePublisherSubscription createSubscription(
                Subscriber subscriber, Optional<byte[]> configuration)
                throws ConfigurablePublisherException {
            if (mShouldThrowException) {
                throw ConfigurablePublisherException.createSubscriptionFailed();
            }
            mCreateSubscriptionCalled = true;
            return new FakeConfigurablePublisherSubscription();
        }

        @Override
        public Optional<byte[]> getLatestMessage(Optional<byte[]> configuration)
                throws ConfigurablePublisherException {
            if (mShouldThrowException) {
                throw ConfigurablePublisherException.getLatestMessageFailed();
            }
            return mLatestMessage;
        }
    }

    /** A fake implementation of {@link ISubscriber} for testing purposes. */
    private static class FakeSubscriber implements ISubscriber {
        @Override
        public void sendData(byte[] data) throws RemoteException {}

        @Override
        public IBinder asBinder() {
            return null;
        }

        @Override
        public String getInterfaceHash() {
            return ISubscriber.HASH;
        }

        @Override
        public int getInterfaceVersion() {
            return ISubscriber.VERSION;
        }
    }

    /** A fake implementation of {@link ConfigurablePublisherSubscription} for testing. */
    private static class FakeConfigurablePublisherSubscription
            implements ConfigurablePublisherSubscription {
        @Override
        public void unsubscribe() {}
    }

    @Before
    public void setUp() {
        mFakePublisher = new FakeConfigurablePublisher();
        mPublisherAidl = new ConfigurablePublisherAidlImpl(mFakePublisher);
    }

    @Test
    public void createSubscription_success_returnsSubscription() throws RemoteException {
        IConfigurablePublisherSubscription subscription =
                mPublisherAidl.createSubscription(new FakeSubscriber(), TEST_CONFIG);

        assertNotNull(subscription);
        assertTrue(mFakePublisher.wasCreateSubscriptionCalled());
    }

    @Test
    public void createSubscription_publisherThrows_throwsRemoteException() {
        mFakePublisher.setShouldThrowException(true);

        assertThrows(
                RemoteException.class,
                () -> mPublisherAidl.createSubscription(new FakeSubscriber(), TEST_CONFIG));
    }

    @Test
    public void getLatestMessage_successWithValue_returnsData() throws RemoteException {
        mFakePublisher.setLatestMessage(TEST_DATA);

        byte[] result = mPublisherAidl.getLatestMessage(TEST_CONFIG);

        assertArrayEquals(TEST_DATA, result);
    }

    @Test
    public void getLatestMessage_successWithNoValue_returnsNull() throws RemoteException {
        mFakePublisher.setLatestMessage(null); // Corresponds to Optional.empty()

        byte[] result = mPublisherAidl.getLatestMessage(TEST_CONFIG);

        assertNull(result);
    }

    @Test
    public void getLatestMessage_publisherThrows_throwsRemoteException() {
        mFakePublisher.setShouldThrowException(true);

        assertThrows(RemoteException.class, () -> mPublisherAidl.getLatestMessage(TEST_CONFIG));
    }
}
