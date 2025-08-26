// Copyright 2025 Google LLC
package google.sdv.telemetry.configurable_publisher_sdk;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import android.os.RemoteException;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/** On-device unit tests for {@link ConfigurablePublisherSubscriptionAidlImpl}. */
@RunWith(AndroidJUnit4.class)
public class ConfigurablePublisherSubscriptionAidlImplTest {

    private FakeConfigurablePublisherSubscription mFakeSubscription;
    private ConfigurablePublisherSubscriptionAidlImpl mSubscriptionAidl;

    /**
     * A fake implementation of the {@link ConfigurablePublisherSubscription} interface for
     * controlling test behavior.
     */
    private static class FakeConfigurablePublisherSubscription
            implements ConfigurablePublisherSubscription {
        private boolean mShouldThrowException = false;
        private boolean mUnsubscribeCalled = false;

        void setShouldThrowException(boolean shouldThrow) {
            this.mShouldThrowException = shouldThrow;
        }

        boolean wasUnsubscribeCalled() {
            return mUnsubscribeCalled;
        }

        @Override
        public void unsubscribe() throws SubscriptionException {
            if (mShouldThrowException) {
                throw new SubscriptionException("Test Subscription Exception");
            }
            mUnsubscribeCalled = true;
        }
    }

    @Before
    public void setUp() {
        mFakeSubscription = new FakeConfigurablePublisherSubscription();
        mSubscriptionAidl = new ConfigurablePublisherSubscriptionAidlImpl(mFakeSubscription);
    }

    @Test
    public void unsubscribe_success_callsUnderlyingUnsubscribe() throws RemoteException {
        mSubscriptionAidl.unsubscribe();
        assertTrue(mFakeSubscription.wasUnsubscribeCalled());
    }

    @Test
    public void unsubscribe_subscriptionThrows_throwsRemoteException() {
        mFakeSubscription.setShouldThrowException(true);

        assertThrows(RemoteException.class, () -> mSubscriptionAidl.unsubscribe());
    }
}
