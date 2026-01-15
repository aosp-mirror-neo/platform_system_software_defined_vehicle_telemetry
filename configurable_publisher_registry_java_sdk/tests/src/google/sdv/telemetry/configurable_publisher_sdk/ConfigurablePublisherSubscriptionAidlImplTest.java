// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
