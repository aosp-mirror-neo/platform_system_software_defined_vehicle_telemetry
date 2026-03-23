/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.android.sdv.telemetry.sdk.configurable_publisher_registry;

import static com.google.common.truth.Truth.assertThat;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import androidx.core.util.Preconditions;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import com.android.sdv.telemetry.sdk.configurable_publisher_registry.codec.ProtobufCodec;

import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import google.sdv.telemetry.configurable_publisher.ISubscriber;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;

@RunWith(AndroidJUnit4.class)
public class PublisherTest {
    @Rule public MockitoRule mRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    @Mock private IConfigurablePublisherRegistry mMockIConfigurablePublisherRegistry;
    @Mock private ISubscriber mMockISubscriber;

    private Registry mRegistry;

    // The exact metadata doesn't really matter, because it is passed as is through Binder to
    // Telemetry - but these tests mock Binder, so Telemetry never receives it.
    private static final PublisherMetadata sMetadata =
            new PublisherMetadata(new PublisherName("service"), new byte[0], "message", true);

    @Before
    public void setUp() {
        mRegistry = new Registry(mMockIConfigurablePublisherRegistry);
    }

    /**
     * A publisher that publishes `StringValue` protobuf messages, and takes `Int32Value` protobuf
     * messages as its config.
     *
     * <p>When subscribed to, it publishes 3 consecutive integers (as strings). If no config is
     * provided, they start at 10. If a config is provided, they start at the value provided in the
     * config.
     *
     * <p>When getting the latest message, it returns the provided config as a string. If no config
     * is provided, it returns an empty Optional.
     */
    private static class TestPublisher extends Publisher<StringValue, Int32Value> {

        TestPublisher(PublisherMetadata metadata) {
            super(
                    new ProtobufCodec<>(StringValue.parser()),
                    new ProtobufCodec<>(Int32Value.parser()),
                    metadata);
        }

        private static class Subscription implements Flow.Subscription {
            private final Flow.Subscriber<? super StringValue> mSubscriber;
            private final int mOffset;
            private final Semaphore mSemaphore = new Semaphore(0);
            private final Thread mThread;

            Subscription(Flow.Subscriber<? super StringValue> subscriber, int offset) {
                mSubscriber = subscriber;
                mOffset = offset;
                mThread = new Thread(this::produce);
                mThread.setDaemon(true);
            }

            @Override
            public void request(long n) {
                Preconditions.checkArgument(n > 0, "Must be positive");
                mSemaphore.release((int) Math.min(n, Integer.MAX_VALUE));
            }

            @Override
            public void cancel() {
                mThread.interrupt();
            }

            private void start() {
                mThread.start();
            }

            private void produce() {
                try {
                    for (int i = 0; i < 3; i++) {
                        int value = i + mOffset;
                        // Wait until the next value is requested. May throw `InterruptedException`
                        // if the subscription is/was cancelled.
                        mSemaphore.acquire();
                        mSubscriber.onNext(
                                StringValue.newBuilder().setValue(Integer.toString(value)).build());
                    }
                    mSubscriber.onComplete();
                } catch (InterruptedException e) {
                    // Flow.Subscription::cancel was called.
                    return;
                } catch (Exception e) {
                    mSubscriber.onError(e);
                    return;
                }
            }
        }

        @Override
        public void subscribe(
                Optional<Int32Value> config, Flow.Subscriber<StringValue> subscriber) {
            int offset = config.map(Int32Value::getValue).orElse(10);
            var subscription = new Subscription(subscriber, offset);
            subscriber.onSubscribe(subscription);
            subscription.start();
        }

        @Override
        public Optional<StringValue> getOnDemand(Optional<Int32Value> config) {
            return config.map(
                    c -> StringValue.newBuilder().setValue(Integer.toString(c.getValue())).build());
        }
    }

    /**
     * Converts an integer into a serialized `Int32Value` protobuf message.
     *
     * @param value The integer to serialize.
     * @return The serialized `Int32Value` protobuf message.
     */
    private static byte[] int32Bytes(int value) {
        return Int32Value.newBuilder().setValue(value).build().toByteArray();
    }

    /**
     * Converts a string into a serialized `StringValue` protobuf message.
     *
     * @param value The string to serialize.
     * @return The serialized `StringValue` protobuf message.
     */
    private static byte[] stringBytes(String value) {
        return StringValue.newBuilder().setValue(value).build().toByteArray();
    }

    @Test
    public void register_validPublisher_registersAndUnregistersWithRegistry() throws Exception {
        TestPublisher publisher = new TestPublisher(sMetadata);

        try (Registration registration = mRegistry.register(publisher)) {
            verify(mMockIConfigurablePublisherRegistry)
                    .registerPublisher(refEq(sMetadata.toAidl()), any());
        }
        verify(mMockIConfigurablePublisherRegistry).unregisterPublisher(eq("service"));
    }

    @Test
    public void register_validPublisher_createSubscription() throws Exception {
        TestPublisher publisher = new TestPublisher(sMetadata);

        try (Registration registration = mRegistry.register(publisher)) {
            // Capture the `IConfigurablePublisher` instance that would be passed through Binder to
            // Telemetry.
            ArgumentCaptor<IConfigurablePublisher> captor =
                    ArgumentCaptor.forClass(IConfigurablePublisher.class);
            verify(mMockIConfigurablePublisherRegistry)
                    .registerPublisher(refEq(sMetadata.toAidl()), captor.capture());
            IConfigurablePublisher binderPublisher = captor.getValue();

            IConfigurablePublisherSubscription subscription =
                    binderPublisher.createSubscription(mMockISubscriber, null);
            assertThat(subscription).isNotNull();
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("10"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("11"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("12"));
            subscription.unsubscribe();

            subscription = binderPublisher.createSubscription(mMockISubscriber, int32Bytes(0));
            assertThat(subscription).isNotNull();
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("0"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("1"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("2"));
            subscription.unsubscribe();

            subscription = binderPublisher.createSubscription(mMockISubscriber, int32Bytes(123));
            assertThat(subscription).isNotNull();
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("123"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("124"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("125"));
            subscription.unsubscribe();

            subscription = binderPublisher.createSubscription(mMockISubscriber, int32Bytes(-456));
            assertThat(subscription).isNotNull();
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("-456"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("-455"));
            verify(mMockISubscriber, timeout(50)).sendData(stringBytes("-454"));
            subscription.unsubscribe();
        }
        verify(mMockIConfigurablePublisherRegistry).unregisterPublisher(eq("service"));
    }

    @Test
    public void register_validPublisher_getLatestMessage() throws Exception {
        TestPublisher publisher = new TestPublisher(sMetadata);

        try (Registration registration = mRegistry.register(publisher)) {
            // Capture the `IConfigurablePublisher` instance that would be passed through Binder to
            // Telemetry.
            ArgumentCaptor<IConfigurablePublisher> captor =
                    ArgumentCaptor.forClass(IConfigurablePublisher.class);
            verify(mMockIConfigurablePublisherRegistry)
                    .registerPublisher(refEq(sMetadata.toAidl()), captor.capture());
            IConfigurablePublisher binderPublisher = captor.getValue();

            byte[] latestMessage = binderPublisher.getLatestMessage(null);
            assertThat(latestMessage).isNull();

            latestMessage = binderPublisher.getLatestMessage(int32Bytes(0));
            assertThat(latestMessage).isEqualTo(stringBytes("0"));

            latestMessage = binderPublisher.getLatestMessage(int32Bytes(123));
            assertThat(latestMessage).isEqualTo(stringBytes("123"));

            latestMessage = binderPublisher.getLatestMessage(int32Bytes(-456));
            assertThat(latestMessage).isEqualTo(stringBytes("-456"));
        }
        verify(mMockIConfigurablePublisherRegistry).unregisterPublisher(eq("service"));
    }
}
