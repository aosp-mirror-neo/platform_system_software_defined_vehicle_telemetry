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

import android.os.RemoteException;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.core.util.Preconditions;

import com.android.sdv.telemetry.sdk.configurable_publisher_registry.codec.Codec;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherSubscription;
import google.sdv.telemetry.configurable_publisher.ISubscriber;

import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;

/**
 * Base class for publishers.
 *
 * <p>Overwrite `subscribe` and `getOnDemand` in your implementation.
 */
public abstract class Publisher<T, U> {
    private static final String TAG = "Publisher";

    private abstract static class PublisherBinder extends IConfigurablePublisher.Stub {
        @Override
        public final String getInterfaceHash() {
            return IConfigurablePublisher.HASH;
        }

        @Override
        public final int getInterfaceVersion() {
            return IConfigurablePublisher.VERSION;
        }
    }

    private abstract static class SubscriptionBinder
            extends IConfigurablePublisherSubscription.Stub {
        @Override
        public final String getInterfaceHash() {
            return IConfigurablePublisherSubscription.HASH;
        }

        @Override
        public final int getInterfaceVersion() {
            return IConfigurablePublisherSubscription.VERSION;
        }
    }

    private final Codec<T> mMessageCodec;
    private final Codec<U> mConfigCodec;
    private final PublisherMetadata mMetadata;

    /**
     * Create a new publisher.
     *
     * @param messageCodec The codec to use for messages
     * @param configCodec The codec to use for configs
     * @param metadata The metadata for this publisher
     */
    public Publisher(
            final Codec<T> messageCodec,
            final Codec<U> configCodec,
            final PublisherMetadata metadata) {
        mMessageCodec = messageCodec;
        mConfigCodec = configCodec;
        mMetadata = metadata;
    }

    /**
     * Subscribe to messages.
     *
     * <p>Never publishes any messages if not overwritten.
     *
     * @param config Decoded configuration
     * @param subscriber The subscriber to receive messages
     */
    public void subscribe(Optional<U> config, Flow.Subscriber<T> subscriber) {
        subscriber.onSubscribe(
                new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                        Preconditions.checkArgument(n > 0, "Must be positive");
                    }

                    @Override
                    public void cancel() {
                        subscriber.onComplete();
                    }
                });
    }

    /**
     * Get the current message, if available.
     *
     * <p>Always returns `Optional::empty()` if not overwritten.
     *
     * @param config Decoded configuration
     * @return The current message, or `Optional::empty()` if no message is available
     */
    public Optional<T> getOnDemand(Optional<U> config) {
        return Optional.empty();
    }

    final PublisherMetadata getMetadata() {
        return mMetadata;
    }

    final IConfigurablePublisher toBinder() {
        return new PublisherBinder() {
            @Override
            public IConfigurablePublisherSubscription createSubscription(
                    ISubscriber subscriber, @Nullable byte[] configBytes) throws RemoteException {
                return Publisher.this.subscribe(decodeConfig(configBytes), subscriber);
            }

            @Override
            public @Nullable byte[] getLatestMessage(@Nullable byte[] configBytes)
                    throws RemoteException {
                return Publisher.this
                        .getOnDemand(decodeConfig(configBytes))
                        .map(Publisher.this.mMessageCodec::encode)
                        .orElse(null);
            }

            private Optional<U> decodeConfig(@Nullable byte[] configBytes) throws RemoteException {
                if (configBytes == null) {
                    return Optional.empty();
                }
                U config;
                try {
                    config = Publisher.this.mConfigCodec.decode(configBytes);
                } catch (Exception e) {
                    Log.e(
                            TAG,
                            "Failed to decode config for service "
                                    + Publisher.this.mMetadata.name(),
                            e);
                    throw new RemoteException("Failed to decode config: " + e.getMessage());
                }
                return Optional.of(config);
            }
        };
    }

    private final IConfigurablePublisherSubscription subscribe(
            Optional<U> config, ISubscriber subscriberBinder) {
        var subscriber =
                new Flow.Subscriber<T>() {
                    private Subscription mSubscription;

                    @Override
                    public void onSubscribe(Subscription subscription) {
                        mSubscription = subscription;
                        mSubscription.request(
                                1); // request 1 message (onNext is called once it arrives)
                    }

                    @Override
                    public void onNext(T message) {
                        try {
                            subscriberBinder.sendData(Publisher.this.mMessageCodec.encode(message));
                        } catch (RemoteException e) {
                            Log.e(
                                    TAG,
                                    "Failed to publish message to Telemetry for service "
                                            + mMetadata.name(),
                                    e);
                            throw new RuntimeException(e);
                        }
                        mSubscription.request(
                                1); // request 1 message (onNext is called once it arrives)
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Log.e(
                                TAG,
                                "An error occurred while publishign to Telemetry for service "
                                        + mMetadata.name(),
                                throwable);
                        throw new RuntimeException(throwable);
                    }

                    @Override
                    public void onComplete() {
                        Log.i(
                                TAG,
                                "Publishing to Telemetry for service "
                                        + mMetadata.name()
                                        + " completed");
                    }
                };

        this.subscribe(config, subscriber);

        return new SubscriptionBinder() {
            @Override
            public void unsubscribe() throws RemoteException {
                subscriber.mSubscription.cancel();
            }
        };
    }
}
