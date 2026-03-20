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

import android.os.IBinder;
import android.os.RemoteException;

import androidx.annotation.VisibleForTesting;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry;
import google.sdv.telemetry.java.Binder.BinderServiceAccessor;

public final class Registry {
    static {
        System.loadLibrary("binder_accessor");
    }

    private static final String sServiceName =
            "google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry/default";

    private final IConfigurablePublisherRegistry mRegistryBinder;

    public Registry() throws RemoteException {
        IBinder binder = BinderServiceAccessor.getService(sServiceName);
        if (binder == null) {
            throw new RemoteException("Binder service '" + sServiceName + "' not found");
        }
        mRegistryBinder = IConfigurablePublisherRegistry.Stub.asInterface(binder);
    }

    @VisibleForTesting
    Registry(IConfigurablePublisherRegistry registry) {
        mRegistryBinder = registry;
    }

    final Registration register(PublisherMetadata metadata, IConfigurablePublisher publisher)
            throws RemoteException {
        mRegistryBinder.registerPublisher(metadata.toAidl(), publisher);
        return new Registration(this, metadata.name());
    }

    final void unregister(PublisherName publisherName) throws RemoteException {
        mRegistryBinder.unregisterPublisher(publisherName.value());
    }
}
