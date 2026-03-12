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

import android.os.IBinder;
import android.os.RemoteException;

import com.google.common.annotations.VisibleForTesting;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry;
import google.sdv.telemetry.configurable_publisher.PublisherInfo;
import google.sdv.telemetry.java.Binder.BinderServiceAccessor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Client facing class that maintains the list of configurable publishers in memory */
public class ConfigurablePublisherRegistry {

    private static final String CONF_PUB_REGISTRY_URI =
            "google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry/default";

    private static IConfigurablePublisherRegistry registryService;
    private final Set<String> registeredPublishers;

    /**
     * Factory method to create a new registry client. Connects to the remote service.
     *
     * @return A new instance of ConfigurablePublisherRegistry.
     * @throws ConfigurablePublisherRegistryException if the client cannot connect to the service.
     */
    public static ConfigurablePublisherRegistry connect()
            throws ConfigurablePublisherRegistryException {
        System.loadLibrary("binder_accessor");
        IBinder regService = BinderServiceAccessor.getService(CONF_PUB_REGISTRY_URI);
        registryService = IConfigurablePublisherRegistry.Stub.asInterface(regService);

        // Check if the service was actually found. If not, throw an exception.
        if (regService == null) {
            throw ConfigurablePublisherRegistryException.createServiceFailed(
                    "Configurable publisher registry service was not found. It may not be"
                            + " running.");
        }

        IConfigurablePublisherRegistry serviceInterface =
                IConfigurablePublisherRegistry.Stub.asInterface(regService);
        if (serviceInterface == null) {
            throw ConfigurablePublisherRegistryException.createServiceFailed(
                    "Failed to cast binder proxy to IConfigurablePublisherRegistry interface.");
        }

        return new ConfigurablePublisherRegistry(serviceInterface);
    }

    @VisibleForTesting
    ConfigurablePublisherRegistry(IConfigurablePublisherRegistry service) {
        this.registryService = service;
        this.registeredPublishers = Collections.synchronizedSet(new HashSet<>());
    }

    /**
     * Attempts to register a ConfigurablePublisher with the telemetry service.
     *
     * @param publisherInfo The metadata about the publisher.
     * @param publisher The actual publisher implementation.
     * @throws ConfigurablePublisherRegistryException if the registration fails due to a binder
     *     error.
     */
    public void registerPublisher(PublisherInfoJava publisherInfo, ConfigurablePublisher publisher)
            throws ConfigurablePublisherRegistryException {

        String serviceName = publisherInfo.getServiceName();
        synchronized (registeredPublishers) {
            if (registeredPublishers.contains(serviceName)) {
                throw ConfigurablePublisherRegistryException.publisherAlreadyRegistered(
                        serviceName);
            }

            ConfigurablePublisherAidlImpl publisherImpl =
                    new ConfigurablePublisherAidlImpl(publisher);
            PublisherInfo publisherInfoAidl = publisherInfo.toAidlRepresentation();

            try {
                registryService.registerPublisher(publisherInfoAidl, publisherImpl);
            } catch (RemoteException e) {
                throw ConfigurablePublisherRegistryException.binderError(e);
            }

            this.registeredPublishers.add(serviceName);
        }
    }

    /**
     * Attempts to unregister a ConfigurablePublisher from the telemetry service.
     *
     * @param serviceName The unique name of the service to unregister.
     * @throws ConfigurablePublisherRegistryException if the unregistration fails due to a binder
     *     error.
     */
    public void unregisterPublisher(String serviceName)
            throws ConfigurablePublisherRegistryException {
        synchronized (registeredPublishers) {
            if (!registeredPublishers.contains(serviceName)) {
                throw ConfigurablePublisherRegistryException.publisherNotFound(serviceName);
            }

            try {
                registryService.unregisterPublisher(serviceName);
            } catch (RemoteException e) {
                throw ConfigurablePublisherRegistryException.binderError(e);
            }
            this.registeredPublishers.remove(serviceName);
        }
    }
}
