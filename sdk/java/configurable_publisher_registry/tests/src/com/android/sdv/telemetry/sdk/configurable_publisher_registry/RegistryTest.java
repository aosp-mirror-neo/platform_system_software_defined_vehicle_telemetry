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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import google.sdv.telemetry.configurable_publisher.IConfigurablePublisher;
import google.sdv.telemetry.configurable_publisher.IConfigurablePublisherRegistry;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

@RunWith(AndroidJUnit4.class)
public class RegistryTest {
    @Rule public MockitoRule mRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    @Mock private IConfigurablePublisherRegistry mMockIConfigurablePublisherRegistry;
    @Mock private IConfigurablePublisher mMockPublisher;

    private Registry mRegistry;

    @Before
    public void setUp() {
        mRegistry = new Registry(mMockIConfigurablePublisherRegistry);
    }

    @Test
    public void register_validPublisher_registersAndUnregisters() throws Exception {
        PublisherMetadata metadata =
                new PublisherMetadata(
                        new PublisherName("testService"), new byte[0], "message", true);
        var publisher = mock(Publisher.class);
        when(publisher.getMetadata()).thenReturn(metadata);
        when(publisher.toBinder()).thenReturn(mMockPublisher);

        try (Registration registration = mRegistry.register(publisher)) {
            verify(mMockIConfigurablePublisherRegistry)
                    .registerPublisher(refEq(metadata.toAidl()), eq(mMockPublisher));
        }
        verify(mMockIConfigurablePublisherRegistry)
                .unregisterPublisher(eq(metadata.name().value()));
    }
}
