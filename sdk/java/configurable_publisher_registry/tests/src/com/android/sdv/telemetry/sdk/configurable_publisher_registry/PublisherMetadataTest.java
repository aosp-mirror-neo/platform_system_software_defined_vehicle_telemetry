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

import androidx.test.ext.junit.runners.AndroidJUnit4;

import google.sdv.telemetry.configurable_publisher.PublisherInfo;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
public class PublisherMetadataTest {
    @Test
    public void publisherMetadata_create_setsPropertiesCorrectly() {
        PublisherName name = new PublisherName("testService");
        byte[] fileDescriptorSet = new byte[] {1, 2, 3};
        String messageFullName = "testMessageType";
        boolean supportsOnDemandFetching = true;

        PublisherMetadata metadata =
                new PublisherMetadata(
                        name, fileDescriptorSet, messageFullName, supportsOnDemandFetching);

        assertThat(metadata.name()).isEqualTo(name);
        assertThat(metadata.fileDescriptorSet()).isEqualTo(fileDescriptorSet);
        assertThat(metadata.messageFullName()).isEqualTo(messageFullName);
        assertThat(metadata.supportsOnDemandFetching()).isEqualTo(supportsOnDemandFetching);
    }

    @Test
    public void toAidl_validMetadata_returnsPublisherInfo() {
        PublisherName name = new PublisherName("testService2");
        byte[] fileDescriptorSet = new byte[] {4, 5, 6};
        String messageFullName = "testMessageType2";
        boolean supportsOnDemandFetching = false;

        PublisherMetadata metadata =
                new PublisherMetadata(
                        name, fileDescriptorSet, messageFullName, supportsOnDemandFetching);

        PublisherInfo info = metadata.toAidl();
        assertThat(info.serviceName).isEqualTo(name.value());
        assertThat(info.fileDescriptorSet).isEqualTo(fileDescriptorSet);
        assertThat(info.messageTypeName).isEqualTo(messageFullName);
        assertThat(info.supportsGet).isEqualTo(supportsOnDemandFetching);
    }
}
