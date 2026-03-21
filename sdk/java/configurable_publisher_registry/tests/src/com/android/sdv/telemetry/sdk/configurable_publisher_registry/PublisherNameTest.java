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

import static org.junit.Assert.assertThrows;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
public class PublisherNameTest {
    @Test
    public void constructor_validValue_createsInstance() {
        PublisherName name = new PublisherName("test-publisher");
        assertThat(name.value()).isEqualTo("test-publisher");
    }

    @Test
    public void constructor_nullValue_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new PublisherName(null));
    }

    @Test
    public void constructor_emptyValue_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new PublisherName(""));
    }

    @Test
    public void equals_sameValue_isTrue() {
        PublisherName name1 = new PublisherName("publisher");
        PublisherName name2 = new PublisherName("publisher");

        assertThat(name1).isEqualTo(name2);
    }

    @Test
    public void equals_differentValue_isFalse() {
        PublisherName name1 = new PublisherName("publisher1");
        PublisherName name2 = new PublisherName("publisher2");

        assertThat(name1).isNotEqualTo(name2);
    }
}
