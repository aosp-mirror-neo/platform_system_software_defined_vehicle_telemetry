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

package com.android.sdv.telemetry.sdk.configurable_publisher_registry.codec;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
public class IgnoreCodecTest {
    private final IgnoreCodec mCodec = new IgnoreCodec();

    @Test
    public void encode_returnsEmptyByteArray() {
        byte[] encoded = mCodec.encode(null);
        assertThat(encoded).isEmpty();
    }

    @Test
    public void decode_returnsNull() {
        assertThat(mCodec.decode(new byte[0])).isNull();
        assertThat(mCodec.decode(new byte[] {1, 2, 3})).isNull();
    }

    @Test
    public void decode_null_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> mCodec.decode(null));
    }
}
