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
public class PassthroughCodecTest {
    private final PassthroughCodec mCodec = new PassthroughCodec();

    @Test
    public void encode_validInput_returnsInputBytes() {
        byte[] input = new byte[] {1, 2, 3};
        byte[] encoded = mCodec.encode(input);
        assertThat(encoded).isEqualTo(input);
    }

    @Test
    public void decode_validInput_returnsInputBytes() {
        byte[] input = new byte[] {4, 5, 6};
        byte[] decoded = mCodec.decode(input);
        assertThat(decoded).isEqualTo(input);
    }

    @Test
    public void roundtrip_validInput_preservesMessage() {
        byte[] input = new byte[] {1, 2, 3};
        byte[] encoded = mCodec.encode(input);
        byte[] decoded = mCodec.decode(encoded);
        assertThat(decoded).isEqualTo(input);
    }

    @Test
    public void encode_null_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> mCodec.encode(null));
    }

    @Test
    public void decode_null_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> mCodec.decode(null));
    }
}
