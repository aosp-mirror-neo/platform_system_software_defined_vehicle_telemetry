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

import com.google.protobuf.Int32Value;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
public class ProtobufCodecTest {
    private final ProtobufCodec<Int32Value> mCodec = new ProtobufCodec<>(Int32Value.parser());

    @Test
    public void encode_validMessage_encodesMessageToBytes() {
        Int32Value message = Int32Value.newBuilder().setValue(123).build();
        byte[] encoded = mCodec.encode(message);
        assertThat(encoded).isEqualTo(message.toByteArray());
    }

    @Test
    public void decode_validBytes_decodesBytesToMessage() throws Exception {
        Int32Value message = Int32Value.newBuilder().setValue(456).build();
        byte[] bytes = message.toByteArray();
        Int32Value decoded = mCodec.decode(bytes);
        assertThat(decoded).isEqualTo(message);
    }

    @Test
    public void roundtrip_validMessage_preservesMessage() throws Exception {
        Int32Value message = Int32Value.newBuilder().setValue(789).build();
        byte[] encoded = mCodec.encode(message);
        Int32Value decoded = mCodec.decode(encoded);
        assertThat(decoded).isEqualTo(message);
    }

    @Test
    public void encode_null_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> mCodec.encode(null));
    }

    @Test
    public void decode_null_throwsNullPointerException() throws Exception {
        assertThrows(NullPointerException.class, () -> mCodec.decode(null));
    }
}
