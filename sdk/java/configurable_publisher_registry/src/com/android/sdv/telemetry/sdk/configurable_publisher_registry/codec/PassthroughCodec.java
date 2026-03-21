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

import androidx.core.util.Preconditions;

/** A simple codec that passes through byte arrays without any modification. */
public final class PassthroughCodec implements Codec<byte[]> {
    /**
     * Returns the input byte array as is.
     *
     * @param bytes The byte array to encode.
     * @return The same byte array.
     */
    @Override
    public byte[] encode(byte[] bytes) {
        Preconditions.checkNotNull(bytes);
        return bytes;
    }

    /**
     * Returns the input byte array as is.
     *
     * @param bytes The byte array to decode.
     * @return The same byte array.
     */
    @Override
    public byte[] decode(byte[] bytes) {
        Preconditions.checkNotNull(bytes);
        return bytes;
    }
}
