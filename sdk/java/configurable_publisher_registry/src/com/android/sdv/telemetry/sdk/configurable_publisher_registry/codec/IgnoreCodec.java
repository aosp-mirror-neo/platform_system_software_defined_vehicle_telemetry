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

/**
 * A codec that does nothing.
 *
 * <p>This codec is typically used when a publisher does not have a configuration, and, as such,
 * doesn't care about the received configuration.
 */
public final class IgnoreCodec implements Codec<Void> {
    /**
     * "Encodes" a {@link Void} object into an empty byte array.
     *
     * @param object The object to encode.
     * @return Always returns an empty byte array.
     */
    @Override
    public byte[] encode(Void object) {
        return new byte[0];
    }

    /**
     * "Decodes" a byte array into a {@link Void} object.
     *
     * @param bytes The byte array to decode.
     * @return Always returns {@code Void}.
     */
    @Override
    public Void decode(byte[] bytes) {
        Preconditions.checkNotNull(bytes);
        return null;
    }
}
