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

/**
 * Interface for encoding and decoding objects of type T to and from byte arrays.
 *
 * @param <T> The type of the object to be encoded or decoded.
 */
public interface Codec<T> {
    /**
     * Encodes the given object into a byte array.
     *
     * @param object The object to encode.
     * @return The encoded byte array.
     */
    byte[] encode(T object);

    /**
     * Decodes the given byte array into an object of type T.
     *
     * @param bytes The byte array to decode.
     * @return The decoded object.
     * @throws Exception If an error occurs during decoding.
     */
    T decode(byte[] bytes) throws Exception;
}
