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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

/**
 * A codec for encoding and decoding Protocol Buffer messages.
 *
 * @param <T> The type of the Protocol Buffer message, extending {@link MessageLite}.
 */
public final class ProtobufCodec<T extends MessageLite> implements Codec<T> {
    private final Parser<T> mParser;

    /**
     * Creates a new ProtobufCodec with the given parser.
     *
     * @param parser The parser for the message type.
     */
    public ProtobufCodec(Parser<T> parser) {
        mParser = parser;
    }

    /**
     * Encodes the given Protocol Buffer message into a byte array.
     *
     * @param message The message to encode.
     * @return The encoded byte array.
     */
    @Override
    public byte[] encode(T message) {
        Preconditions.checkNotNull(message);
        return message.toByteArray();
    }

    /**
     * Decodes the given byte array into a Protocol Buffer message.
     *
     * @param bytes The byte array to decode.
     * @return The decoded Protocol Buffer message.
     * @throws InvalidProtocolBufferException If an error occurs during parsing.
     */
    @Override
    public T decode(byte[] bytes) throws InvalidProtocolBufferException {
        Preconditions.checkNotNull(bytes);
        return mParser.parseFrom(bytes);
    }
}
