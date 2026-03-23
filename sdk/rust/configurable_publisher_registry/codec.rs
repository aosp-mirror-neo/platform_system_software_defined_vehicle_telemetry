// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The codec module provides helpers for serialization and deserialization of objects.

use protobuf::Message;
use std::convert::Infallible;
use std::error::Error;
use std::marker::PhantomData;

/// Interface for encoding and decoding objects of type T to and from byte vectors.
pub trait Codec: 'static {
    /// The type to encode from and decode to.
    type T;
    /// Errors returned during encoding
    type EncodeError: Error;
    /// Errors returned during decoding
    type DecodeError: Error;

    /// Encodes the given object into a byte vectors.
    fn encode(object: Self::T) -> Result<Vec<u8>, Self::EncodeError>;

    /// Decodes the given byte vector into an object of type T.
    fn decode(bytes: &[u8]) -> Result<Self::T, Self::DecodeError>;
}

/// A codec that does nothing.
///
/// This codec is typically used when a publisher does not have a configuration, and, as such,
/// doesn't care about the received configuration.
impl Codec for () {
    type T = ();
    type EncodeError = Infallible;
    type DecodeError = Infallible;

    fn encode(_object: Self::T) -> Result<Vec<u8>, Self::EncodeError> {
        Ok(vec![])
    }

    fn decode(_bytes: &[u8]) -> Result<Self::T, Self::DecodeError> {
        Ok(())
    }
}

/// A simple codec that passes through byte arrays without modification.
#[non_exhaustive]
pub struct PassthroughCodec;

impl Codec for PassthroughCodec {
    type T = Vec<u8>;
    type EncodeError = Infallible;
    type DecodeError = Infallible;

    fn encode(vec: Self::T) -> Result<Vec<u8>, Self::EncodeError> {
        Ok(vec)
    }

    fn decode(bytes: &[u8]) -> Result<Self::T, Self::DecodeError> {
        Ok(bytes.to_vec())
    }
}

/// A codec for encoding and decoding protobuf messages.
#[non_exhaustive]
pub struct ProtobufCodec<T: Message>(PhantomData<T>);

impl<T: Message> Codec for ProtobufCodec<T> {
    type T = T;
    type EncodeError = protobuf::Error;
    type DecodeError = protobuf::Error;

    fn encode(message: Self::T) -> Result<Vec<u8>, Self::EncodeError> {
        message.write_to_bytes()
    }

    fn decode(bytes: &[u8]) -> Result<Self::T, Self::DecodeError> {
        Self::T::parse_from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::container_eq;
    use googletest::matchers::eq;
    use googletest::matchers::ok;
    use protobuf::well_known_types::wrappers::Int32Value;

    #[googletest::test]
    fn unit_codec_encode_success() {
        let input = ();
        let result = <()>::encode(input);
        expect_that!(result, ok(container_eq([])));
    }

    #[googletest::test]
    fn unit_codec_decode_success() {
        let input = &[1, 2, 3];
        let result = <()>::decode(input);
        expect_that!(result, ok(eq(())));
    }

    #[googletest::test]
    fn vec_u8_codec_encode_success() {
        let input = vec![1, 2, 3];
        let result = PassthroughCodec::encode(input);
        expect_that!(result, ok(container_eq([1, 2, 3])));
    }

    #[googletest::test]
    fn vec_u8_codec_decode_success() {
        let input = &[4, 5, 6];
        let result = PassthroughCodec::decode(input);
        expect_that!(result, ok(container_eq([4, 5, 6])));
    }

    #[googletest::test]
    fn protobuf_codec_encode_success() {
        let input = Int32Value { value: 12, ..Default::default() };
        let result = ProtobufCodec::<Int32Value>::encode(input);
        expect_that!(result, ok(container_eq([8, 12])));
    }

    #[googletest::test]
    fn protobuf_codec_decode_success() {
        let input = [8, 34];
        let result = ProtobufCodec::<Int32Value>::decode(&input);
        expect_that!(result, ok(eq(&Int32Value { value: 34, ..Default::default() })));
    }
}
