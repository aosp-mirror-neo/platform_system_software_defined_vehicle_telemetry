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

#![cfg(test)]

use protobuf::reflect::ReflectValueRef;
use protobuf::{reflect::FieldDescriptor, reflect::MessageDescriptor, MessageFull};
use serde::ser::SerializeStruct;
use std::sync::OnceLock;
use std::time::Duration;

/// Represents a row of the data generator CSV file. The columns are based on the fields of a
/// protobuf message `T`.
pub(crate) struct CsvRow<T: MessageFull> {
    delay_before_publishing: Option<Duration>,

    value: T,
}

impl<T: MessageFull> CsvRow<T> {
    pub(crate) fn no_delay(value: T) -> Self {
        Self { delay_before_publishing: None, value }
    }

    #[expect(dead_code, reason = "Not yet used.")]
    pub(crate) fn delayed(delay: Duration, value: T) -> Self {
        Self { delay_before_publishing: Some(delay), value }
    }

    /// This static method returns a reference to `T`'s `MessageDescriptor` and its
    /// `FieldDescriptor`s. We cannot simply call `T::descriptor().fields()` inside of the
    /// `serialize` method below, because `serialize_field` expects keys with static lifetime.
    pub(crate) fn descriptors() -> &'static (MessageDescriptor, Vec<FieldDescriptor>) {
        static DESCRIPTORS: OnceLock<(MessageDescriptor, Vec<FieldDescriptor>)> = OnceLock::new();
        DESCRIPTORS.get_or_init(|| {
            let descriptor = T::descriptor();
            let fields = descriptor.fields().collect::<Vec<_>>();
            (descriptor, fields)
        })
    }
}

impl<T: MessageFull> serde::Serialize for CsvRow<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let (descriptor, fields) = Self::descriptors();

        let mut state = serializer.serialize_struct(
            descriptor.full_name(),
            // "_delay_before_publishing" + protobuf field count
            1 + fields.len(),
        )?;

        state.serialize_field(
            "_delay_before_publishing",
            &self.delay_before_publishing.map(|d| d.as_millis().to_string()).unwrap_or_default(),
        )?;
        for field in fields {
            state.serialize_field(
                field.name(),
                &match field
                    .get_singular(&self.value)
                    .expect("Only singular fields are currently implemented.")
                {
                    ReflectValueRef::U32(value) => value.to_string(),
                    ReflectValueRef::U64(value) => value.to_string(),
                    ReflectValueRef::I32(value) => value.to_string(),
                    ReflectValueRef::I64(value) => value.to_string(),
                    ReflectValueRef::F32(value) => value.to_string(),
                    ReflectValueRef::F64(value) => value.to_string(),
                    ReflectValueRef::Bool(value) => value.to_string(),
                    ReflectValueRef::String(value) => value.to_owned(),
                    ReflectValueRef::Bytes(_) => {
                        unimplemented!("Bytes field type is not yet implemented.")
                    }
                    ReflectValueRef::Enum(_, _) => {
                        unimplemented!("Enum field type is not yet implemented.")
                    }
                    ReflectValueRef::Message(_) => {
                        unimplemented!("Message field type is not yet implemented.")
                    }
                },
            )?;
        }

        state.end()
    }
}

pub(crate) fn into_csv_bytes<T: MessageFull>(data: &[CsvRow<T>]) -> Vec<u8> {
    let mut csv_writer = csv::Writer::from_writer(vec![]);
    for row in data {
        csv_writer.serialize(row).unwrap();
    }

    csv_writer.into_inner().unwrap()
}
