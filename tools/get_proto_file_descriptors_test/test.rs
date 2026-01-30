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

use protobuf::descriptor::{FileDescriptorProto, FileDescriptorSet};
use protobuf::text_format::parse_from_str;
use protobuf::Message;
use std::fs;
use std::path::{Path, PathBuf};

fn get_fixture_path(file: &str) -> PathBuf {
    const FIXTURE_FOLDER: &str = "get_proto_file_descriptors_test";
    Path::new(FIXTURE_FOLDER).join(file)
}

#[test]
fn parse_descriptor_from_binary_proto_success() {
    let input = fs::read(get_fixture_path("descriptor.pb")).unwrap();

    let _proto = FileDescriptorProto::parse_from_bytes(&input).unwrap();
}

#[test]
fn parse_descriptor_set_from_binary_proto_success() {
    let input = fs::read(get_fixture_path("descriptor_set.pb")).unwrap();

    let _proto = FileDescriptorSet::parse_from_bytes(&input).unwrap();
}

#[test]
fn parse_descriptor_from_binary_proto_hex_success() {
    let input = fs::read_to_string(get_fixture_path("descriptor.txt")).unwrap();
    let input = input
        .split(',')
        .map(|hex| u8::from_str_radix(hex.trim_start_matches("0x"), 16))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let _proto = FileDescriptorProto::parse_from_bytes(&input).unwrap();
}

#[test]
fn parse_descriptor_set_from_binary_proto_hex_success() {
    let input = fs::read_to_string(get_fixture_path("descriptor_set.txt")).unwrap();
    let input = input
        .split(',')
        .map(|hex| u8::from_str_radix(hex.trim_start_matches("0x"), 16))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let _proto = FileDescriptorSet::parse_from_bytes(&input).unwrap();
}

#[test]
fn parse_descriptor_from_text_proto_success() {
    let input = fs::read_to_string(get_fixture_path("descriptor.textproto")).unwrap();

    let _proto = parse_from_str::<FileDescriptorProto>(&input).unwrap();
}

#[test]
fn parse_descriptor_set_from_text_proto_success() {
    let input = fs::read_to_string(get_fixture_path("descriptor_set.textproto")).unwrap();

    let _proto = parse_from_str::<FileDescriptorSet>(&input).unwrap();
}
