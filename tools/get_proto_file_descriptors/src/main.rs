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

//! Tool to generate file descriptor set proto and file descriptor proto from a .proto file for
//! debugging purposes.
//!
//! Usage:
//! ```
//! mm sdv_telemetry_get_proto_file_descriptors && sdv_telemetry_get_proto_file_descriptors path/to/file.proto
//! ```

use std::env;
use std::fmt::Write;
use std::fs;
use std::path::{absolute, Path, PathBuf};

use anyhow::bail;
use clap::Parser as ClapParser;
use protobuf::text_format;
use protobuf::Message;
use protobuf_parse::Parser as ProtoParser;

#[derive(ClapParser, Debug)]
#[command()]
struct Args {
    /// Path to the .proto file
    #[arg(value_name = "PROTO_FILE")]
    proto_file: String,

    /// Output directory. Defaults to the current directory.
    #[arg(long = "out-dir", value_name = "DIR")]
    out_dir: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let proto_path = absolute(&args.proto_file)?;
    let out_dir = match args.out_dir {
        Some(out_dir) => PathBuf::from(out_dir),
        None => env::current_dir()?,
    };
    fs::create_dir_all(&out_dir)?;

    let file_descriptor_set = ProtoParser::new()
        .protoc()
        .protoc_path(Path::new(
            // When run in a genrule, we need to access `aprotoc` from `tools/out/bin`. When run
            // outside of a genrule, we need to access `aprotoc` from path. Since there is no good
            // way to detect whether we are running as part of a genrule, use existence of the
            // `aprotoc` binary in `tools/out/bin` as an indicator.
            if fs::exists("tools/out/bin/aprotoc")? { "tools/out/bin/aprotoc" } else { "aprotoc" },
        ))
        .include(proto_path.parent().unwrap())
        .input(proto_path)
        .file_descriptor_set()?;

    let [first_file_descriptor, ..] = &file_descriptor_set.file[..] else {
        bail!("Expected at least one file descriptor in file descriptor set.");
    };

    let files = [
        (
            "descriptor_set.binpb",
            Box::new(|| Ok(file_descriptor_set.write_to_bytes()?))
                as Box<dyn Fn() -> anyhow::Result<Vec<u8>>>,
        ),
        (
            "descriptor_set.txtpb",
            Box::new(|| Ok(text_format::print_to_string_pretty(&file_descriptor_set).into()))
                as Box<dyn Fn() -> anyhow::Result<Vec<u8>>>,
        ),
        (
            "descriptor_set.txt",
            Box::new(|| to_comma_separated_hex(&file_descriptor_set))
                as Box<dyn Fn() -> anyhow::Result<Vec<u8>>>,
        ),
        (
            "descriptor.binpb",
            Box::new(|| Ok(first_file_descriptor.write_to_bytes()?))
                as Box<dyn Fn() -> anyhow::Result<Vec<u8>>>,
        ),
        (
            "descriptor.txtpb",
            Box::new(|| Ok(text_format::print_to_string_pretty(first_file_descriptor).into()))
                as Box<dyn Fn() -> anyhow::Result<Vec<u8>>>,
        ),
        (
            "descriptor.txt",
            Box::new(|| to_comma_separated_hex(first_file_descriptor))
                as Box<dyn Fn() -> anyhow::Result<Vec<u8>>>,
        ),
    ];

    for (name, writer) in &files {
        fs::write(out_dir.join(name), writer()?)?;
    }

    println!("Wrote the following files:");
    println!();
    for (name, _) in files {
        println!("{}", out_dir.join(name).display());
    }

    Ok(())
}

fn to_comma_separated_hex(message: &impl Message) -> anyhow::Result<Vec<u8>> {
    let bytes = message.write_to_bytes()?;
    let mut output = String::with_capacity(bytes.len() * 5);
    for (i, byte) in bytes.iter().enumerate() {
        write!(&mut output, "0x{byte:02x}")?;
        if i < bytes.len() - 1 {
            output.push(',');
        }
    }
    Ok(output.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::descriptor::{FileDescriptorProto, FileDescriptorSet};
    use protobuf::Message;

    fn contains_message(file_descriptor_proto: &FileDescriptorProto) -> bool {
        file_descriptor_proto
            .message_type
            .iter()
            .any(|message_type| message_type.name.as_ref().unwrap() == "SampleMessage")
    }

    #[test]
    fn parse_descriptor_from_binary_proto_success() {
        let input = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.binpb"));

        let file_descriptor_proto = FileDescriptorProto::parse_from_bytes(input).unwrap();

        assert!(contains_message(&file_descriptor_proto));
    }

    #[test]
    fn parse_descriptor_set_from_binary_proto_success() {
        let input = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor_set.binpb"));

        let file_descriptor_set = FileDescriptorSet::parse_from_bytes(input).unwrap();

        assert!(file_descriptor_set.file.iter().any(contains_message));
    }

    #[test]
    fn parse_descriptor_from_binary_proto_hex_success() {
        let input = include_str!(concat!(env!("OUT_DIR"), "/descriptor.txt"));
        let input = input
            .split(',')
            .map(|hex| u8::from_str_radix(hex.trim_start_matches("0x"), 16))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let file_descriptor_proto = FileDescriptorProto::parse_from_bytes(&input).unwrap();

        assert!(contains_message(&file_descriptor_proto));
    }

    #[test]
    fn parse_descriptor_set_from_binary_proto_hex_success() {
        let input = include_str!(concat!(env!("OUT_DIR"), "/descriptor_set.txt"));
        let input = input
            .split(',')
            .map(|hex| u8::from_str_radix(hex.trim_start_matches("0x"), 16))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let file_descriptor_set = FileDescriptorSet::parse_from_bytes(&input).unwrap();

        assert!(file_descriptor_set.file.iter().any(contains_message));
    }

    #[test]
    fn parse_descriptor_from_text_proto_success() {
        let input = include_str!(concat!(env!("OUT_DIR"), "/descriptor.txtpb"));

        let file_descriptor_proto =
            text_format::parse_from_str::<FileDescriptorProto>(input).unwrap();

        assert!(contains_message(&file_descriptor_proto));
    }

    #[test]
    fn parse_descriptor_set_from_text_proto_success() {
        let input = include_str!(concat!(env!("OUT_DIR"), "/descriptor_set.txtpb"));

        let file_descriptor_set = text_format::parse_from_str::<FileDescriptorSet>(input).unwrap();

        assert!(file_descriptor_set.file.iter().any(contains_message));
    }
}
