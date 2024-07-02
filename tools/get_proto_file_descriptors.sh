#!/bin/bash
# Copyright 2024 Google LLC

# Simple tool to convert a `.proto` file into a file descriptor set and a file
# descriptor proto. Pass the path to the `.proto` file as the argument, and this
# will create the following files:
#
# - `FileDescriptorSet`: `descriptor_set.bp`, descriptor_set.textproto`, and
#   `descriptor_set.txt`
# - `FileDescriptorProto`: `descriptor.bp`, descriptor.textproto`, and
#   `descriptor.txt`

set -eu -o pipefail

if [[ -z "${ANDROID_BUILD_TOP+x}" ]]
then
  echo "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target."
  exit 1
fi

TEMP_DIR=$(mktemp -d)

# Remove temp dir when the script is done or cancelled
trap "rm -rf $TEMP_DIR" 0 2 3 15 # Run the command when one of these signals is received: 0=exit shell, 2=interrupt, 3=quit, 15=terminate

if ! command -v protoc &> /dev/null || ! command -v xxd &> /dev/null || ! command -v txtpbfmt &> /dev/null || ! command -v gqui &> /dev/null; then
  echo "[ERROR] Either protoc, xxd, txtpbfmt, or gqui is not installed. Please install them as follows:"
  echo ""
  echo "sudo glinux-add-repo gqui"
  echo "sudo apt update"
  echo "sudo apt install -y gqui protobuf-compiler xxd text-proto-format"
  exit 1
fi

# LINT.IfChange

# These are necessary for gqui to pick up the right protos.
ln -s $ANDROID_BUILD_TOP/external/protobuf/src/google $TEMP_DIR/google
ln -s $ANDROID_BUILD_TOP/system                       $TEMP_DIR/system

convert_proto() {
  cd $TEMP_DIR

  # Step 1: Convert the `FileDescriptorSet` into a textproto.
  gqui from "$1" \
      proto google.protobuf.FileDescriptorSet \
      --protoprint_annotations=no \
      --protoprint_record_separator=newline \
      --outfile="$2"

  # Step 2: Convert the `FileDescriptorSet` into its first `FileDescriptorProto`.
  gqui from "$1" \
      proto google.protobuf.FileDescriptorSet \
      select file[0].raw_ format "%r" \
      --outfile="$3"

  # Step 2: Convert the `FileDescriptorProto` into a textproto.
  gqui from "$3" \
      proto google.protobuf.FileDescriptorProto \
      --protoprint_annotations=no \
      --protoprint_record_separator=newline \
      --outfile="$4"

  cd -
}

protoc --include_imports --descriptor_set_out=$(pwd)/descriptor_set.pb $1

convert_proto \
      "rawproto:$(pwd)/descriptor_set.pb" \
      "textproto:$(pwd)/descriptor_set.textproto" \
      "rawproto:$(pwd)/descriptor.pb" \
      "textproto:$(pwd)/descriptor.textproto"

# Remove colons that are not supported by Rust's textproto implementation.
txtpbfmt -skip_all_colons $(pwd)/descriptor_set.textproto
txtpbfmt -skip_all_colons $(pwd)/descriptor.textproto

xxd -p $(pwd)/descriptor_set.pb | tr -d '\n' | sed 's/\(..\)/0x\1,/g;s/,$//' > $(pwd)/descriptor_set.txt
xxd -p $(pwd)/descriptor.pb | tr -d '\n' | sed 's/\(..\)/0x\1,/g;s/,$//' > $(pwd)/descriptor.txt

echo "Wrote the following files:"
echo ""
echo "$(pwd)/descriptor_set.pb"
echo "$(pwd)/descriptor_set.textproto"
echo "$(pwd)/descriptor_set.txt"
echo "$(pwd)/descriptor.pb"
echo "$(pwd)/descriptor.textproto"
echo "$(pwd)/descriptor.txt"

exit 0

# LINT.ThenChange(get_proto_file_descriptors_test/descriptor.textproto)
#
# Note: This is just a reminder to re-run this script whenever you make changes
# to it, using the following command, and update the `descriptor_*` files in the
# `get_proto_file_descriptors_test` folder:
#
# ```
# cd tools/get_proto_file_descriptors_test &&  ../get_proto_file_descriptors.sh sample_publisher.proto
# ```
