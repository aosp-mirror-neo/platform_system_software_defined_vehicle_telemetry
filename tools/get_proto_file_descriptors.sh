#!/bin/bash
# Copyright 2024 Google LLC

# Simple tool to convert a `.proto` file into a file descriptor set. Pass the
# path to the `.proto` file as the argument, and this will create
# `descriptors.bp`, descriptors.textproto`, and `descriptors.txt` (for easy
# copy-pasting of binary data in a programming language of your choice)` files.

set -eu -o pipefail

if [[ -z "$ANDROID_BUILD_TOP" ]]
then
  echo "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target."
  exit 1
fi

TEMP_DIR=$(mktemp -d)

# Remove temp dir when the script is done or cancelled
trap "rm -rf $TEMP_DIR" 0 2 3 15 # Run the command when one of these signals is received: 0=exit shell, 2=interrupt, 3=quit, 15=terminate

if ! command -v protoc &> /dev/null || ! command -v xxd &> /dev/null || ! command -v gqui &> /dev/null; then
  echo "[ERROR] Either protoc, xxd, or gqui is not installed. Please install them as follows:"
  echo ""
  echo "sudo glinux-add-repo gqui"
  echo "sudo apt update"
  echo "sudo apt install -y gqui protobuf-compiler xxd"
  exit 1
fi

# These are necessary for gqui to pick up the right protos.
ln -s $ANDROID_BUILD_TOP/external/protobuf/src/google $TEMP_DIR/google
ln -s $ANDROID_BUILD_TOP/system                       $TEMP_DIR/system

convert_proto() {
  echo "Converting $1 to $2 as $3"

  cd $TEMP_DIR
  gqui from "$1" \
      proto $3 \
      --outfile="$2"
  cd -
}

protoc --include_imports --descriptor_set_out=$(pwd)/descriptors.pb $1

convert_proto \
      "rawproto:$(pwd)/descriptors.pb" \
      "textproto:$(pwd)/descriptors.textproto" \
      google.protobuf.FileDescriptorSet

xxd -p $(pwd)/descriptors.pb | tr -d '\n' | sed 's/\(..\)/0x\1,/g;s/,$//' > $(pwd)/descriptors.txt

echo "Wrote the following files:"
echo ""
echo "$(pwd)/descriptors.pb"
echo "$(pwd)/descriptors.textproto"
echo "$(pwd)/descriptors.txt"

exit 0
