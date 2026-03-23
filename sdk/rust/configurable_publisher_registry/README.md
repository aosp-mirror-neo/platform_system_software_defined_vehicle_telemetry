<!--
  Copyright (C) 2026 The Android Open Source Project

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Configurable Publisher Registry Rust SDK

The Configurable Publisher Registry Rust SDK provides a framework for creating
and registering custom Telemetry publishers, which allows you to easily publish
messages to the Telemetry Service.

## Overview

This SDK simplifies the process of creating publishers and registering them with
the system's `IConfigurablePublisherRegistry`.

Key components of the SDK include:

-  **`Publisher`**: The trait for defining a publisher. It handles the
   underlying Binder communication and requires you to implement the logic for
   producing data based on an optional configuration.
-  **`Registry`**: Handles registering and unregistering your publisher with the
   system service.
-  **`Codec<T>`**: Trait for encoding messages and decoding configurations
   between Rust objects and byte arrays.
-  **`PublisherMetadata`**: Contains information about the publisher, such as
   its name, Protobuf file descriptor set, message name, and whether it supports
   fetching data on demand.

To use this SDK from your `rust_library` / `rust_binary` rule, add the following
dependency to it:

```blueprint
rust_<library|binary> {
    rustlibs: [
        // ...
        "libsdv_telemetry_sdk_configurable_publisher_registry_rust",
        // ...
    ],
}
```

## Creating a Publisher

To create a custom publisher, implement the `Publisher` trait for your struct.
You need to provide:

1.  A `MessageCodec` for your message type.
2.  A `ConfigCodec` for your configuration type.
3.  Implementation of `get_metadata(&mut self) -> PublisherMetadata` describing
    your publisher.
4.  (Optional) implementation of `subscribe(&mut self, config) -> BoxStream<'static, T>`,
    if your publisher supports streaming messages.
5.  (Optional) implementation of `get_on_demand(&mut self, config) -> Option<T>`,
    if your publisher supports fetching data on demand.

### Provided Codecs

The SDK provides several built-in codecs in the `codec` module:

-  **`ProtobufCodec<T: Message>`**: Encodes/decodes Protobuf messages.
-  **`()` (unit codec)**: Ignores messages. This is useful for when you don't
   care about the message type, such as when your publisher is not configurable.
-  **`PassthroughCodec`**: Simply passes byte arrays through without
   modification. This is mostly useful for debugging.

### Example

Here is a simplified example of creating a publisher emitting instances of
`MyProtobufMessage`, and ignoring the config argument.

```rust
use futures::future;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use sdv_telemetry_sdk_configurable_publisher_registry::{Publisher, PublisherMetadata, PublisherName};
use sdv_telemetry_sdk_configurable_publisher_registry::codec::ProtobufCodec;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::sleep;
use tokio_stream::wrappers::WatchStream;

// Assuming MyProtobufMessage is a protobuf message generated in your project.

pub struct MyPublisher {
    tx: watch::Sender<Option<MyProtobufMessage>>,
}

impl MyPublisher {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self { tx }
    }

    pub fn start_publishing(&self) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            loop {
                let msg: MyProtobufMessage = ...;
                tx.send_replace(Some(message));

                sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

impl Publisher for MyPublisher {
    type MessageCodec = ProtobufCodec<MyProtobufMessage>;
    type ConfigCodec = ();

    fn get_metadata(&mut self) -> PublisherMetadata {
        let mut metadata =
            PublisherMetadata::new("my_custom_publisher".parse::<PublisherName>().unwrap());

        let message_descriptor = MyProtobufMessage::descriptor();
        let file_descriptor_set = FileDescriptorSet {
            file: vec![message_descriptor.file_descriptor_proto().to_owned()],
            ..Default::default()
        };

        metadata.file_descriptor_set = file_descriptor_set.write_to_bytes().unwrap();
        metadata.message_full_name = "my.package.MyProtobufMessage".to_owned();
        metadata.supports_on_demand_fetching = true;
        metadata
    }

    fn subscribe(&mut self, _config: Option<()>) -> BoxStream<'static, MyProtobufMessage> {
        WatchStream::new(self.tx.subscribe()).filter_map(future::ready).boxed()
    }

    fn get_on_demand(&mut self, _config: Option<()>) -> Option<MyProtobufMessage> {
        self.tx.borrow().to_owned()
    }
}
```

Note that the above example uses a `tokio::sync::watch` channel to handle
state and streaming, but this is not required, as long as you return a valid
`BoxStream` in `subscribe`. If required, you may want to use `ProtobufCodec`
instead of `()` for the `ConfigCodec`, so that your publisher can react
differently depending on the configuration.

## Registering a Publisher

Once you have created your publisher instance, you register it using the
`Registry`. Registration returns a `Registration` object, allowing you to
unregister the publisher when it's no longer needed.

```rust
use sdv_telemetry_sdk_configurable_publisher_registry::Registry;
use tokio::sync::watch;

// 1. Instantiate your publisher
let my_publisher = MyPublisher::new();

// 2. Start publishing.
my_publisher.start_publishing();

// 3. Obtain the registry and register the publisher
let mut registry = Registry::new()?;
let registration = registry.register(my_publisher)?;

// The publisher is now active and can be discovered by and subscribed to by
// the Telemetry Service.

// 4. Unregister when done
registry.unregister(registration)?;
```
