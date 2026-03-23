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

# Configurable Publisher Registry Java SDK

The Configurable Publisher Registry Java SDK provides a framework for creating
and registering custom Telemetry publishers, which allows you to easily publish
messages to the Telemetry Service.

## Overview

This SDK simplifies the process of creating publishers and registering them with
the system's `IConfigurablePublisherRegistry`.

Key components of the SDK include:

-  **`Publisher<T, U>`**: The abstract base class for defining a publisher. It
   handles the underlying Binder communication and requires you to implement the
   logic for producing data (`T`) based on an optional configuration (`U`).
-  **`Registry`**: Handles registering and unregistering your publisher with the
   system service.
-  **`Codec<T>`**: Interfaces for encoding messages and decoding configurations
   between Java objects and byte arrays.
-  **`PublisherMetadata`**: Contains information about the publisher, such as
   its name, Protobuf file descriptor set, message name, and whether it supports
   fetching data on demand.

To use this SDK from your `java_library` / `java_binary` rule, add the following
dependencies to it:

```blueprint
java_<library|binary> {
    static_libs: [
        // ...
        "libsdv_telemetry_sdk_configurable_publisher_registry_java",
        // ...
    ],
    jni_libs: [
        // ...
        "libbinder_accessor",
        // ...
    ],
}
```

## Creating a Publisher

To create a custom publisher, extend the `Publisher<T, U>` class. You need to
provide:

1.  A `Codec<T>` for your message type.
2.  A `Codec<U>` for your configuration type.
3.  `PublisherMetadata` describing your publisher.
4.  (Optional) implementation of `void subscribe(config, subscriber)`, if your
    publisher supports streaming messages.
5.  (Optional) implementation of `Optional<T> getOnDemand(config)`, if your
    publisher supports fetching data on demand.

### Provided Codecs

The SDK provides several built-in codecs in the
`com.android.sdv.telemetry.sdk.configurable_publisher_registry.codec` package:

-  **`ProtobufCodec<T extends MessageLite>`**: Encodes/decodes Protobuf
   messages.
-  **`IgnoreCodec`**: Ignores messages. This is useful for when you don't care
   about the message type, such as when your publisher is not configurable.
-  **`PassthroughCodec`**: Simply passes byte arrays through without
   modification. This is mostly useful for debugging.

### Example

Here is a simplified example of creating a publisher emitting instances of
`MyProtobufMessage`, and ignoring the config argument.

```java
import com.android.sdv.telemetry.sdk.configurable_publisher_registry.Publisher;
import com.android.sdv.telemetry.sdk.configurable_publisher_registry.PublisherMetadata;
import com.android.sdv.telemetry.sdk.configurable_publisher_registry.PublisherName;
import com.android.sdv.telemetry.sdk.configurable_publisher_registry.codec.IgnoreCodec;
import com.android.sdv.telemetry.sdk.configurable_publisher_registry.codec.ProtobufCodec;

import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

public class MyPublisher extends Publisher<MyProtobufMessage, Void> {
    private static final PublisherMetadata sMetadata = new PublisherMetadata(
                new PublisherName("my_custom_publisher"),
                // A serialized protobuf `FileDescriptorSet` (e.g., generated
                // via the Configurable Publisher Registry Middleware).
                new byte[] { ... },
                "my.package.MyProtobufMessage",
                /* supportsOnDemandFetching */ true);

    private final mPublisher = new SubmissionPublisher<MyProtobufMessage>();

    public MyPublisher() {
        super(
                new ProtobufCodec<>(MyProtobufMessage.parser()),
                new IgnoreCodec(),
                sMetadata);

        var thread = new Thread(() -> {
            while (true) {
                MyProtobufMessage msg = ...;
                mPublisher.submit(msg);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        thread.start();
        thread.setDaemon(true);
    }

    @Override
    public void subscribe(Optional<Void> config, Flow.Subscriber<MyProtobufMessage> subscriber) {
        mPublisher.subscribe(subscriber);
    }

    @Override
    public Optional<MyProtobufMessage> getOnDemand(Optional<Void> config) {
        MyProtobufMessage msg = ...;
        return Optional.of(msg);
    }
}
```

Note that the above example uses a separate thread set up in its constructor for
publishing messages every second, but this is not required, as long as you
follow the `Flow.Subscriber` interface's contract in `MyPublisher::subscribe`.
The use of `SubmissionPublisher` is also not required, but is an easy example to
use here. If required, you may want to use `ProtobufCodec` instead of
`IgnoreCodec` for the config codec, so that your publisher can react differently
depending on the configuration.

## Registering a Publisher

Once you have created your publisher instance, you register it using the
`Registry`. Registration returns a `Registration` object which implements
`AutoCloseable`, allowing you to unregister the publisher when it's no longer
needed.

```java
import android.os.RemoteException;

import com.android.sdv.telemetry.sdk.configurable_publisher_registry.Registration;
import com.android.sdv.telemetry.sdk.configurable_publisher_registry.Registry;

// 1. Instantiate your publisher
MyPublisher myPublisher = new MyPublisher();

try {
    // 2. Obtain the registry and register the publisher
    Registry registry = new Registry();
    Registration registration = registry.register(myPublisher);

    // The publisher is now active and can be discovered by and subscribed to by
    // the Telemetry Service.

    // 3. Unregister when done (or use a try-with-resources block)
    registration.close();
} catch (RemoteException e) {
    // TODO: Handle binder errors
}
```
