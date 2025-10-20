// Copyright 2025 Google LLC

package google.sdv.telemetry.descriptor_generator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ProtoDescriptorGeneratorTest {
  private static final String EXPECTED_ROOT_MESSAGE_NAME =
      ".com.google.sdv.telemetry.test.TestMessage";

  @Test
  public void testGeneratedClass_hasCorrectRootMessageName() throws Exception {
    // This test loads the compiled class and checks the root message name constant.
    Class<?> generatedClass =
        Class.forName("com.google.android.sdv.telemetry.schemas.TestMessageSchema");
    Field rootMessageNameField = generatedClass.getField("ROOT_MESSAGE_NAME");
    String rootMessageName = (String) rootMessageNameField.get(null);
    assertThat(rootMessageName).isEqualTo(EXPECTED_ROOT_MESSAGE_NAME);
  }

  @Test
  public void testGeneratedClass_hasValidDescriptor() throws Exception {
    // This test validates the core functionality: ensuring the DESCRIPTOR byte array is present,
    // is a valid protobuf descriptor, and contains the correct information.
    Class<?> generatedClass =
        Class.forName("com.google.android.sdv.telemetry.schemas.TestMessageSchema");
    Field descriptorField = generatedClass.getField("DESCRIPTOR");
    byte[] descriptorBytes = (byte[]) descriptorField.get(null);

    FileDescriptorProto fileDescriptorProto = FileDescriptorProto.parseFrom(descriptorBytes);

    assertThat(fileDescriptorProto.getName()).isEqualTo("TestMessageSchema.proto");

    FileDescriptor fileDescriptor =
        FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[0]);
    assertThat(fileDescriptor.findMessageTypeByName("TestMessage")).isNotNull();
  }

  @Test
  public void testGeneration_fromProtoWithSyntaxError_failsWithProtocError() throws Exception {
    String protocToolPath = "/usr/bin/protoc";
    String invalidProtoPath = "InvalidMessageSchema.proto";

    String[] args = {
      "--proto_file", invalidProtoPath,
      "--protoc_tool", protocToolPath,
      "--output_file", "InvalidMessageSchema.java",
      "--output_class", "InvalidMessageSchema",
      "--output_package", "com.google.android.sdv.telemetry.schemas",
      "--root_message_name", "com.google.sdv.telemetry.test.InvalidMessage"
    };

    try {
      ProtoDescriptorGenerator.main(args);
      fail("Expected a RuntimeException to be thrown, but none was.");
    } catch (RuntimeException e) {
      assertThat(e.getMessage()).isEqualTo("protoc tool execution failed with exit code: 1");
    }
  }
}
