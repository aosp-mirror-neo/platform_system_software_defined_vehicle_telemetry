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

package google.sdv.telemetry.descriptor_generator;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.fail;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.lang.reflect.Field;

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
        // This test validates that the FILE_DESCRIPTOR_SET byte array is a valid FileDescriptorSet
        // and that the necessary message types can be resolved from it.
        Class<?> generatedClass =
                Class.forName("com.google.android.sdv.telemetry.schemas.TestMessageSchema");
        Field descriptorSetField = generatedClass.getField("FILE_DESCRIPTOR_SET");
        byte[] descriptorSetBytes = (byte[]) descriptorSetField.get(null);

        FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(descriptorSetBytes);
        FileDescriptorProto fileDescriptorProto = fileDescriptorSet.getFile(0);
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
