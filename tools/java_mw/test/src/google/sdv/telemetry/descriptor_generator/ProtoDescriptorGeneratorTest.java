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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

@RunWith(JUnit4.class)
public final class ProtoDescriptorGeneratorTest {
    private static final String EXPECTED_ROOT_MESSAGE_NAME =
            ".com.google.sdv.telemetry.test.TestMessage";

    @Rule public TemporaryFolder mTempDir = new TemporaryFolder();

    private Path getTempDir() {
        return mTempDir.getRoot().toPath();
    }

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
        String[] args = {
            "--proto_file", "./reference_files/InvalidMessageSchema.proto",
            "--output_file", "InvalidMessageSchema.java",
            "--output_class", "InvalidMessageSchema",
            "--output_package", "com.google.android.sdv.telemetry.schemas",
            "--root_message_name", "com.google.sdv.telemetry.test.InvalidMessage"
        };

        var thrown =
                assertThrows(RuntimeException.class, () -> ProtoDescriptorGenerator.main(args));
        assertThat(thrown)
                .hasMessageThat()
                .isEqualTo(
                        "protoc execution failed (exit code: 1).\n"
                            + "reference_files/InvalidMessageSchema.proto:26:1: Expected \";\".\n");
    }

    @Test
    public void testGeneration_withProtoFileImportingAnotherProtoFile_succeeds() throws Exception {
        Path outputFile = getTempDir().resolve("OutputClass.java");

        Path message1File =
                writeTempFile(
                        "Message1.proto",
                        """
                        syntax = "proto3";
                        package test;

                        import "Message2.proto";

                        message Message1 { Message2 b = 1; }
                        """);
        writeTempFile(
                "Message2.proto",
                """
                syntax = "proto3";
                package test;

                message Message2 { int32 value = 1; }
                """);

        String[] args = {
            "--proto_file", message1File.toString(),
            "--root_message_name", "test.Message1",
            "--output_file", outputFile.toString(),
            "--output_class", "OutputClass",
            "--output_package", ""
        };
        ProtoDescriptorGenerator.main(args);

        ClassParseResult result = parseGeneratedJavaClass(outputFile, "OutputClass");
        assertThat(result.rootMessageName).isEqualTo(".test.Message1");

        assertThat(result.fds.getFileCount()).isEqualTo(2);
        FileDescriptorProto file1FdProto = getFdByName(result.fds, "Message1.proto");
        FileDescriptorProto file2FdProto = getFdByName(result.fds, "Message2.proto");

        FileDescriptor file2Fd = FileDescriptor.buildFrom(file2FdProto, new FileDescriptor[0]);
        assertThat(file2Fd.findMessageTypeByName("Message2")).isNotNull();

        FileDescriptor file1Fd =
                FileDescriptor.buildFrom(file1FdProto, new FileDescriptor[] {file2Fd});
        assertThat(file1Fd.findMessageTypeByName("Message1")).isNotNull();
    }

    private Path writeTempFile(String name, String content) throws IOException {
        Path filePath = getTempDir().resolve(name);
        Files.writeString(filePath, content);
        return filePath;
    }

    private static FileDescriptorProto getFdByName(FileDescriptorSet fds, String name) {
        for (FileDescriptorProto fileProto : fds.getFileList()) {
            if (name.equals(fileProto.getName())) {
                return fileProto;
            }
        }
        fail("Could not find file descriptor named " + name + " in file descriptor set.");
        return null;
    }

    private static record ClassParseResult(FileDescriptorSet fds, String rootMessageName) {}

    private static ClassParseResult parseGeneratedJavaClass(Path path, String className)
            throws Exception {
        assertThat(Files.exists(path)).isTrue();

        // Step 1: Compile the Java code.
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        int exitCode = compiler.run(null, null, null, path.toString());
        assertThat(exitCode).isEqualTo(0);

        // Step 2: Load the compiled class.
        var classLoader = URLClassLoader.newInstance(new URL[] {path.getParent().toUri().toURL()});
        Class<?> cls = Class.forName(className, true, classLoader);

        // Step 3: Read the generated fields.
        var fileDescriptorSetBytes = (byte[]) cls.getField("FILE_DESCRIPTOR_SET").get(null);
        var rootMessageName = (String) cls.getField("ROOT_MESSAGE_NAME").get(null);

        return new ClassParseResult(
                FileDescriptorSet.parseFrom(fileDescriptorSetBytes), rootMessageName);
    }
}
