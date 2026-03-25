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

package com.android.sdv.telemetry.sdk.descriptor_generator;

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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

@RunWith(JUnit4.class)
public final class DescriptorGeneratorTest {
    @Rule public TemporaryFolder mTempDir = new TemporaryFolder();

    private Path getTempDir() {
        return mTempDir.getRoot().toPath();
    }

    @Test
    public void testGeneratedClass_containsExpectedData() throws Exception {
        Class<?> generatedClass =
                Class.forName("com.google.android.sdv.telemetry.schemas.TestMessageSchema");

        var messageFullName = (String) generatedClass.getMethod("getMessageFullName").invoke(null);
        var descriptorSetBytes =
                (byte[]) generatedClass.getMethod("getFileDescriptorSet").invoke(null);

        assertThat(messageFullName).isEqualTo(".com.google.sdv.telemetry.test.TestMessage");

        FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(descriptorSetBytes);
        assertThat(fileDescriptorSet.getFileCount()).isEqualTo(1);
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
            "--proto_path", "./reference_files",
            "--output_file", "InvalidMessageSchema.java",
            "--output_class", "InvalidMessageSchema",
            "--output_package", "com.google.android.sdv.telemetry.schemas",
            "--message_full_name", "com.google.sdv.telemetry.test.InvalidMessage"
        };

        var thrown = assertThrows(RuntimeException.class, () -> DescriptorGenerator.main(args));
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
            "--proto_file",
            message1File.toString(),
            "--proto_path",
            message1File.getParent().toString(),
            "--message_full_name",
            "test.Message1",
            "--output_file",
            outputFile.toString(),
            "--output_class",
            "OutputClass",
            "--output_package",
            ""
        };
        DescriptorGenerator.main(args);

        ClassParseResult result = parseGeneratedJavaClass(outputFile, "OutputClass");
        assertThat(result.messageFullName).isEqualTo(".test.Message1");

        assertThat(result.fds.getFileCount()).isEqualTo(2);
        FileDescriptorProto file1FdProto = getFdByName(result.fds, "Message1.proto");
        FileDescriptorProto file2FdProto = getFdByName(result.fds, "Message2.proto");

        FileDescriptor file2Fd = FileDescriptor.buildFrom(file2FdProto, new FileDescriptor[0]);
        assertThat(file2Fd.findMessageTypeByName("Message2")).isNotNull();

        FileDescriptor file1Fd =
                FileDescriptor.buildFrom(file1FdProto, new FileDescriptor[] {file2Fd});
        assertThat(file1Fd.findMessageTypeByName("Message1")).isNotNull();
    }

    @Test
    public void testGeneration_fromProtoWithRootMessageNameNotFound_fails() throws Exception {
        String[] args = {
            "--proto_file", "./reference_files/TestMessageSchema.proto",
            "--proto_path", "./reference_files",
            "--output_file", "TestMessageSchema.java",
            "--output_class", "TestMessageSchema",
            "--output_package", "com.google.android.sdv.telemetry.schemas",
            "--message_full_name", "com.example.NonExistingMessage"
        };

        var thrown = assertThrows(RuntimeException.class, () -> DescriptorGenerator.main(args));
        assertThat(thrown)
                .hasMessageThat()
                .isEqualTo(
                        "Message named 'com.example.NonExistingMessage' not found in descriptor"
                                + " set. Available message names:"
                                + " com.google.sdv.telemetry.test.TestMessage");
    }

    @Test
    public void testGeneration_withProtoFileImportingAnotherProtoFileInOtherDirectory_succeeds()
            throws Exception {
        Path outputFile = getTempDir().resolve("OutputClass.java");

        Path message1File =
                writeTempFile(
                        "dir1/Message1.proto",
                        """
                        syntax = "proto3";
                        package test;

                        import "Message2.proto";

                        message Message1 { Message2 b = 1; }
                        """);
        Path message2File =
                writeTempFile(
                        "dir2/Message2.proto",
                        """
                        syntax = "proto3";
                        package test;

                        message Message2 { int32 value = 1; }
                        """);

        String[] args = {
            "--proto_file", message1File.toString(),
            "--proto_path", message1File.getParent().toString(),
            "--proto_path", message2File.getParent().toString(),
            "--message_full_name", "test.Message1",
            "--output_file", outputFile.toString(),
            "--output_class", "OutputClass",
            "--output_package", ""
        };
        DescriptorGenerator.main(args);

        ClassParseResult result = parseGeneratedJavaClass(outputFile, "OutputClass");
        assertThat(result.messageFullName).isEqualTo(".test.Message1");

        assertThat(result.fds.getFileCount()).isEqualTo(2);
        FileDescriptorProto file1FdProto = getFdByName(result.fds, "Message1.proto");
        FileDescriptorProto file2FdProto = getFdByName(result.fds, "Message2.proto");

        FileDescriptor file2Fd = FileDescriptor.buildFrom(file2FdProto, new FileDescriptor[0]);
        assertThat(file2Fd.findMessageTypeByName("Message2")).isNotNull();

        FileDescriptor file1Fd =
                FileDescriptor.buildFrom(file1FdProto, new FileDescriptor[] {file2Fd});
        assertThat(file1Fd.findMessageTypeByName("Message1")).isNotNull();
    }

    @Test
    public void testGeneration_withUnusedImport_fails() throws Exception {
        Path outputFile = getTempDir().resolve("OutputClass.java");

        Path message1File =
                writeTempFile(
                        "Message1.proto",
                        """
                        syntax = "proto3";
                        package test;

                        import "Message2.proto"; // This import is unused.

                        message Message1 { int32 value = 1; }
                        """);
        writeTempFile(
                "Message2.proto",
                """
                syntax = "proto3";

                package test;

                message Message2 { int32 value = 1; }
                """);

        String[] args = {
            "--proto_file",
            message1File.toString(),
            "--proto_path",
            message1File.getParent().toString(),
            "--message_full_name",
            "test.Message1",
            "--output_file",
            outputFile.toString(),
            "--output_class",
            "OutputClass",
            "--output_package",
            ""
        };
        var e = assertThrows(RuntimeException.class, () -> DescriptorGenerator.main(args));
        assertThat(e).hasMessageThat().contains("warning: Import Message2.proto is unused.");
    }

    @Test
    public void testGeneration_withUnusedImport_succeedsIfWarningsNonFatal() throws Exception {
        Path outputFile = getTempDir().resolve("OutputClass.java");

        Path message1File =
                writeTempFile(
                        "Message1.proto",
                        """
                        syntax = "proto3";
                        package test;

                        import "Message2.proto"; // This import is unused.

                        message Message1 { int32 value = 1; }
                        """);
        writeTempFile(
                "Message2.proto",
                """
                syntax = "proto3";

                package test;

                message Message2 { int32 value = 1; }
                """);

        String[] args = {
            "--fatal_warnings",
            "false",
            "--proto_file",
            message1File.toString(),
            "--proto_path",
            message1File.getParent().toString(),
            "--message_full_name",
            "test.Message1",
            "--output_file",
            outputFile.toString(),
            "--output_class",
            "OutputClass",
            "--output_package",
            ""
        };
        DescriptorGenerator.main(args);

        ClassParseResult result = parseGeneratedJavaClass(outputFile, "OutputClass");
        assertThat(result.messageFullName).isEqualTo(".test.Message1");

        assertThat(result.fds.getFileCount()).isEqualTo(2);
        FileDescriptorProto file1FdProto = getFdByName(result.fds, "Message1.proto");
        FileDescriptorProto file2FdProto = getFdByName(result.fds, "Message2.proto");

        FileDescriptor file2Fd = FileDescriptor.buildFrom(file2FdProto, new FileDescriptor[0]);
        assertThat(file2Fd.findMessageTypeByName("Message2")).isNotNull();

        FileDescriptor file1Fd = FileDescriptor.buildFrom(file1FdProto, new FileDescriptor[0]);
        assertThat(file1Fd.findMessageTypeByName("Message1")).isNotNull();
    }

    private Path writeTempFile(String relativePath, String content) throws IOException {
        Path filePath = getTempDir().resolve(relativePath);
        Files.createDirectories(filePath.getParent());
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

    private static record ClassParseResult(FileDescriptorSet fds, String messageFullName) {}

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
        var fileDescriptorSetBytes = (byte[]) cls.getMethod("getFileDescriptorSet").invoke(null);
        var messageFullName = (String) cls.getMethod("getMessageFullName").invoke(null);

        return new ClassParseResult(
                FileDescriptorSet.parseFrom(fileDescriptorSetBytes), messageFullName);
    }
}
