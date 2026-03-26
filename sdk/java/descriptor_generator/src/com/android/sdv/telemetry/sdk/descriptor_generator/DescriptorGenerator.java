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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import javax.lang.model.element.Modifier;

/**
 * A host-side tool to generate a Java file containing a serialized protobuf FileDescriptorProto.
 */
final class DescriptorGenerator {
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static class Args {
        @Parameter(
                names = "--proto_file",
                required = true,
                description = "Path to the input .proto file.")
        private String protoFile;

        @Parameter(
                names = "--proto_path",
                required = true,
                description =
                        "Specify the directory in which to search for imports. May be specified"
                            + " multiple times; directories will be searched in order. The file"
                            + " provided via `--proto_file` must also be in a directory provided"
                            + " via `--proto_path`.")
        private List<String> protoPaths = new ArrayList<>();

        @Parameter(
                names = "--fatal_warnings",
                description = "Treat warnings as errors. Defaults to `true`.",
                arity = 1)
        private boolean fatalWarnings = true;

        @Parameter(
                names = "--output_file",
                required = true,
                description = "Path to the output .java file.")
        private String outputFile;

        @Parameter(
                names = "--output_package",
                required = true,
                description = "Java package for the generated class.")
        private String outputPackage;

        @Parameter(
                names = "--output_class",
                required = true,
                description = "Name of the generated Java class.")
        private String outputClass;

        @Parameter(
                names = "--message_full_name",
                required = true,
                description = "Fully qualified name of the message.")
        private String messageFullName;
    }

    public static void main(String[] args) throws Exception {
        Args jcArgs = new Args();
        JCommander.newBuilder().addObject(jcArgs).build().parse(args);

        // When run in a genrule, we need to access `aprotoc` from `./tools/out/bin`. When run in a
        // test, we need to access `aprotoc` from `.` When run outside of a genrule, we need to
        // access `aprotoc` from path. Since there is no good way to detect whether we are running
        // as part of a genrule or a test, use existence of the `aprotoc` binary in
        // `./tools/out/bin` and `.` as an indicator.
        Path aprotocPath =
                Stream.of("./tools/out/bin/aprotoc", "./aprotoc")
                        .map(Path::of)
                        .filter(Files::exists)
                        .findFirst()
                        .orElseGet(() -> Path.of("aprotoc"));

        Path protoFile = Paths.get(jcArgs.protoFile);
        List<Path> protoPaths = jcArgs.protoPaths.stream().map(Paths::get).toList();
        Path outputFile = Paths.get(jcArgs.outputFile);
        String outputPackage = jcArgs.outputPackage;
        String outputClass = jcArgs.outputClass;
        String messageFullName = jcArgs.messageFullName;
        boolean fatalWarnings = jcArgs.fatalWarnings;

        List<String> command = new ArrayList<>();
        command.add(aprotocPath.toString());
        if (fatalWarnings) {
            command.add("--fatal_warnings");
        }
        command.add("--descriptor_set_out=/dev/stdout");
        command.add("--include_imports");
        protoPaths.stream().map(p -> "--proto_path=" + p).forEach(command::add);
        command.add(protoFile.toString());
        Process process = new ProcessBuilder(command).start();

        var stdoutFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try (InputStream stdout = process.getInputStream()) {
                                return stdout.readAllBytes();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        var stderrFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try (InputStream stderr = process.getErrorStream()) {
                                return stderr.readAllBytes();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        int exitCode = process.waitFor();
        byte[] stdout = stdoutFuture.join();
        byte[] stderr = stderrFuture.join();

        if (exitCode != 0) {
            String error = new String(stderr, UTF_8);
            throw new RuntimeException(
                    "protoc execution failed (exit code: " + exitCode + ").\n" + error);
        }

        // It's possible for protoc to succeed but not produce any output.
        if (stdout.length == 0) {
            String error = new String(stderr, UTF_8);
            throw new IOException("protoc execution succeeded but produced no output.\n" + error);
        }

        FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(stdout);
        var messageNames =
                descriptorSet.getFileList().stream()
                        .flatMap(
                                f ->
                                        f.getMessageTypeList().stream()
                                                .map(m -> f.getPackage() + "." + m.getName()))
                        .toList();
        if (!messageNames.stream().anyMatch(n -> n.equals(messageFullName))) {
            throw new RuntimeException(
                    "Message named '"
                            + messageFullName
                            + "' not found in descriptor set. Available message names: "
                            + String.join(", ", messageNames));
        }

        byte[] descriptorSetBytes = descriptorSet.toByteArray();

        var blocks = new ArrayList<CodeBlock>();
        for (byte b : descriptorSetBytes) {
            blocks.add(CodeBlock.of("$L", b));
        }
        CodeBlock descriptorSetInitializer =
                CodeBlock.builder()
                        .add("new byte[] { ")
                        .add(CodeBlock.join(blocks, ", "))
                        .add(" }")
                        .build();

        FieldSpec messageFullNameField =
                FieldSpec.builder(String.class, "sMessageFullName")
                        .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                        .initializer("$S", "." + messageFullName)
                        .build();

        FieldSpec descriptorSetField =
                FieldSpec.builder(byte[].class, "sFileDescriptorSet")
                        .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                        .initializer(descriptorSetInitializer)
                        .build();

        MethodSpec messageFullNameMethod =
                MethodSpec.methodBuilder("getMessageFullName")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                        .returns(String.class)
                        .addStatement("return sMessageFullName")
                        .build();

        MethodSpec descriptorSetMethod =
                MethodSpec.methodBuilder("getFileDescriptorSet")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                        .returns(byte[].class)
                        .addStatement("return sFileDescriptorSet")
                        .build();

        TypeSpec generatedClass =
                TypeSpec.classBuilder(outputClass)
                        .addJavadoc(
                                "Generated by {@link $T}. DO NOT EDIT.", DescriptorGenerator.class)
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addField(messageFullNameField)
                        .addField(descriptorSetField)
                        .addMethod(messageFullNameMethod)
                        .addMethod(descriptorSetMethod)
                        .build();

        JavaFile javaFile = JavaFile.builder(outputPackage, generatedClass).build();

        Files.createDirectories(outputFile.getParent());
        try (var writer = new OutputStreamWriter(Files.newOutputStream(outputFile), UTF_8)) {
            javaFile.writeTo(writer);
        }
    }
}
