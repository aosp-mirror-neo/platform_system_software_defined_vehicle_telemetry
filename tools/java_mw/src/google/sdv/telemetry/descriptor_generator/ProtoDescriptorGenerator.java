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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import javax.lang.model.element.Modifier;

/**
 * A host-side tool to generate a Java file containing a serialized protobuf FileDescriptorProto.
 */
public final class ProtoDescriptorGenerator {
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static class Args {
        @Parameter(
                names = "--proto_file",
                required = true,
                description = "Path to the input .proto file.")
        private String protoFile;

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
                names = "--root_message_name",
                required = true,
                description = "Fully qualified name of the root message.")
        private String rootMessageName;
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
        Path outputFile = Paths.get(jcArgs.outputFile);
        String outputPackage = jcArgs.outputPackage;
        String outputClass = jcArgs.outputClass;
        String rootMessageName = jcArgs.rootMessageName;

        Process process =
                new ProcessBuilder(
                                aprotocPath.toString(),
                                "--descriptor_set_out=/dev/stdout",
                                "--proto_path=" + protoFile.getParent(),
                                protoFile.toString())
                        .start();

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

        FieldSpec rootMessageField =
                FieldSpec.builder(String.class, "ROOT_MESSAGE_NAME")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                        .initializer("$S", "." + rootMessageName)
                        .build();

        FieldSpec descriptorSetField =
                FieldSpec.builder(byte[].class, "FILE_DESCRIPTOR_SET")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                        .initializer(descriptorSetInitializer)
                        .build();

        TypeSpec generatedClass =
                TypeSpec.classBuilder(outputClass)
                        .addJavadoc(
                                "Generated by {@link $T}. DO NOT EDIT.",
                                ProtoDescriptorGenerator.class)
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        .addField(rootMessageField)
                        .addField(descriptorSetField)
                        .build();

        JavaFile javaFile = JavaFile.builder(outputPackage, generatedClass).build();

        Files.createDirectories(outputFile.getParent());
        try (var writer = new OutputStreamWriter(Files.newOutputStream(outputFile), UTF_8)) {
            javaFile.writeTo(writer);
        }
    }
}
