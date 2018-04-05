/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.testkit.grpc;

import java.util.Optional;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.google.rpc.Status;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Titus GRPC error replies are based on Google RPC model: https://github.com/googleapis/googleapis/tree/master/google/rpc.
 * Additional error context data is encoded in HTTP2 headers. A client is not required to understand and process the error
 * context, but it will provide more insight into the cause of the error.
 */
public class GrpcClientErrorUtils {

    private static final Logger logger = LoggerFactory.getLogger(GrpcClientErrorUtils.class);

    public static final String X_TITUS_ERROR = "X-Titus-Error";
    public static final String X_TITUS_ERROR_BIN = "X-Titus-Error-bin";

    public static final Metadata.Key<String> KEY_TITUS_ERROR_REPORT = Metadata.Key.of(X_TITUS_ERROR, Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> KEY_TITUS_ERROR_REPORT_BIN = Metadata.Key.of(X_TITUS_ERROR_BIN, Metadata.BINARY_BYTE_MARSHALLER);

    public static Status getStatus(StatusRuntimeException error) {
        if (error.getTrailers() == null) {
            return Status.newBuilder().setCode(-1).setMessage(error.getMessage()).build();
        }
        try {
            byte[] data = error.getTrailers().get(KEY_TITUS_ERROR_REPORT_BIN);
            if (data == null) {
                return Status.newBuilder().setCode(-1).setMessage(error.getMessage()).build();
            }
            return Status.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Something went wrong with status parsing", e);
            throw new IllegalArgumentException(e);
        }
    }

    public static <D extends MessageOrBuilder> Optional<D> getDetail(StatusRuntimeException error, Class<D> detailType) {
        Status status = getStatus(error);
        for (Any any : status.getDetailsList()) {
            Descriptors.Descriptor descriptor = any.getDescriptorForType();
            Descriptors.FieldDescriptor typeUrlField = descriptor.findFieldByName("type_url");
            String typeUrl = (String) any.getField(typeUrlField);

            Class type;
            if (typeUrl.contains(DebugInfo.class.getSimpleName())) {
                type = DebugInfo.class;
            } else if (typeUrl.contains(BadRequest.class.getSimpleName())) {
                type = BadRequest.class;
            } else {
                return Optional.empty();
            }
            if (type == detailType) {
                Message unpack;
                try {
                    unpack = any.unpack(type);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException("Cannot unpack error details", e);
                }
                return Optional.of((D) unpack);
            }
        }
        return Optional.empty();
    }

    public static void printDetails(StatusRuntimeException error) {
        System.out.println("Error context:");

        if (error.getTrailers() == null) {
            System.out.println("no trailers");
            return;
        }

        // ASCII encoded error message
        String errorMessage = error.getTrailers().get(KEY_TITUS_ERROR_REPORT);
        if (errorMessage == null) {
            System.out.println("no context data available");
            return;
        }
        System.out.printf("%s=%s\n", X_TITUS_ERROR, errorMessage);

        // GRPC RPC 'Status'
        System.out.println(X_TITUS_ERROR_BIN + ':');
        getStatus(error).getDetailsList().forEach(GrpcClientErrorUtils::print);
    }

    private static void print(Any any) {
        Descriptors.Descriptor descriptor = any.getDescriptorForType();
        Descriptors.FieldDescriptor typeUrlField = descriptor.findFieldByName("type_url");
        String typeUrl = (String) any.getField(typeUrlField);

        Class type;
        if (typeUrl.contains(DebugInfo.class.getSimpleName())) {
            type = DebugInfo.class;
        } else if (typeUrl.contains(BadRequest.class.getSimpleName())) {
            type = BadRequest.class;
        } else {
            System.out.println("Unknown detail type " + typeUrl);
            return;
        }
        try {
            System.out.printf("Detail %s:\n", type);
            System.out.println(any.unpack(type));
        } catch (InvalidProtocolBufferException e) {
            System.out.println("Something went wrong with detail parsing");
            e.printStackTrace();
        }
    }
}
