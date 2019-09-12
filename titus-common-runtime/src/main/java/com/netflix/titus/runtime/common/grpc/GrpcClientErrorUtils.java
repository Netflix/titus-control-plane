/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.common.grpc;

import java.util.Optional;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.google.rpc.Status;
import com.netflix.titus.common.util.ExceptionExt;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Titus GRPC error replies are based on Google RPC model: https://github.com/googleapis/googleapis/tree/master/google/rpc.
 * Additional error context data is encoded in HTTP2 headers. A client is not required to understand and process the error
 * context, but it will provide more insight into the cause of the error.
 */
public final class GrpcClientErrorUtils {

    private static final Logger logger = LoggerFactory.getLogger(GrpcClientErrorUtils.class);

    public static final String X_TITUS_ERROR = "X-Titus-Error";
    public static final String X_TITUS_ERROR_BIN = "X-Titus-Error-bin";

    public static final Metadata.Key<String> KEY_TITUS_ERROR_REPORT = Metadata.Key.of(X_TITUS_ERROR, Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> KEY_TITUS_ERROR_REPORT_BIN = Metadata.Key.of(X_TITUS_ERROR_BIN, Metadata.BINARY_BYTE_MARSHALLER);

    private GrpcClientErrorUtils() {
    }

    public static String toDetailedMessage(Throwable error) {
        if (error == null) {
            return "<empty throwable in method invocation>";
        }
        Throwable next = error;
        while (!(next instanceof StatusRuntimeException) && next.getCause() != null) {
            next = next.getCause();
        }

        // If this is not GRPC error, return full message chain.
        if (!(next instanceof StatusRuntimeException)) {
            return ExceptionExt.toMessageChain(error);
        }

        return next.getMessage();
    }

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
}
