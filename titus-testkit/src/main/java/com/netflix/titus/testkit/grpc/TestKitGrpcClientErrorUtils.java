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

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.netflix.titus.runtime.common.grpc.GrpcClientErrorUtils;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

import static com.netflix.titus.runtime.common.grpc.GrpcClientErrorUtils.KEY_TITUS_ERROR_REPORT;
import static com.netflix.titus.runtime.common.grpc.GrpcClientErrorUtils.X_TITUS_ERROR;
import static com.netflix.titus.runtime.common.grpc.GrpcClientErrorUtils.X_TITUS_ERROR_BIN;

/**
 * Titus GRPC error replies are based on Google RPC model: https://github.com/googleapis/googleapis/tree/master/google/rpc.
 * Additional error context data is encoded in HTTP2 headers. A client is not required to understand and process the error
 * context, but it will provide more insight into the cause of the error.
 */
public class TestKitGrpcClientErrorUtils {

    public static <STUB extends AbstractStub<STUB>> STUB attachCallHeaders(STUB client) {
        Metadata metadata = new Metadata();
        metadata.put(V3HeaderInterceptor.CALLER_ID_KEY, "testkitClient");
        metadata.put(V3HeaderInterceptor.CALL_REASON_KEY, "test call");
        metadata.put(V3HeaderInterceptor.DEBUG_KEY, "true");
        return client.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
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
        GrpcClientErrorUtils.getStatus(error).getDetailsList().forEach(TestKitGrpcClientErrorUtils::print);
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
