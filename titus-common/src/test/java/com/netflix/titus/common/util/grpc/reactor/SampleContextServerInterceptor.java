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

package com.netflix.titus.common.util.grpc.reactor;

import java.util.Optional;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

public class SampleContextServerInterceptor implements ServerInterceptor {

    private static String CONTEXT_HEADER = "X-Titus-SimpleContext";
    private static Metadata.Key<String> CONTEXT_KEY = Metadata.Key.of(CONTEXT_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    private static Context.Key<SampleContext> CALLER_ID_CONTEXT_KEY = Context.key(CONTEXT_HEADER);

    public static final SampleContext CONTEXT_UNDEFINED = new SampleContext("undefined");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String value = headers.get(CONTEXT_KEY);
        SampleContext context;
        if (value != null) {
            context = new SampleContext(value);
        } else {
            context = CONTEXT_UNDEFINED;
        }
        return Contexts.interceptCall(Context.current().withValue(CALLER_ID_CONTEXT_KEY, context), call, headers, next);
    }

    public static SampleContext serverResolve() {
        if (Context.current() == Context.ROOT) {
            return CONTEXT_UNDEFINED;
        }

        SampleContext context = CALLER_ID_CONTEXT_KEY.get();
        return context == null ? CONTEXT_UNDEFINED : context;
    }

    public static <GRPC_STUB extends AbstractStub<GRPC_STUB>> GRPC_STUB attachClientContext(GRPC_STUB stub, Optional<SampleContext> contextOpt) {
        Metadata metadata = new Metadata();
        metadata.put(CONTEXT_KEY, contextOpt.orElse(CONTEXT_UNDEFINED).getValue());
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }
}
