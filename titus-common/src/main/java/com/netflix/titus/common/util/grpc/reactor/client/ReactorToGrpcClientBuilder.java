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

package com.netflix.titus.common.util.grpc.reactor.client;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.netflix.titus.common.util.grpc.GrpcToReactUtil;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.AbstractStub;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>, CONTEXT> {

    private static final long DEFAULT_REQUEST_TIMEOUT_MS = 60_000;

    @VisibleForTesting
    public static final long DEFAULT_STREAMING_TIMEOUT_MS = 12 * 60 * 60_000;

    private static final BiFunction EMPTY_STUB_DECORATOR = (stub, context) -> stub;

    private final Class<REACT_API> reactApi;
    private final GRPC_STUB grpcStub;
    private final ServiceDescriptor grpcServiceDescriptor;
    private final Class<CONTEXT> contextType;

    private final Set<Class> nonGrpcParameters;

    private Duration timeout;
    private Duration streamingTimeout;
    private BiFunction<GRPC_STUB, Optional<CONTEXT>, GRPC_STUB> grpcStubDecorator;

    private ReactorToGrpcClientBuilder(Class<REACT_API> reactApi,
                                       GRPC_STUB grpcStub,
                                       ServiceDescriptor grpcServiceDescriptor,
                                       Class<CONTEXT> contextType) {
        this.reactApi = reactApi;
        this.grpcStub = grpcStub;
        this.grpcServiceDescriptor = grpcServiceDescriptor;
        this.contextType = contextType;
        this.nonGrpcParameters = Collections.singleton(this.contextType);
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB, CONTEXT> withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB, CONTEXT> withStreamingTimeout(Duration streamingTimeout) {
        this.streamingTimeout = streamingTimeout;
        return this;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB, CONTEXT> withGrpcStubDecorator(BiFunction<GRPC_STUB, Optional<CONTEXT>, GRPC_STUB> grpcStubDecorator) {
        this.grpcStubDecorator = grpcStubDecorator;
        return this;
    }

    public static <REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>, CONTEXT> ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB, CONTEXT> newBuilder(
            Class<REACT_API> reactApi,
            GRPC_STUB grpcStub,
            ServiceDescriptor grpcServiceDescriptor,
            Class<CONTEXT> contextType) {
        Preconditions.checkArgument(reactApi.isInterface(), "Interface type required");
        return new ReactorToGrpcClientBuilder<>(reactApi, grpcStub, grpcServiceDescriptor, contextType);
    }

    public static <REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>, CONTEXT> ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB, CONTEXT> newBuilderWithDefaults(
            Class<REACT_API> reactApi,
            GRPC_STUB grpcStub,
            ServiceDescriptor grpcServiceDescriptor,
            Class<CONTEXT> contextType) {
        Preconditions.checkArgument(reactApi.isInterface(), "Interface type required");
        return new ReactorToGrpcClientBuilder<>(reactApi, grpcStub, grpcServiceDescriptor, contextType)
                .withTimeout(Duration.ofMillis(DEFAULT_REQUEST_TIMEOUT_MS))
                .withStreamingTimeout(Duration.ofMillis(DEFAULT_STREAMING_TIMEOUT_MS))
                .withGrpcStubDecorator(EMPTY_STUB_DECORATOR);
    }

    public REACT_API build() {
        Preconditions.checkNotNull(timeout, "GRPC request timeout not set");
        Preconditions.checkNotNull(streamingTimeout, "GRPC streaming request timeout not set");
        Preconditions.checkNotNull(grpcStubDecorator, "GRPC stub decorator not set");

        Map<Method, Function<Object[], Publisher>> methodMap = buildMethodMap();
        return (REACT_API) Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class[]{reactApi},
                new ReactorClientInvocationHandler(reactApi, methodMap)
        );
    }

    private Map<Method, Function<Object[], Publisher>> buildMethodMap() {
        Map<Method, Function<Object[], Publisher>> mapping = new HashMap<>();
        for (Method method : reactApi.getMethods()) {
            mapping.put(method, createGrpcBridge(method));
        }
        return mapping;
    }

    private Function<Object[], Publisher> createGrpcBridge(Method reactMethod) {
        int grpcArgPos = -1;
        int callMetadataPos = -1;
        Class<?>[] argTypes = reactMethod.getParameterTypes();
        for (int i = 0; i < argTypes.length; i++) {
            if (Message.class.isAssignableFrom(argTypes[i])) {
                grpcArgPos = i;
            }
            if (contextType.isAssignableFrom(argTypes[i])) {
                callMetadataPos = i;
            }
        }

        Method grpcMethod = GrpcToReactUtil.getGrpcMethod(grpcStub, reactMethod, nonGrpcParameters);

        if (reactMethod.getReturnType().isAssignableFrom(Mono.class)) {
            return new MonoMethodBridge<>(
                    reactMethod,
                    grpcMethod,
                    grpcArgPos,
                    callMetadataPos,
                    grpcStubDecorator,
                    grpcStub,
                    timeout
            );
        }

        return new FluxMethodBridge<>(
                reactMethod,
                grpcServiceDescriptor,
                grpcMethod,
                grpcArgPos,
                callMetadataPos,
                grpcStubDecorator,
                grpcStub,
                timeout,
                streamingTimeout
        );
    }
}
