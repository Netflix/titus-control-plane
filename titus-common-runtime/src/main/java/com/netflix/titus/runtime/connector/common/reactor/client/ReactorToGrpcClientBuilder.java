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

package com.netflix.titus.runtime.connector.common.reactor.client;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactUtil;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.AbstractStub;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>> {

    private static final Set<Class> NON_GRPC_PARAMETERS = Collections.singleton(CallMetadata.class);

    private final Class<REACT_API> reactApi;
    private final GRPC_STUB grpcStub;
    private final ServiceDescriptor grpcServiceDescriptor;

    private Duration timeout;
    private Duration streamingTimeout;
    private CallMetadataResolver callMetadataResolver;

    private ReactorToGrpcClientBuilder(Class<REACT_API> reactApi, GRPC_STUB grpcStub, ServiceDescriptor grpcServiceDescriptor) {
        this.reactApi = reactApi;
        this.grpcStub = grpcStub;
        this.grpcServiceDescriptor = grpcServiceDescriptor;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> withStreamingTimeout(Duration streamingTimeout) {
        this.streamingTimeout = streamingTimeout;
        return this;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> withCallMetadataResolver(CallMetadataResolver callMetadataResolver) {
        this.callMetadataResolver = callMetadataResolver;
        return this;
    }

    public static <REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>> ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> newBuilder(
            Class<REACT_API> reactApi,
            GRPC_STUB grpcStub,
            ServiceDescriptor grpcServiceDescriptor) {
        Preconditions.checkArgument(reactApi.isInterface(), "Interface type required");
        return new ReactorToGrpcClientBuilder<>(reactApi, grpcStub, grpcServiceDescriptor);
    }

    public static <REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>> ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> newBuilderWithDefaults(
            Class<REACT_API> reactApi,
            GRPC_STUB grpcStub,
            ServiceDescriptor grpcServiceDescriptor) {
        Preconditions.checkArgument(reactApi.isInterface(), "Interface type required");
        return new ReactorToGrpcClientBuilder<>(reactApi, grpcStub, grpcServiceDescriptor)
                .withTimeout(Duration.ofMillis(GrpcRequestConfiguration.DEFAULT_REQUEST_TIMEOUT_MS))
                .withStreamingTimeout(Duration.ofMillis(GrpcRequestConfiguration.DEFAULT_STREAMING_TIMEOUT_MS))
                .withCallMetadataResolver(AnonymousCallMetadataResolver.getInstance());
    }

    public REACT_API build() {
        Preconditions.checkNotNull(timeout, "GRPC request timeout not set");
        Preconditions.checkNotNull(streamingTimeout, "GRPC streaming request timeout not set");

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
            if (CallMetadata.class.isAssignableFrom(argTypes[i])) {
                callMetadataPos = i;
            }
        }

        Method grpcMethod = GrpcToReactUtil.getGrpcMethod(grpcStub, reactMethod, NON_GRPC_PARAMETERS);

        if (reactMethod.getReturnType().isAssignableFrom(Mono.class)) {
            return new MonoMethodBridge<>(
                    reactMethod,
                    grpcMethod,
                    grpcArgPos,
                    callMetadataPos,
                    callMetadataResolver,
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
                callMetadataResolver,
                grpcStub,
                timeout,
                streamingTimeout
        );
    }
}
