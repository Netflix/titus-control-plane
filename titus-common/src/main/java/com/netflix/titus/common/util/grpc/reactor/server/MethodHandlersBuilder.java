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

package com.netflix.titus.common.util.grpc.reactor.server;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import com.netflix.titus.common.util.ReflectionExt;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.grpc.protobuf.ProtoMethodDescriptorSupplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.common.util.grpc.GrpcToReactUtil.toMethodNameFromFullName;

class MethodHandlersBuilder<CONTEXT> {

    private final Map<String, Method> reactorMethodMap;
    private final Class<CONTEXT> contextType;

    private final List<UnaryMethodHandler> unaryMethodHandlers;
    private final List<ServerStreamingMethodHandler> serverStreamingMethodHandlers;

    MethodHandlersBuilder(Object reactorService,
                          ServiceDescriptor serviceDefinition,
                          Class<CONTEXT> contextType,
                          Supplier<CONTEXT> contextResolver) {
        this.reactorMethodMap = Stream.of(reactorService.getClass().getMethods())
                .filter(m -> !ReflectionExt.isObjectMethod(m))
                .collect(Collectors.toMap(Method::getName, Function.identity()));
        this.contextType = contextType;

        List<UnaryMethodHandler> unaryMethodHandlers = new ArrayList<>();
        List<ServerStreamingMethodHandler> serverStreamingMethodHandlers = new ArrayList<>();
        serviceDefinition.getMethods().forEach(methodDescriptor -> {
            GrpcToReactorMethodBinding binding = findReactorMethod(methodDescriptor);
            if (binding.isMono()) {
                unaryMethodHandlers.add(new UnaryMethodHandler<>(binding, contextResolver, reactorService));
            } else {
                serverStreamingMethodHandlers.add(new ServerStreamingMethodHandler<>(binding, contextResolver, reactorService));
            }
        });

        this.unaryMethodHandlers = unaryMethodHandlers;
        this.serverStreamingMethodHandlers = serverStreamingMethodHandlers;
    }

    List<UnaryMethodHandler> getUnaryMethodHandlers() {
        return unaryMethodHandlers;
    }

    List<ServerStreamingMethodHandler> getServerStreamingMethodHandlers() {
        return serverStreamingMethodHandlers;
    }

    private GrpcToReactorMethodBinding findReactorMethod(MethodDescriptor<?, ?> methodDescriptor) {
        String methodName = toMethodNameFromFullName(methodDescriptor.getFullMethodName());
        Method reactorMethod = reactorMethodMap.get(methodName);
        Preconditions.checkNotNull(reactorMethod, "Cannot find corresponding Reactor method for: {}", methodDescriptor);

        ProtoMethodDescriptorSupplier methodDescriptorSupplier = (ProtoMethodDescriptorSupplier) methodDescriptor.getSchemaDescriptor();
        Descriptors.MethodDescriptor md = methodDescriptorSupplier.getMethodDescriptor();
        String inputTypeName = md.getInputType().getName();
        String outputTypeName = md.getOutputType().getName();

        Class<?> reactorReturnType = reactorMethod.getReturnType();
        boolean isMono = reactorReturnType.isAssignableFrom(Mono.class);
        boolean isFlux = !isMono && reactorReturnType.isAssignableFrom(Flux.class);
        Preconditions.checkArgument(isMono || isFlux, "Mono or Flux return types allowed only");
        Type[] returnTypeParameters = ((ParameterizedType) reactorMethod.getGenericReturnType()).getActualTypeArguments();
        Preconditions.checkArgument(
                returnTypeParameters != null && returnTypeParameters.length == 1,
                "Expected one type parameter in the return type: %s", methodDescriptor.getFullMethodName()
        );
        Class returnTypeParameter = (Class) returnTypeParameters[0];

        // Check return types
        if (returnTypeParameter == Void.class) {
            Preconditions.checkArgument(
                    outputTypeName.equals("Empty"),
                    "Reactor Mono<Void>/Flux<Void> can be mapped to GRPC/Empty only: %s", methodDescriptor.getFullMethodName()
            );
        } else {
            Preconditions.checkArgument(
                    returnTypeParameter.getSimpleName().equals(outputTypeName),
                    "Different GRPC and Reactor API return types: %s", methodDescriptor.getFullMethodName()
            );
        }

        // Check method arguments
        if (reactorMethod.getParameterCount() == 0) {
            Preconditions.checkArgument(
                    inputTypeName.equals(Empty.class.getSimpleName()),
                    "Only Empty request argument allowed for Reactor methods with no parameters: %s", methodDescriptor.getFullMethodName()
            );
            return new GrpcToReactorMethodBinding<>(methodDescriptor, reactorMethod, -1, isMono, returnTypeParameter);
        }
        if (reactorMethod.getParameterCount() == 1) {
            if (reactorMethod.getParameterTypes()[0] == contextType) {
                Preconditions.checkArgument(
                        inputTypeName.equals(Empty.class.getSimpleName()),
                        "Only Empty request argument allowed for Reactor methods with no parameters: %s", methodDescriptor.getFullMethodName()
                );
                return new GrpcToReactorMethodBinding<>(methodDescriptor, reactorMethod, 0, isMono, returnTypeParameter);
            }
            Preconditions.checkArgument(
                    inputTypeName.equals(reactorMethod.getParameterTypes()[0].getSimpleName()),
                    "Reactor and GRPC parameter types do not match: %s", methodDescriptor.getFullMethodName()
            );
            return new GrpcToReactorMethodBinding<>(methodDescriptor, reactorMethod, -1, isMono, returnTypeParameter);
        }
        if (reactorMethod.getParameterCount() == 2) {
            Preconditions.checkArgument(
                    reactorMethod.getParameterTypes()[0] == contextType || reactorMethod.getParameterTypes()[1] == contextType,
                    "Expected one GRPC method argument, and one CallMetadata value in Reactor method mapped to: %s", methodDescriptor.getFullMethodName()
            );

            int callMetadataPos = reactorMethod.getParameterTypes()[0] == contextType ? 0 : 1;
            int grpcArgumentPos = callMetadataPos == 0 ? 1 : 0;
            Preconditions.checkArgument(
                    inputTypeName.equals(reactorMethod.getParameterTypes()[grpcArgumentPos].getSimpleName()),
                    "Reactor and GRPC parameter types do not match: %s", methodDescriptor.getFullMethodName()
            );
            return new GrpcToReactorMethodBinding<>(methodDescriptor, reactorMethod, callMetadataPos, isMono, returnTypeParameter);
        }

        throw new IllegalArgumentException("Cannot map GRPC method to any reactor method: " + methodDescriptor.getFullMethodName());
    }
}
