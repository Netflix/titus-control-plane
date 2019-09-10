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

package com.netflix.titus.runtime.connector.common.reactor;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.netflix.titus.common.util.CollectionsExt;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GrpcToReactUtil {

    /**
     * For request/response GRPC calls, we set execution deadline at both Reactor and GRPC level. As we prefer the timeout
     * be triggered by GRPC, which may give us potentially more insight, we adjust Reactor timeout value by this factor.
     */
    public static final double RX_CLIENT_TIMEOUT_FACTOR = 1.2;

    public static Method getGrpcMethod(AbstractStub<?> grpcStub, Method method, Set<Class> handlerTypes) {
        List<Class> transferredParameters = Arrays.stream((Class[]) method.getParameterTypes())
                .filter(type -> !handlerTypes.contains(type))
                .collect(Collectors.toList());

        Preconditions.checkArgument(transferredParameters.size() <= 1, "Method must have one or zero protobuf object parameters but is: %s", method);

        Class<?> requestType = CollectionsExt.firstOrDefault(transferredParameters, Empty.class);
        Preconditions.checkArgument(Message.class.isAssignableFrom(requestType), "Not protobuf message in method parameter: %s", method);

        Class<?> returnType = method.getReturnType();
        Preconditions.checkArgument(Flux.class.isAssignableFrom(returnType) || Mono.class.isAssignableFrom(returnType), "Flux or Mono return type expected but is: %s", returnType);

        try {
            return grpcStub.getClass().getMethod(method.getName(), requestType, StreamObserver.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("React method does not match any of the available GRPC stub methods: " + method);
        }
    }

    public static String toMethodNameFromFullName(String fullName) {
        int begin = fullName.indexOf('/');

        Preconditions.checkState(begin >= 0, "Not GRPC full name: " + fullName);
        Preconditions.checkState(begin + 1 < fullName.length(), "Not GRPC full name: " + fullName);

        return Character.toLowerCase(fullName.charAt(begin + 1)) + fullName.substring(begin + 2);
    }

    public static boolean isEmptyToVoidResult(Method reactMethod, Method grpcMethod) {
        Preconditions.checkArgument(reactMethod.getReturnType().isAssignableFrom(Mono.class) || reactMethod.getReturnType().isAssignableFrom(Flux.class));

        return hasTypeParameter(reactMethod.getGenericReturnType(), 0, Void.class)
                && hasTypeParameter(grpcMethod.getGenericParameterTypes()[1], 0, Empty.class);
    }

    private static boolean hasTypeParameter(Type type, int position, Class<?> parameterClass) {
        if (!(type instanceof ParameterizedType)) {
            return false;
        }

        ParameterizedType ptype = (ParameterizedType) type;
        Type[] typeArguments = ptype.getActualTypeArguments();
        if (position >= typeArguments.length) {
            return false;
        }
        Type parameterType = typeArguments[position];
        return parameterClass.isAssignableFrom((Class<?>) parameterType);
    }
}
