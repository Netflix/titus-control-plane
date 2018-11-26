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

package com.netflix.titus.runtime.connector.common.react;

import java.lang.reflect.Method;

import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class GrpcToReactUtil {

    static Method getGrpcMethod(AbstractStub<?> grpcStub, Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Preconditions.checkArgument(parameterTypes.length <= 1, "Expected method with none or one protobuf object parameter but is: %s", method);

        Class<?> requestType = parameterTypes.length == 0 ? Empty.class : parameterTypes[0];
        Preconditions.checkArgument(Message.class.isAssignableFrom(requestType), "Not protobuf message in method parameter: %s", requestType);

        Class<?> returnType = method.getReturnType();
        Preconditions.checkArgument(Flux.class.isAssignableFrom(returnType) || Mono.class.isAssignableFrom(returnType), "Flux or Mono return type expected but is: %s", returnType);

        try {
            return grpcStub.getClass().getMethod(method.getName(), new Class[]{requestType, StreamObserver.class});
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("React method does not match any of the available GRPC stub methods: " + method);
        }
    }

    static String toMethodNameFromFullName(String fullName) {
        int begin = fullName.indexOf('/');

        Preconditions.checkState(begin >= 0, "Not GRPC full name: " + fullName);
        Preconditions.checkState(begin + 1 < fullName.length(), "Not GRPC full name: " + fullName);

        return Character.toLowerCase(fullName.charAt(begin + 1)) + fullName.substring(begin + 2);
    }
}
