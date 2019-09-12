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

import io.grpc.MethodDescriptor;

class GrpcToReactorMethodBinding<REQ, RESP> {

    private final MethodDescriptor<REQ, RESP> methodDescriptor;
    private final Method reactorMethod;
    private final int callMetadataPos;
    private final int grpcArgumentPos;
    private final boolean isMono;
    private final Class returnTypeParameter;

    GrpcToReactorMethodBinding(MethodDescriptor<REQ, RESP> methodDescriptor,
                               Method reactorMethod,
                               int callMetadataPos,
                               boolean isMono,
                               Class returnTypeParameter) {
        this.methodDescriptor = methodDescriptor;
        this.reactorMethod = reactorMethod;
        this.callMetadataPos = callMetadataPos;
        this.grpcArgumentPos = callMetadataPos < 0
                ? (reactorMethod.getParameterCount() == 0 ? -1 : 0)
                : (reactorMethod.getParameterCount() == 1 ? -1 : (callMetadataPos == 0 ? 1 : 0));
        this.isMono = isMono;
        this.returnTypeParameter = returnTypeParameter;
    }

    MethodDescriptor<REQ, RESP> getMethodDescriptor() {
        return methodDescriptor;
    }

    Method getReactorMethod() {
        return reactorMethod;
    }

    int getCallMetadataPos() {
        return callMetadataPos;
    }

    int getGrpcArgumentPos() {
        return grpcArgumentPos;
    }

    boolean isMono() {
        return isMono;
    }

    Class getReturnTypeParameter() {
        return returnTypeParameter;
    }
}
