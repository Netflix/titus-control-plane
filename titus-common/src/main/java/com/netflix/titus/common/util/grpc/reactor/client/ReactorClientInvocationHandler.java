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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Function;

import org.reactivestreams.Publisher;

class ReactorClientInvocationHandler<REACT_API> implements InvocationHandler {

    private final Map<Method, Function<Object[], Publisher>> methodMap;
    private final String toStringValue;

    ReactorClientInvocationHandler(Class<REACT_API> reactApi, Map<Method, Function<Object[], Publisher>> methodMap) {
        this.methodMap = methodMap;
        this.toStringValue = reactApi.getSimpleName() + "[react to GRPC bridge]";
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Function<Object[], Publisher> methodHandler = methodMap.get(method);
        if (methodHandler != null) {
            return methodHandler.apply(args);
        }
        if (method.getName().equals("toString")) {
            return toStringValue;
        }
        return method.invoke(this, args);
    }
}
