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

package com.netflix.titus.common.util.proxy.internal;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.netflix.titus.common.util.proxy.ProxyInvocationHandler;

abstract class AbstractInvocationHandler<API, NATIVE> implements ProxyInvocationHandler<NATIVE> {

    private static final Set<Method> EXCLUDED_METHODS = buildExcludedMethodsSet();

    private final Set<Method> includedMethodSet;
    private final Map<Method, String> methodSignatures;

    AbstractInvocationHandler(Class<API> apiInterface) {
        this.includedMethodSet = buildIncludedMethodsSet(apiInterface);
        this.methodSignatures = buildMethodSignatures(apiInterface, includedMethodSet);
    }

    Set<Method> getIncludedMethods() {
        return includedMethodSet;
    }

    String getMethodSignature(Method method) {
        return methodSignatures.get(method);
    }

    private Set<Method> buildIncludedMethodsSet(Class<API> apiInterface) {
        Set<Method> result = new HashSet<>();
        for (Method method : apiInterface.getMethods()) {
            if (!EXCLUDED_METHODS.contains(method)) {
                result.add(method);
            }
        }
        return result;
    }

    private Map<Method, String> buildMethodSignatures(Class<API> apiInterface, Set<Method> methods) {
        Map<Method, String> result = new HashMap<>();
        methods.forEach(method -> result.put(
                method,
                apiInterface.getSimpleName() + '.' + method.getName()
        ));
        return result;
    }

    private static Set<Method> buildExcludedMethodsSet() {
        Set<Method> result = new HashSet<>();
        Collections.addAll(result, Object.class.getMethods());
        return result;
    }
}
