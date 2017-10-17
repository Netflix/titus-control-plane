/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util.proxy.internal;

import java.lang.reflect.Method;
import java.util.function.Function;

import io.netflix.titus.common.util.proxy.ProxyInvocationChain;

/**
 * Given condition function, allows or dis-allows calls to a wrapped API.
 */
public class GuardingInvocationHandler<API, NATIVE> extends AbstractInvocationHandler<API, NATIVE> {

    private final Function<API, Boolean> predicate;
    private final String errorMessage;

    public GuardingInvocationHandler(Class<API> apiInterface, Function<API, Boolean> predicate) {
        super(apiInterface);
        this.predicate = predicate;
        this.errorMessage = apiInterface.getName() + " service call not allowed; service not in active state";
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args, NATIVE nativeHandler, ProxyInvocationChain chain) throws Throwable {
        if (!getIncludedMethods().contains(method)) {
            return chain.invoke(proxy, method, args, nativeHandler);
        }

        if (!predicate.apply((API) proxy)) {
            throw new IllegalStateException(errorMessage);
        }

        return chain.invoke(proxy, method, args, nativeHandler);
    }
}
