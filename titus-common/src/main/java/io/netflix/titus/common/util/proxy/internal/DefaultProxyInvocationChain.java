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
import java.util.List;

import io.netflix.titus.common.util.proxy.ProxyInvocationChain;
import io.netflix.titus.common.util.proxy.ProxyInvocationHandler;

public class DefaultProxyInvocationChain<NATIVE> implements ProxyInvocationChain<NATIVE> {

    private static final ProxyInvocationHandler UNTERMINATED_HANDLER = (proxy, method, args, nativeHandler, chain) -> {
        throw new IllegalStateException("Method " + method.getName() + " invocation not handled");
    };

    private final ProxyInvocationHandler<NATIVE> handler;
    private final ProxyInvocationChain<NATIVE> remainingChain;

    public DefaultProxyInvocationChain(List<ProxyInvocationHandler<NATIVE>> handlers) {
        if (handlers.isEmpty()) {
            this.handler = UNTERMINATED_HANDLER;
            this.remainingChain = null;
        } else {
            this.handler = handlers.get(0);
            this.remainingChain = new DefaultProxyInvocationChain(handlers.subList(1, handlers.size()));
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args, NATIVE nativeHandler) throws Throwable {
        return handler.invoke(proxy, method, args, nativeHandler, remainingChain);
    }
}
