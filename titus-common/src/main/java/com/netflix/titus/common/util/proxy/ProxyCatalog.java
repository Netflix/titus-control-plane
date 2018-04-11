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

package com.netflix.titus.common.util.proxy;

import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.proxy.internal.GuardingInvocationHandler;
import com.netflix.titus.common.util.proxy.internal.InvocationHandlerBridge;
import com.netflix.titus.common.util.proxy.internal.SpectatorInvocationHandler;

/**
 * A collection of common interface decorators (logging, metrics collection).
 */
public final class ProxyCatalog {

    private ProxyCatalog() {
    }

    public static <API, INSTANCE extends API> LoggingProxyBuilder<API, INSTANCE> createLoggingProxy(
            Class<API> apiInterface, INSTANCE instance) {
        return new LoggingProxyBuilder<>(apiInterface, instance);
    }

    /**
     * A default proxy logger logs:
     * <ul>
     * <li>requests/successful replies at DEBUG level</li>
     * <li>exceptions (including error observable replies) at ERROR level</li>
     * </ul>
     */
    public static <API, INSTANCE extends API> API createDefaultLoggingProxy(Class<API> apiInterface, INSTANCE instance) {
        return new LoggingProxyBuilder<>(apiInterface, instance).build();
    }

    public static <API, INSTANCE extends API> API createSpectatorProxy(Class<API> apiInterface, INSTANCE instance, TitusRuntime titusRuntime,
                                                                       boolean followObservableResults) {
        return (API) Proxy.newProxyInstance(
                apiInterface.getClassLoader(),
                new Class<?>[]{apiInterface},
                new InvocationHandlerBridge<>(new SpectatorInvocationHandler<>(apiInterface, titusRuntime, followObservableResults), instance)
        );
    }

    public static <API, INSTANCE extends API> API createSpectatorProxy(Class<API> apiInterface, INSTANCE instance, TitusRuntime titusRuntime) {
        return createSpectatorProxy(apiInterface, instance, titusRuntime, false);
    }

    public static <API, INSTANCE extends API> API createGuardingProxy(Class<API> apiInterface, INSTANCE instance, Supplier<Boolean> predicate) {
        return (API) Proxy.newProxyInstance(
                apiInterface.getClassLoader(),
                new Class<?>[]{apiInterface},
                new InvocationHandlerBridge<>(new GuardingInvocationHandler<>(apiInterface, myInstance -> predicate.get()), instance)
        );
    }
}
