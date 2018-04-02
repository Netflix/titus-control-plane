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

package com.netflix.titus.common.framework.fit.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.util.ReflectionExt;
import rx.Observable;

public class FitInvocationHandler implements InvocationHandler {

    private final Object delegate;
    private final Map<Method, Function<Object[], Object>> methodHandlers;

    public FitInvocationHandler(Object delegate, Map<Method, Function<Object[], Object>> methodHandlers) {
        this.delegate = delegate;
        this.methodHandlers = methodHandlers;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Function<Object[], Object> handler = methodHandlers.get(method);
        if (handler == null) {
            return method.invoke(delegate, args);
        }
        return handler.apply(args);
    }

    private static Object handleSynchronous(FitInjection injection, Method method, Object target, Object[] arguments) {
        injection.beforeImmediate(method.getName());

        Object result;
        try {
            result = method.invoke(target, arguments);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Reflective invocation of method not allowed: " + method.getName(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Unexpected method invocation error: " + method.getName(), e);
        }

        injection.afterImmediate(method.getName());

        return result;
    }

    private static CompletableFuture<?> handleCompletableFuture(FitInjection injection, Method method, Object target, Object[] arguments) {
        return injection.aroundCompletableFuture(method.getName(), () -> {
            try {
                return (CompletableFuture<?>) method.invoke(target, arguments);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Reflective invocation of method not allowed: " + method.getName(), e);
            } catch (InvocationTargetException e) {
                throw new IllegalStateException("Unexpected method invocation error: " + method.getName(), e);
            }
        });
    }

    private static ListenableFuture<?> handleListenableFuture(FitInjection injection, Method method, Object target, Object[] arguments) {
        return injection.aroundListenableFuture(method.getName(), () -> {
            try {
                return (ListenableFuture<?>) method.invoke(target, arguments);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Reflective invocation of method not allowed: " + method.getName(), e);
            } catch (InvocationTargetException e) {
                throw new IllegalStateException("Unexpected method invocation error: " + method.getName(), e);
            }
        });
    }

    private static Observable<?> handleObservable(FitInjection injection, Method method, Object target, Object[] arguments) {
        return injection.aroundObservable(method.getName(), () -> {
            try {
                return (Observable<?>) method.invoke(target, arguments);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Reflective invocation of method not allowed: " + method.getName(), e);
            } catch (InvocationTargetException e) {
                throw new IllegalStateException("Unexpected method invocation error: " + method.getName(), e);
            }
        });
    }

    public static <I> I newProxy(Object delegate, FitInjection injection) {
        List<Class<?>> interfaces = ReflectionExt.findAllInterfaces(delegate.getClass());
        Preconditions.checkArgument(
                interfaces.size() == 1,
                "%s expected to implement exactly one interface", delegate.getClass().getName()
        );
        Class<?> interf = interfaces.get(0);

        Map<Method, Function<Object[], Object>> handlers = new HashMap<>();
        for (Method method : interf.getMethods()) {
            Class<?> returnType = method.getReturnType();
            if (returnType.isAssignableFrom(CompletableFuture.class)) {
                handlers.put(method, args -> handleCompletableFuture(injection, method, delegate, args));
            } else if (returnType.isAssignableFrom(ListenableFuture.class)) {
                handlers.put(method, args -> handleListenableFuture(injection, method, delegate, args));
            } else if (returnType.isAssignableFrom(Observable.class)) {
                handlers.put(method, args -> handleObservable(injection, method, delegate, args));
            } else {
                handlers.put(method, args -> handleSynchronous(injection, method, delegate, args));
            }
        }
        return (I) Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class<?>[]{interf}, new FitInvocationHandler(delegate, handlers)
        );
    }
}
