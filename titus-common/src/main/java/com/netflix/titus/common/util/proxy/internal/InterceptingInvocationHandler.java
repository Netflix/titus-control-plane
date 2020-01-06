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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import com.netflix.titus.common.util.ReflectionExt;
import com.netflix.titus.common.util.proxy.ProxyInvocationChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

/**
 */
abstract class InterceptingInvocationHandler<API, NATIVE, CONTEXT> extends AbstractInvocationHandler<API, NATIVE> {

    private static final Logger logger = LoggerFactory.getLogger(InterceptingInvocationHandler.class);

    private final Set<Method> observableResultFollowers;
    private final Set<Method> fluxResultFollowers;
    private final Set<Method> completableResultFollowers;
    private final Set<Method> monoResultFollowers;

    InterceptingInvocationHandler(Class<API> apiInterface, boolean followObservableResults) {
        super(apiInterface);

        ProxyMethodSegregator<API> methodSegregator = new ProxyMethodSegregator<>(apiInterface, followObservableResults, getIncludedMethods());
        this.observableResultFollowers = methodSegregator.getFollowedObservables();
        this.fluxResultFollowers = methodSegregator.getFollowedFlux();
        this.completableResultFollowers = methodSegregator.getFollowedCompletables();
        this.monoResultFollowers = methodSegregator.getFollowedMonos();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args, NATIVE nativeHandler, ProxyInvocationChain chain) throws Throwable {
        Method effectiveMethod = ReflectionExt.findInterfaceMethod(method).orElse(method);
        if (!getIncludedMethods().contains(effectiveMethod)) {
            return chain.invoke(proxy, method, args, nativeHandler);
        }

        // Before
        CONTEXT context = null;
        try {
            context = before(method, args);
        } catch (Throwable e) {
            logger.warn("Interceptor {}#before hook execution error ({}={})", getClass().getName(), e.getClass().getName(), e.getMessage());
        }

        // Actual
        Object result;
        try {
            result = chain.invoke(proxy, method, args, nativeHandler);
        } catch (InvocationTargetException e) {
            try {
                afterException(method, e, context);
            } catch (Throwable ie) {
                logger.warn("Interceptor {}#afterException hook execution error ({}={})", getClass().getName(), ie.getClass().getName(), ie.getMessage());
            }
            throw e.getCause();
        }

        // After
        try {
            after(method, result, context);
        } catch (Throwable e) {
            logger.warn("Interceptor {}#after hook execution error ({}={})", getClass().getName(), e.getClass().getName(), e.getMessage());
        }
        if (observableResultFollowers.contains(method)) {
            result = afterObservable(method, (Observable<Object>) result, context);
        } else if (fluxResultFollowers.contains(method)) {
            result = afterFlux(method, (Flux<Object>) result, context);
        } else if (completableResultFollowers.contains(method)) {
            result = afterCompletable(method, (Completable) result, context);
        } else if (monoResultFollowers.contains(method)) {
            result = afterMono(method, (Mono<Object>) result, context);
        }

        return result;
    }

    protected abstract CONTEXT before(Method method, Object[] args);

    protected abstract void after(Method method, Object result, CONTEXT context);

    protected abstract void afterException(Method method, Throwable cause, CONTEXT context);

    protected abstract Observable<Object> afterObservable(Method method, Observable<Object> result, CONTEXT context);

    protected abstract Flux<Object> afterFlux(Method method, Flux<Object> result, CONTEXT context);

    protected abstract Completable afterCompletable(Method method, Completable result, CONTEXT context);

    protected abstract Mono<Object> afterMono(Method method, Mono<Object> result, CONTEXT context);
}
