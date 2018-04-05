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

package com.netflix.titus.common.framework.fit.internal.action;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.titus.common.framework.fit.AbstractFitAction;
import com.netflix.titus.common.framework.fit.FitActionDescriptor;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.FitUtil;
import com.netflix.titus.common.util.CollectionsExt;
import rx.Observable;

public class FitErrorAction extends AbstractFitAction {

    public static final FitActionDescriptor DESCRIPTOR = new FitActionDescriptor(
            "exception",
            "Throw an exception",
            CollectionsExt.copyAndAdd(
                    FitUtil.PERIOD_ERROR_PROPERTIES,
                    "before", "Throw exception before running the downstream action (defaults to 'true')"
            )
    );

    private final Constructor<? extends Throwable> exceptionConstructor;
    private final Supplier<Boolean> shouldFailFunction;

    public FitErrorAction(String id, Map<String, String> properties, FitInjection injection) {
        super(id, DESCRIPTOR, properties, injection);

        this.shouldFailFunction = FitUtil.periodicErrors(properties);

        try {
            this.exceptionConstructor = getInjection().getExceptionType().getConstructor(String.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Invalid exception type: " + getInjection().getExceptionType());
        }
    }

    @Override
    public void beforeImmediate(String injectionPoint) {
        if (runBefore) {
            doFail(injectionPoint);
        }
    }

    @Override
    public void afterImmediate(String injectionPoint) {
        if (!runBefore) {
            doFail(injectionPoint);
        }
    }

    @Override
    public <T> Supplier<Observable<T>> aroundObservable(String injectionPoint, Supplier<Observable<T>> source) {
        if (runBefore) {
            return () -> Observable.defer(() -> {
                doFail(injectionPoint);
                return source.get();
            });
        }
        return () -> source.get().concatWith(Observable.defer(() -> {
            doFail(injectionPoint);
            return Observable.empty();
        }));
    }

    @Override
    public <T> Supplier<CompletableFuture<T>> aroundCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        if (runBefore) {
            return () -> {
                if (shouldFailFunction.get()) {
                    CompletableFuture<T> future = new CompletableFuture<>();
                    future.completeExceptionally(newException(injectionPoint));
                    return future;
                }
                return source.get();
            };
        }
        return () -> source.get().thenApply(result -> doFail(injectionPoint) ? null : result);
    }

    @Override
    public <T> Supplier<ListenableFuture<T>> aroundListenableFuture(String injectionPoint, Supplier<ListenableFuture<T>> source) {
        if (runBefore) {
            return () -> {
                if (shouldFailFunction.get()) {
                    return Futures.immediateFailedFuture(newException(injectionPoint));
                }
                return source.get();
            };
        }
        return () -> Futures.transform(source.get(), (Function<T, T>) input -> doFail(injectionPoint) ? null : input);
    }

    private boolean doFail(String injectionPoint) {
        if (shouldFailFunction.get()) {
            Throwable exception = newException(injectionPoint);
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            }
            throw new RuntimeException("Wrapper", exception);
        }
        return false;
    }

    private Throwable newException(String injectionPoint) {
        try {
            return exceptionConstructor.newInstance("FIT exception at " + injectionPoint);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create FIT exception from type " + getInjection().getExceptionType(), e);
        }
    }
}
