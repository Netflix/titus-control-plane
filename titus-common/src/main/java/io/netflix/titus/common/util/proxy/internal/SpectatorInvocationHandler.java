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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Registry;
import rx.Observable;

/**
 * Method invocation metrics collector.
 */
public class SpectatorInvocationHandler<API, NATIVE> extends InterceptingInvocationHandler<API, NATIVE, Long> {

    private final Class<API> apiInterface;
    private final Registry registry;
    private final String invocationCounterMetricName;
    private final String invocationTimeMetricName;
    private final String resultSubscriptionCountMetricName;
    private final String resultSubscriptionEmitMetricName;
    private final String resultSubscriptionTimeMetricName;

    public SpectatorInvocationHandler(Class<API> apiInterface, Registry registry, boolean followObservableResults) {
        super(apiInterface, followObservableResults);
        this.apiInterface = apiInterface;
        this.registry = registry;

        this.invocationCounterMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".invocations";
        this.invocationTimeMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".executionTime";
        this.resultSubscriptionCountMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".subscriptions";
        this.resultSubscriptionEmitMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".subscriptionEmits";
        this.resultSubscriptionTimeMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".subscriptionTime";
    }

    @Override
    protected Long before(Method method, Object[] args) {
        return System.currentTimeMillis();
    }

    @Override
    protected void after(Method method, Object result, Long startTime) {
        registry.counter(
                invocationCounterMetricName,
                "class", apiInterface.getName(),
                "method", method.getName(),
                "status", "success"
        ).increment();
        reportExecutionTime(method, startTime);
    }

    @Override
    protected void afterException(Method method, Throwable error, Long startTime) {
        registry.counter(
                invocationCounterMetricName,
                "class", apiInterface.getName(),
                "method", method.getName(),
                "status", "error",
                "exception", getExceptionName(error)
        ).increment();
        reportExecutionTime(method, startTime);
    }

    private String getExceptionName(Throwable error) {
        return error instanceof InvocationHandler
                ? error.getCause().getClass().getName()
                : error.getClass().getName();
    }

    @Override
    protected Observable<Object> afterObservable(Method method, Observable<Object> result, Long startTime) {
        long methodExitTime = System.currentTimeMillis();

        return Observable.create(subscriber -> {
            long start = System.currentTimeMillis();

            registry.counter(
                    resultSubscriptionCountMetricName,
                    "class", apiInterface.getName(),
                    "method", method.getName(),
                    "stage", "subscribed"
            ).increment();
            reportSubscriptionExecutionTime(method, methodExitTime, start, "subscribed");

            result.doOnUnsubscribe(() -> {
                registry.counter(
                        resultSubscriptionCountMetricName,
                        "class", apiInterface.getName(),
                        "method", method.getName(),
                        "stage", "unsubscribed"
                ).increment();
            }).subscribe(
                    next -> {
                        registry.counter(
                                resultSubscriptionEmitMetricName,
                                "class", apiInterface.getName(),
                                "method", method.getName()
                        ).increment();
                        subscriber.onNext(next);
                    },
                    error -> {
                        registry.counter(
                                resultSubscriptionCountMetricName,
                                "class", apiInterface.getName(),
                                "method", method.getName(),
                                "stage", "onError",
                                "exception", getExceptionName(error)
                        ).increment();
                        reportSubscriptionExecutionTime(method, start, System.currentTimeMillis(), "error");

                        subscriber.onError(error);
                    },
                    () -> {
                        registry.counter(
                                resultSubscriptionCountMetricName,
                                "class", apiInterface.getName(),
                                "method", method.getName(),
                                "stage", "onCompleted"
                        ).increment();
                        reportSubscriptionExecutionTime(method, start, System.currentTimeMillis(), "success");

                        subscriber.onCompleted();
                    }
            );
        });
    }

    private void reportExecutionTime(Method method, Long startTime) {
        registry.timer(
                invocationTimeMetricName,
                "class", apiInterface.getName(),
                "method", method.getName()
        ).record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    private void reportSubscriptionExecutionTime(Method method, Long startTime, long endTime, String stage) {
        registry.timer(
                resultSubscriptionTimeMetricName,
                "class", apiInterface.getName(),
                "method", method.getName(),
                "stage", stage
        ).record(endTime - startTime, TimeUnit.MILLISECONDS);
    }
}
