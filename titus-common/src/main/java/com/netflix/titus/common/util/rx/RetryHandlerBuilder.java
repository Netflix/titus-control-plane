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

package com.netflix.titus.common.util.rx;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.ExceptionExt;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

/**
 * Rx retry functions (see {@link Observable#retry(Func2)}).
 */
public final class RetryHandlerBuilder {

    private static final Logger logger = LoggerFactory.getLogger(RetryHandlerBuilder.class);

    private Scheduler scheduler = Schedulers.computation();
    private reactor.core.scheduler.Scheduler reactorScheduler;

    private int retryCount = -1;
    private long retryDelayMs = -1;
    private long maxDelay = Long.MAX_VALUE;
    private String title = "observable";

    private Action1<Throwable> onErrorHook = e -> {
    };
    private Supplier<Boolean> retryWhenCondition;
    private Predicate<Throwable> retryOnThrowableCondition;

    private RetryHandlerBuilder() {
    }

    public RetryHandlerBuilder withScheduler(Scheduler scheduler) {
        Preconditions.checkNotNull(scheduler);
        this.scheduler = scheduler;
        return this;
    }

    public RetryHandlerBuilder withReactorScheduler(reactor.core.scheduler.Scheduler scheduler) {
        Preconditions.checkNotNull(scheduler);
        this.reactorScheduler = scheduler;
        return this;
    }

    public RetryHandlerBuilder withRetryCount(int retryCount) {
        Preconditions.checkArgument(retryCount > 0, "Retry count must be >0");
        this.retryCount = retryCount;
        return this;
    }

    public RetryHandlerBuilder withUnlimitedRetries() {
        this.retryCount = Integer.MAX_VALUE / 2;
        return this;
    }

    public RetryHandlerBuilder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
        Preconditions.checkArgument(retryDelay > 0, "Retry delay must be >0");
        Preconditions.checkArgument(retryDelay < Integer.MAX_VALUE, "Retry delay should be < Integer.MAX_VALUE");
        this.retryDelayMs = timeUnit.toMillis(retryDelay);
        return this;
    }

    public RetryHandlerBuilder withMaxDelay(long maxDelay, TimeUnit timeUnit) {
        Preconditions.checkArgument(maxDelay > 0, "Max delay must be >0");
        this.maxDelay = timeUnit.toMillis(maxDelay);
        return this;
    }

    public RetryHandlerBuilder withDelay(long initial, long max, TimeUnit timeUnit) {
        withRetryDelay(initial, timeUnit);
        withMaxDelay(max, timeUnit);
        return this;
    }

    public RetryHandlerBuilder withTitle(String title) {
        Preconditions.checkNotNull(title);
        this.title = title;
        return this;
    }

    public RetryHandlerBuilder withOnErrorHook(Consumer<Throwable> hook) {
        Preconditions.checkNotNull(title);
        this.onErrorHook = hook::accept;
        return this;
    }

    public RetryHandlerBuilder withRetryWhen(Supplier<Boolean> retryWhenCondition) {
        this.retryWhenCondition = retryWhenCondition;
        return this;
    }

    public RetryHandlerBuilder withRetryOnThrowable(Predicate<Throwable> retryOnThrowable) {
        this.retryOnThrowableCondition = retryOnThrowable;
        return this;
    }

    public RetryHandlerBuilder but() {
        RetryHandlerBuilder newInstance = new RetryHandlerBuilder();
        newInstance.retryCount = retryCount;
        newInstance.scheduler = scheduler;
        newInstance.retryDelayMs = retryDelayMs;
        newInstance.maxDelay = maxDelay;
        newInstance.title = title;
        newInstance.onErrorHook = onErrorHook;
        return newInstance;
    }

    public Func1<Observable<? extends Throwable>, Observable<?>> buildExponentialBackoff() {
        Preconditions.checkState(retryCount > 0, "Retry count not defined");
        Preconditions.checkState(retryDelayMs > 0, "Retry delay not defined");

        return failedAttempts -> failedAttempts
                .doOnNext(error -> onErrorHook.call(error))
                .zipWith(Observable.range(0, retryCount + 1), RetryItem::new)
                .flatMap(retryItem -> {
                    if (retryWhenCondition != null && !retryWhenCondition.get()) {
                        String errorMessage = String.format(
                                "Retry condition not met for %s. Last error: %s. Returning an error to the caller",
                                title, retryItem.cause.getMessage()
                        );
                        return Observable.error(new IOException(errorMessage, retryItem.cause));
                    }

                    if (retryOnThrowableCondition != null && !retryOnThrowableCondition.test(retryItem.cause)) {
                        String errorMessage = String.format(
                                "Retry condition for the last error not met for %s. Last error: %s. Returning an error to the caller",
                                title, retryItem.cause.getMessage()
                        );
                        return Observable.error(new IOException(errorMessage, retryItem.cause));
                    }

                    if (retryItem.retry == retryCount) {
                        String errorMessage = String.format(
                                "Retry limit reached for %s. Last error: %s. Returning an error to the caller",
                                title, retryItem.cause.getMessage()
                        );
                        return Observable.error(new IOException(errorMessage, retryItem.cause));
                    }
                    long expDelay = buildDelay(retryItem.retry);
                    if (retryItem.cause instanceof TimeoutException) {
                        logger.info("Delaying timed-out {} retry by {}[ms]", title, expDelay);
                    } else {
                        logger.info("Delaying failed {} retry by {}[ms]: {}", title, expDelay, ExceptionExt.toMessageChain(retryItem.cause));
                        logger.debug("Exception", retryItem.cause);
                    }
                    return Observable.timer(expDelay, TimeUnit.MILLISECONDS, scheduler);
                });
    }

    public Function<Flux<Throwable>, Publisher<?>> buildReactorExponentialBackoff() {
        Preconditions.checkState(retryCount > 0, "Retry count not defined");
        Preconditions.checkState(retryDelayMs > 0, "Retry delay not defined");
        Preconditions.checkNotNull(reactorScheduler, "Reactor scheduler not set");

        return failedAttempts -> failedAttempts
                .doOnError(error -> onErrorHook.call(error))
                .zipWith(Flux.range(0, retryCount + 1), RetryItem::new)
                .flatMap(retryItem -> {
                    if (retryWhenCondition != null && !retryWhenCondition.get()) {
                        String errorMessage = String.format(
                                "Retry condition not met for %s. Last error: %s. Returning an error to the caller",
                                title, retryItem.cause.getMessage()
                        );
                        return Flux.error(new IOException(errorMessage, retryItem.cause));
                    }

                    if (retryOnThrowableCondition != null && !retryOnThrowableCondition.test(retryItem.cause)) {
                        String errorMessage = String.format(
                                "Retry condition for the last error not met for %s. Last error: %s. Returning an error to the caller",
                                title, retryItem.cause.getMessage()
                        );
                        return Flux.error(new IOException(errorMessage, retryItem.cause));
                    }

                    if (retryItem.retry == retryCount) {
                        String errorMessage = String.format(
                                "Retry limit reached for %s. Last error: %s. Returning an error to the caller",
                                title, retryItem.cause.getMessage()
                        );
                        return Flux.error(new IOException(errorMessage, retryItem.cause));
                    }
                    long expDelay = buildDelay(retryItem.retry);
                    if (retryItem.cause instanceof TimeoutException) {
                        logger.info("Delaying timed-out {} retry by {}[ms]", title, expDelay);
                    } else {
                        logger.info("Delaying failed {} retry by {}[ms]: {}", title, expDelay, ExceptionExt.toMessageChain(retryItem.cause));
                        logger.debug("Exception", retryItem.cause);
                    }
                    return Flux.interval(Duration.ofMillis(expDelay), reactorScheduler).take(1);
                });
    }

    public Retry buildRetryExponentialBackoff() {
        return new Retry() {
            @Override
            public Publisher<?> generateCompanion(Flux<RetrySignal> retrySignals) {
                return buildReactorExponentialBackoff().apply(retrySignals.map(RetrySignal::failure));
            }
        };
    }

    private long buildDelay(int retry) {
        // at retry == 31, delay expression [ (1 << retry) * retryDelayMs ] causes overflows for long value
        // hence we fall back to maxDelay for subsequent retries
        if (retry > 30) {
            return maxDelay;
        }
        final long backOffTime = (1 << retry) * retryDelayMs;
        return Math.min(maxDelay, backOffTime);
    }

    public static RetryHandlerBuilder retryHandler() {
        return new RetryHandlerBuilder();
    }

    static class RetryItem {
        private Throwable cause;
        private int retry;

        RetryItem(Throwable cause, int retry) {
            this.cause = cause;
            this.retry = retry;
        }
    }
}
