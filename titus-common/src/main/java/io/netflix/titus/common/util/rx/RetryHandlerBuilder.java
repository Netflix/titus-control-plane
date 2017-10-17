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

package io.netflix.titus.common.util.rx;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private int retryCount = -1;
    private long retryDelayMs = -1;
    private long maxDelay = Long.MAX_VALUE;
    private String title = "observable";

    private Action1<Throwable> onErrorHook = e -> {
    };

    private RetryHandlerBuilder() {
    }

    public RetryHandlerBuilder withScheduler(Scheduler scheduler) {
        Preconditions.checkNotNull(scheduler);
        this.scheduler = scheduler;
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
        Preconditions.checkArgument(retryCount > 0, "Retry delay must be >0");
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

    public RetryHandlerBuilder but() {
        RetryHandlerBuilder newInstance = new RetryHandlerBuilder();
        newInstance.retryCount = retryCount;
        newInstance.scheduler = scheduler;
        newInstance.retryCount = retryCount;
        newInstance.retryDelayMs = retryDelayMs;
        newInstance.maxDelay = maxDelay;
        newInstance.title = title;
        newInstance.onErrorHook = onErrorHook;
        return newInstance;
    }

    public Func1<Observable<? extends Throwable>, Observable<?>> buildExponentialBackoff() {
        Preconditions.checkState(retryCount > 0, "Retry count not defined");
        Preconditions.checkState(retryDelayMs > 0, "Retry delay not defined");

        return failedAttempts -> {
            return failedAttempts
                    .doOnNext(error -> onErrorHook.call(error))
                    .zipWith(Observable.range(0, retryCount + 1), RetryItem::new)
                    .flatMap(retryItem -> {
                        if (retryItem.retry == retryCount) {
                            String errorMessage = String.format(
                                    "Retry limit reached for %s. Last error: %s. Returning an error to the caller",
                                    title, retryItem.cause.getMessage()
                            );
                            return Observable.error(new IOException(errorMessage, retryItem.cause));
                        }
                        long expDelay = Math.min(maxDelay, (1 << retryItem.retry) * retryDelayMs);
                        if (retryItem.cause instanceof TimeoutException) {
                            logger.info("Delaying timed-out {} retry by {}[ms]", title, expDelay);
                        } else {
                            logger.info("Delaying failed {} retry by {}[ms]", title, expDelay);
                        }
                        return Observable.timer(expDelay, TimeUnit.MILLISECONDS, scheduler);
                    });
        };
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
