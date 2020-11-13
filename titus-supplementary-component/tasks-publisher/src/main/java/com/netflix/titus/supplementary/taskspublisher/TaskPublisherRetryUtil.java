/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.titus.supplementary.taskspublisher;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

public class TaskPublisherRetryUtil {
    public static final long INITIAL_RETRY_DELAY_MS = 500;
    public static final long MAX_RETRY_DELAY_MS = 2_000;

    public static Retry buildRetryHandler(long initialRetryDelayMillis,
                                          long maxRetryDelayMillis, int maxRetries) {
        RetryHandlerBuilder retryHandlerBuilder = RetryHandlerBuilder.retryHandler();
        if (maxRetries < 0) {
            retryHandlerBuilder.withUnlimitedRetries();
        } else {
            retryHandlerBuilder.withRetryCount(maxRetries);
        }

        return retryHandlerBuilder
                .withDelay(initialRetryDelayMillis, maxRetryDelayMillis, TimeUnit.MILLISECONDS)
                .withReactorScheduler(Schedulers.elastic())
                .buildRetryExponentialBackoff();
    }

}
