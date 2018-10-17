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

package com.netflix.titus.ext.aws;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.netflix.titus.common.util.rx.RetryHandlerBuilder.retryHandler;

public class RetryWrapper {
    private static Logger log = LoggerFactory.getLogger(RetryWrapper.class);

    private static final int RETRY_COUNT = 3;
    private static final int RETRY_DELAY_SECONDS = 2;

    public static <T> Observable<T> wrapWithExponentialRetry(String retryHandlerTitle, Observable<T> sourceObservable) {

        return wrapWithExponentialRetry(retryHandlerTitle, RETRY_COUNT, RETRY_DELAY_SECONDS, sourceObservable);
    }

    public static <T> Observable<T> wrapWithExponentialRetry(String retryHandlerTitle, int retryCount, int retryDelaySeconds,
                                                             Observable<T> sourceObservable) {
        return sourceObservable.retryWhen(
                retryHandler()
                        .withTitle(retryHandlerTitle)
                        .withRetryCount(retryCount)
                        .withRetryDelay(retryDelaySeconds, TimeUnit.SECONDS)
                        .buildExponentialBackoff());

    }


}
