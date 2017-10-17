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

package io.netflix.titus.common.network.client.internal;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netflix.spectator.api.Id;
import io.netflix.titus.common.network.client.RxRestClient;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;

/**
 * A retryable client, that re-executes requests a configurable number of times. Each subsequent request is
 * delayed with exponential backoff strategy.
 */
public class RetryableRestClient implements RxRestClient {

    private static final Logger logger = LoggerFactory.getLogger(RetryableRestClient.class);

    /* Visible for testing */ static final int MAX_DELAY_MS = 60 * 1000;

    private final RxRestClient delegate;
    private final int retryCount;
    private final RxClientMetric rxClientMetric;
    private final Scheduler scheduler;
    private final long timeoutMs;
    private final long retryDelayMs;

    public RetryableRestClient(RxRestClient delegate, int retryCount, long timeout, long retryDelay, TimeUnit timeUnit,
                               RxClientMetric rxClientMetric, Scheduler scheduler) {
        this.delegate = delegate;
        this.retryCount = retryCount;
        this.rxClientMetric = rxClientMetric;
        this.scheduler = scheduler;
        this.timeoutMs = timeUnit.toMillis(timeout);
        this.retryDelayMs = timeUnit.toMillis(retryDelay);
    }

    @Override
    public <T> Observable<T> doGET(String relativeURI, TypeProvider<T> typeProvider) {
        return runWithRetry(HttpMethod.GET, relativeURI, delegate.doGET(relativeURI, typeProvider));
    }

    @Override
    public <REQ> Observable<Void> doPOST(String relativeURI, REQ entity) {
        return runWithRetry(HttpMethod.POST, relativeURI, delegate.doPOST(relativeURI, entity));
    }

    @Override
    public <REQ, RESP> Observable<RESP> doPOST(String relativeURI, REQ entity, TypeProvider<RESP> replyTypeProvider) {
        return runWithRetry(HttpMethod.POST, relativeURI, delegate.doPOST(relativeURI, entity, replyTypeProvider));
    }

    @Override
    public <REQ> Observable<Void> doPUT(String relativeURI, REQ entity) {
        return runWithRetry(HttpMethod.PUT, relativeURI, delegate.doPUT(relativeURI, entity));
    }

    @Override
    public <REQ, RESP> Observable<RESP> doPUT(String relativeURI, REQ entity, TypeProvider<RESP> replyTypeProvider) {
        return runWithRetry(HttpMethod.PUT, relativeURI, delegate.doPUT(relativeURI, entity, replyTypeProvider));
    }

    private <T> Observable<T> runWithRetry(HttpMethod httpMethod, String relativeURI, Observable<T> request) {
        return request
                .timeout(timeoutMs, TimeUnit.MILLISECONDS, scheduler)
                .retryWhen(createExponentialBackoffHandler(
                        rxClientMetric.createGetId("retry"),
                        retryCount,
                        retryDelayMs,
                        RxRestClientUtil.requestSignature(httpMethod.name(), relativeURI)
                ));
    }

    private Func1<Observable<? extends Throwable>, Observable<?>> createExponentialBackoffHandler(
            Id retryId, int retryCount, long retryDelayMs, String reqSignature) {
        return failedAttempts -> {
            return failedAttempts
                    .doOnNext(error -> rxClientMetric.increment(retryId))
                    .zipWith(Observable.range(0, retryCount + 1), RetryItem::new)
                    .flatMap(retryItem -> {
                        if (retryItem.retry == retryCount) {
                            String errorMessage = String.format(
                                    "Retry limit reached for %s REST call. Last error: %s. Returning an error to the caller",
                                    reqSignature, retryItem.cause.getMessage()
                            );
                            return Observable.error(new IOException(errorMessage, retryItem.cause));
                        }
                        long expDelay = Math.min(MAX_DELAY_MS, (2 << retryItem.retry) * retryDelayMs);
                        if (retryItem.cause instanceof TimeoutException) {
                            logger.info("Delaying timed-out {} REST call retry by {}[ms]", reqSignature, expDelay);
                        } else {
                            logger.info("Delaying failed {} REST call retry by {}[ms]", reqSignature, expDelay);
                        }
                        return Observable.timer(expDelay, TimeUnit.MILLISECONDS, scheduler);
                    });
        };
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
