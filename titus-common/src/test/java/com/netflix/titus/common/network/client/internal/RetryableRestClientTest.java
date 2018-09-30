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

package com.netflix.titus.common.network.client.internal;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.network.client.RxRestClient;
import com.netflix.titus.common.network.client.RxRestClientException;
import com.netflix.titus.common.network.client.TypeProviders;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryableRestClientTest {

    private static final RxRestClient.TypeProvider<String> STRING_TYPE_PROVIDER = TypeProviders.of(String.class);

    private static final long REQ_TIMEOUT_MS = 1000;
    private static final long RETRY_DELAY_MS = 200;
    private static final int RETRY_COUNT = 10;
    private static final String REPLY_TEXT = "ok";
    private static final HttpResponseStatus noRetryStatus = HttpResponseStatus.NOT_FOUND;

    private final TestScheduler testScheduler = Schedulers.test();

    private final RxClientMetric clientMetric = new RxClientMetric("testClient", new DefaultRegistry());

    private final RxRestClient delegate = mock(RxRestClient.class);

    private RetryableRestClient client;

    @Before
    public void setUp() throws Exception {
        client = new RetryableRestClient(delegate,
                RETRY_COUNT, REQ_TIMEOUT_MS, RETRY_DELAY_MS, TimeUnit.MILLISECONDS, new HashSet<>(Collections.singletonList(noRetryStatus)), clientMetric, testScheduler);
    }

    @Test
    public void testRetryOnError() throws Exception {
        when(delegate.doGET(eq("/path"), anyMap(), eq(STRING_TYPE_PROVIDER))).thenReturn(createFailingObservable(RETRY_COUNT));

        ExtTestSubscriber<String> replySubscriber = new ExtTestSubscriber<>();
        client.doGET("/path", STRING_TYPE_PROVIDER).subscribe(replySubscriber);

        testScheduler.advanceTimeBy(RETRY_COUNT * RetryableRestClient.MAX_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(replySubscriber.takeNext()).isEqualTo(REPLY_TEXT);
    }

    @Test
    public void testRetryWithPermanentError() throws Exception {
        when(delegate.doGET(eq("/path"), anyMap(), eq(STRING_TYPE_PROVIDER))).thenReturn(createFailingObservable(RETRY_COUNT + 1));

        ExtTestSubscriber<String> replySubscriber = new ExtTestSubscriber<>();
        client.doGET("/path", STRING_TYPE_PROVIDER).subscribe(replySubscriber);

        testScheduler.advanceTimeBy((RETRY_COUNT + 1) * RetryableRestClient.MAX_DELAY_MS, TimeUnit.MILLISECONDS);
        replySubscriber.assertOnError(IOException.class);
    }

    @Test
    public void testRetryOnTimeout() throws Exception {
        when(delegate.doGET(eq("/path"), anyMap(), eq(STRING_TYPE_PROVIDER))).thenReturn(createSlowObservable(RETRY_COUNT, REQ_TIMEOUT_MS));

        ExtTestSubscriber<String> replySubscriber = new ExtTestSubscriber<>();
        client.doGET("/path", STRING_TYPE_PROVIDER).subscribe(replySubscriber);

        testScheduler.advanceTimeBy(RETRY_COUNT * (RetryableRestClient.MAX_DELAY_MS + REQ_TIMEOUT_MS), TimeUnit.MILLISECONDS);
        assertThat(replySubscriber.takeNext()).isEqualTo(REPLY_TEXT);
    }

    @Test
    public void testNonretryableStatus() throws Exception {
        when(delegate.doGET(eq("/path"), anyMap(), eq(STRING_TYPE_PROVIDER))).thenReturn(createStatusErrorObservable(noRetryStatus));

        ExtTestSubscriber<String> replySubscriber = new ExtTestSubscriber<>();
        client.doGET("/path", STRING_TYPE_PROVIDER).subscribe(replySubscriber);

        replySubscriber.assertOnError(toIntExact(REQ_TIMEOUT_MS), TimeUnit.MILLISECONDS);
        assertThat(replySubscriber.getError()).isExactlyInstanceOf(RxRestClientException.class);
    }

    private Observable<String> createFailingObservable(int errorCount) {
        AtomicInteger reqCount = new AtomicInteger();
        return Observable.create(subscriber -> {
            if (reqCount.getAndIncrement() < errorCount) {
                subscriber.onError(new RuntimeException("simulated network error"));
            } else {
                subscriber.onNext(REPLY_TEXT);
                subscriber.onCompleted();
            }
        });
    }

    private Observable<String> createSlowObservable(int delayCount, long delayMs) {
        AtomicInteger reqCount = new AtomicInteger();
        return Observable.create(subscriber -> {
            if (reqCount.getAndIncrement() < delayCount) {
                Observable.timer(delayMs, TimeUnit.MILLISECONDS, testScheduler).cast(String.class).subscribe(subscriber);
            } else {
                subscriber.onNext(REPLY_TEXT);
                subscriber.onCompleted();
            }
        });
    }

    private Observable<String> createStatusErrorObservable(HttpResponseStatus status) {
        return Observable.error(new RxRestClientException(status.code()));
    }
}