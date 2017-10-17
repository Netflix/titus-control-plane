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
import java.util.concurrent.atomic.AtomicInteger;

import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.common.util.rx.RetryHandlerBuilder.retryHandler;
import static org.assertj.core.api.Assertions.assertThat;

public class RetryHandlerBuilderTest {

    private static final long RETRY_DELAY_SEC = 1;

    private final TestScheduler testScheduler = Schedulers.test();
    private final ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();

    private final RetryHandlerBuilder builder = retryHandler()
            .withScheduler(testScheduler)
            .withTitle("testObservable")
            .withRetryCount(3)
            .withRetryDelay(RETRY_DELAY_SEC, TimeUnit.SECONDS);

    @Test
    public void testRetryOnError() throws Exception {
        Func1<Observable<? extends Throwable>, Observable<?>> retryFun = builder.buildExponentialBackoff();

        observableOf("A", new IOException("Error1"), "B", new IOException("Error2"), "C")
                .retryWhen(retryFun, testScheduler)
                .subscribe(testSubscriber);

        // Expect first item
        testScheduler.triggerActions();
        assertThat(testSubscriber.getLatestItem()).isEqualTo("A");

        // Expect second item
        testScheduler.advanceTimeBy(RETRY_DELAY_SEC, TimeUnit.SECONDS);
        assertThat(testSubscriber.getLatestItem()).isEqualTo("B");

        // Expect third item
        testScheduler.advanceTimeBy(2 * RETRY_DELAY_SEC, TimeUnit.SECONDS);
        assertThat(testSubscriber.getLatestItem()).isEqualTo("C");

        assertThat(testSubscriber.isUnsubscribed());
    }

    @Test
    public void testMaxRetryDelay() throws Exception {
        Func1<Observable<? extends Throwable>, Observable<?>> retryFun = builder
                .withMaxDelay(RETRY_DELAY_SEC * 2, TimeUnit.SECONDS)
                .buildExponentialBackoff();

        observableOf(new IOException("Error1"), new IOException("Error2"), new IOException("Error3"), "A")
                .retryWhen(retryFun, testScheduler)
                .subscribe(testSubscriber);

        long expectedDelay = RETRY_DELAY_SEC + 2 * RETRY_DELAY_SEC + 2 * RETRY_DELAY_SEC;

        // Advance time just before last retry delay
        testScheduler.advanceTimeBy(expectedDelay - 1, TimeUnit.SECONDS);
        assertThat(testSubscriber.takeNext()).isNull();

        // Now cross it
        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getLatestItem()).isEqualTo("A");

        assertThat(testSubscriber.isUnsubscribed());
    }

    @Test
    public void testMaxRetry() throws Exception {
        Func1<Observable<? extends Throwable>, Observable<?>> retryFun = builder
                .withRetryCount(1)
                .buildExponentialBackoff();

        observableOf(new IOException("Error1"), new IOException("Error2"), "A")
                .retryWhen(retryFun, testScheduler)
                .subscribe(testSubscriber);

        testScheduler.advanceTimeBy(RETRY_DELAY_SEC, TimeUnit.SECONDS);

        assertThat(testSubscriber.getError()).isInstanceOf(IOException.class);
    }

    private Observable<String> observableOf(Object... items) {
        AtomicInteger pos = new AtomicInteger();
        return Observable.create(subscriber -> {
            for (int i = pos.get(); i < items.length; i++) {
                pos.incrementAndGet();
                Object item = items[i];

                if (item instanceof Throwable) {
                    subscriber.onError((Throwable) item);
                    return;
                }
                subscriber.onNext((String) item);
            }
            subscriber.onCompleted();
        });
    }
}