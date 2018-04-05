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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

public class SubscriptionTimeoutTest {
    private static final Supplier<Long> TEST_TIMEOUT_MS = () -> 10_000L;

    private SerializedSubject<String, String> source;
    private TestScheduler testScheduler;
    private AssertableSubscriber<String> subscriber;

    @Before
    public void setUp() throws Exception {
        this.source = PublishSubject.<String>create().toSerialized();
        this.testScheduler = Schedulers.test();
        this.subscriber = source
                .lift(new SubscriptionTimeout<>(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, testScheduler))
                .test();
    }

    @Test
    public void doesNotErrorBeforeTimeout() {
        testScheduler.triggerActions();
        subscriber.assertNoValues()
                .assertNoErrors()
                .assertNoTerminalEvent();

        testScheduler.advanceTimeBy(TEST_TIMEOUT_MS.get() - 1, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues()
                .assertNoErrors()
                .assertNoTerminalEvent();

        source.onNext("one");
        subscriber.assertNoErrors()
                .assertValuesAndClear("one")
                .assertNoTerminalEvent();

        source.onCompleted();
        subscriber.assertNoValues()
                .assertNoErrors()
                .assertCompleted();

        source.onNext("invalid");
        subscriber.assertNoValues()
                .assertNoErrors()
                .assertCompleted();

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues()
                .assertNoErrors()
                .assertCompleted();

        testScheduler.advanceTimeBy(TEST_TIMEOUT_MS.get(), TimeUnit.MILLISECONDS);
        subscriber.assertNoValues()
                .assertNoErrors()
                .assertCompleted();
    }

    @Test
    public void timeoutEmitsOnError() {
        testScheduler.triggerActions();
        subscriber.assertNoValues()
                .assertNoErrors()
                .assertNoTerminalEvent();

        // emit some items for some time before the timeout (TEST_TIMEOUT_MS - 1)
        final int items = 10;
        for (int i = 0; i <= items - 1; i++) {
            source.onNext("" + i);
            testScheduler.advanceTimeBy((TEST_TIMEOUT_MS.get() - 1) / items, TimeUnit.MILLISECONDS);
            subscriber.assertNoErrors()
                    .assertValuesAndClear("" + i)
                    .assertNoTerminalEvent();
        }

        testScheduler.advanceTimeBy(TEST_TIMEOUT_MS.get() / items, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues()
                .assertError(TimeoutException.class);

        source.onNext("invalid");
        testScheduler.triggerActions();
        subscriber.assertNoValues();
        testScheduler.advanceTimeBy(TEST_TIMEOUT_MS.get() / items, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();
    }
}