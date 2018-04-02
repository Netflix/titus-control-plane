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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class PeriodicGeneratorTest {

    private static final long INITIAL_DELAY_MS = 1;
    private static final long INTERVAL_MS = 5;

    private static final Throwable SIMULATED_ERROR = new RuntimeException("simulated error");

    private static final List<String> SOURCE_VALUES = asList("A", "B");
    private static final Observable<String> OK_SOURCE = Observable.from(SOURCE_VALUES);
    private static final Observable<String> BAD_SOURCE = Observable.just("A").concatWith(Observable.error(SIMULATED_ERROR));

    private final TestScheduler testScheduler = Schedulers.test();

    private final ExtTestSubscriber<List<String>> testSubscriber = new ExtTestSubscriber<>();

    @Test
    public void testOkEmitter() throws Exception {
        Subscription subscription = ObservableExt.periodicGenerator(
                OK_SOURCE, INITIAL_DELAY_MS, INTERVAL_MS, TimeUnit.MILLISECONDS, testScheduler
        ).subscribe(testSubscriber);

        assertThat(testSubscriber.takeNext()).isNull();

        // Check emits
        expectOkEmit(INITIAL_DELAY_MS);
        expectOkEmit(INTERVAL_MS);
        expectOkEmit(INTERVAL_MS);

        // Unsubscribe
        subscription.unsubscribe();
        testSubscriber.onCompleted();
    }

    @Test
    public void testFailingEmitter() throws Exception {
        ObservableExt.periodicGenerator(
                BAD_SOURCE, INITIAL_DELAY_MS, INTERVAL_MS, TimeUnit.MILLISECONDS, testScheduler
        ).subscribe(testSubscriber);

        // Check emits
        expectBadEmit(INITIAL_DELAY_MS);
    }

    private void expectOkEmit(long delayMs) {
        testScheduler.advanceTimeBy(delayMs - 1, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.takeNext()).isNull();
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.takeNext()).isEqualTo(SOURCE_VALUES);
    }

    private void expectBadEmit(long delayMs) {
        testScheduler.advanceTimeBy(delayMs - 1, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.takeNext()).isNull();
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.getError()).isEqualTo(SIMULATED_ERROR);
    }
}