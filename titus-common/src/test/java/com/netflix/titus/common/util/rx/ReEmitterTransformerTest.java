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

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;

public class ReEmitterTransformerTest {

    private static final long INTERVAL_MS = 1_000;

    public final TestScheduler testScheduler = Schedulers.test();

    private final PublishSubject<String> subject = PublishSubject.create();

    private final ExtTestSubscriber<String> subscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() {
        subject.compose(ObservableExt.reemiter(String::toLowerCase, INTERVAL_MS, TimeUnit.MILLISECONDS, testScheduler)).subscribe(subscriber);
    }

    @Test
    public void testReemits() {
        // Until first item is emitted, there is nothing to re-emit.
        advanceSteps(2);
        assertThat(subscriber.takeNext()).isNull();

        // Emit 'A' and wait for re-emit
        subject.onNext("A");
        assertThat(subscriber.takeNext()).isEqualTo("A");

        advanceSteps(1);
        assertThat(subscriber.takeNext()).isEqualTo("a");

        // Emit 'B' and wait for re-emit
        subject.onNext("B");
        assertThat(subscriber.takeNext()).isEqualTo("B");

        advanceSteps(1);
        assertThat(subscriber.takeNext()).isEqualTo("b");
    }

    @Test
    public void testNoReemits() {
        subject.onNext("A");
        assertThat(subscriber.takeNext()).isEqualTo("A");

        testScheduler.advanceTimeBy(INTERVAL_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(subscriber.takeNext()).isNull();

        subject.onNext("B");
        assertThat(subscriber.takeNext()).isEqualTo("B");

        testScheduler.advanceTimeBy(INTERVAL_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(subscriber.takeNext()).isNull();
    }

    private void advanceSteps(int steps) {
        testScheduler.advanceTimeBy(steps * INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
}