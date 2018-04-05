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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class ComputationTaskInvokerTest {

    private static final long TASK_DELAY_MS = 30;

    private final TestScheduler testScheduler = Schedulers.test();

    private final AtomicInteger invocationCounter = new AtomicInteger();

    private final ComputationTaskInvoker<String> invoker = new ComputationTaskInvoker<>(
            Observable.timer(TASK_DELAY_MS, TimeUnit.MILLISECONDS, testScheduler).map(tick -> "Done round " + invocationCounter.getAndIncrement()), testScheduler
    );

    @Test
    public void testNonContentedComputation() throws Exception {
        ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();
        invoker.recompute().subscribe(testSubscriber);

        testScheduler.advanceTimeBy(TASK_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.takeNext()).isEqualTo("Done round 0");
        assertThat(testSubscriber.isUnsubscribed()).isTrue();
        assertThat(invocationCounter.get()).isEqualTo(1);
    }

    @Test
    public void testContentedComputation() throws Exception {
        ExtTestSubscriber<String> testSubscriber1 = new ExtTestSubscriber<>();
        ExtTestSubscriber<String> testSubscriber2 = new ExtTestSubscriber<>();
        ExtTestSubscriber<String> testSubscriber3 = new ExtTestSubscriber<>();

        invoker.recompute().subscribe(testSubscriber1);

        // Advance time to start computation
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        // Now subscriber again. The new subscriptions should wait for their turn.
        invoker.recompute().subscribe(testSubscriber2);
        invoker.recompute().subscribe(testSubscriber3);

        testScheduler.advanceTimeBy(TASK_DELAY_MS - 1, TimeUnit.MILLISECONDS);

        assertThat(testSubscriber1.takeNext()).isEqualTo("Done round 0");
        assertThat(testSubscriber1.isUnsubscribed()).isTrue();

        assertThat(testSubscriber2.takeNext()).isNull();
        assertThat(testSubscriber3.takeNext()).isNull();

        assertThat(invocationCounter.get()).isEqualTo(1);

        // Now trigger next computation
        testScheduler.advanceTimeBy(TASK_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber2.takeNext()).isEqualTo("Done round 1");
        assertThat(testSubscriber3.takeNext()).isEqualTo("Done round 1");

        assertThat(invocationCounter.get()).isEqualTo(2);
    }

    @Test
    public void testFailingComputations() throws Exception {
        AtomicBoolean flag = new AtomicBoolean();
        ComputationTaskInvoker<String> slowInvoker = new ComputationTaskInvoker<>(
                Observable.create(subscriber -> {
                    if (flag.getAndSet(true)) {
                        subscriber.onNext("OK!");
                        subscriber.onCompleted();
                    } else {
                        Observable.<String>timer(1, TimeUnit.SECONDS, testScheduler)
                                .flatMap(tick -> Observable.<String>error(new RuntimeException("simulated computation error")))
                                .subscribe(subscriber);
                    }
                }),
                testScheduler
        );

        ExtTestSubscriber<String> failingSubscriber = new ExtTestSubscriber<>();
        slowInvoker.recompute().subscribe(failingSubscriber);

        testScheduler.triggerActions();
        ExtTestSubscriber<String> okSubscriber = new ExtTestSubscriber<>();
        slowInvoker.recompute().subscribe(okSubscriber);

        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        failingSubscriber.assertOnError();
        assertThat(okSubscriber.takeNext()).isEqualTo("OK!");
        okSubscriber.assertOnCompleted();
    }
}