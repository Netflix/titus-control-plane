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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.After;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class HeadTransformerTest {

    private static final List<String> HEAD_ITEMS = asList("A", "B");

    private final AtomicBoolean terminateFlag = new AtomicBoolean();

    private final List<String> emittedTicks = new CopyOnWriteArrayList<>();

    private final Observable<String> hotObservable = Observable.interval(0, 1, TimeUnit.MILLISECONDS, Schedulers.io())
            .map(tick -> {
                String value = "T" + tick;
                emittedTicks.add(value);
                return value;
            })
            .doOnTerminate(() -> terminateFlag.set(true))
            .doOnUnsubscribe(() -> terminateFlag.set(true));

    private final Observable<String> combined = hotObservable.compose(ObservableExt.head(() -> {
        while (emittedTicks.size() < 2) { // Tight loop
        }
        return HEAD_ITEMS;
    }));

    private final ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();

    @After
    public void tearDown() throws Exception {
        testSubscriber.unsubscribe();
    }

    @Test
    public void testHotObservableBuffering() throws Exception {
        Subscription subscription = combined.subscribe(testSubscriber);

        // Wait until hot observable emits directly next item
        int currentTick = emittedTicks.size();
        while (emittedTicks.size() == currentTick) {
        }
        int checkedCount = HEAD_ITEMS.size() + currentTick + 1;
        List<String> checkedSequence = testSubscriber.takeNext(checkedCount, 5, TimeUnit.SECONDS);

        testSubscriber.unsubscribe();

        assertThat(terminateFlag.get()).isTrue();
        assertThat(subscription.isUnsubscribed()).isTrue();

        List<String> expected = new ArrayList<>(HEAD_ITEMS);
        expected.addAll(emittedTicks.subList(0, currentTick + 1));
        assertThat(checkedSequence).containsExactlyElementsOf(expected);
    }

    @Test
    public void testExceptionInSubscriberTerminatesSubscription() throws Exception {
        AtomicInteger errorCounter = new AtomicInteger();
        Subscription subscription = combined.subscribe(
                next -> {
                    if (next.equals("T3")) {
                        throw new RuntimeException("simulated error");
                    }
                },
                e -> errorCounter.incrementAndGet()
        );

        // Wait until hot observable emits error
        while (!terminateFlag.get()) {
        }

        assertThat(terminateFlag.get()).isTrue();
        assertThat(subscription.isUnsubscribed()).isTrue();
    }

    @Test
    public void testExceptionInHead() throws Exception {
        Observable.just("A").compose(ObservableExt.head(() -> {
            throw new RuntimeException("Simulated error");
        })).subscribe(testSubscriber);
        testSubscriber.assertOnError();
    }
}