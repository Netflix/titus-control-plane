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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionFactory;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;

import static org.assertj.core.api.Assertions.assertThat;

public class DemultipexTest {

    private final ExtTestSubscriber<String> subscriber1 = new ExtTestSubscriber<>();
    private final ExtTestSubscriber<String> subscriber2 = new ExtTestSubscriber<>();

    @Test
    public void testTwoOutputs() {
        AtomicInteger counter = new AtomicInteger();
        List<Observable<String>> outputs = ObservableExt.demultiplex(
                Observable.defer(() -> Observable.just("Subscription#" + counter.incrementAndGet())),
                2
        );

        assertThat(counter.get()).isEqualTo(0);

        outputs.get(0).subscribe(subscriber1);
        assertThat(counter.get()).isEqualTo(0);

        outputs.get(1).subscribe(subscriber2);
        assertThat(counter.get()).isEqualTo(1);

        assertThat(subscriber1.takeNext()).isEqualTo("Subscription#1");
        assertThat(subscriber2.takeNext()).isEqualTo("Subscription#1");
        subscriber1.assertOnCompleted();
        subscriber2.assertOnCompleted();
    }

    @Test
    public void testCancellationOfOneOutput() {
        List<Observable<String>> outputs = ObservableExt.demultiplex(Observable.never(), 2);

        Subscription subscription1 = outputs.get(0).subscribe(subscriber1);
        outputs.get(1).subscribe(subscriber2);

        assertThat(subscriber2.isUnsubscribed()).isFalse();
        subscription1.unsubscribe();
        assertThat(subscriber2.isUnsubscribed()).isTrue();
        assertThat(subscriber2.getError()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSourceWithTimeout() {
        List<Observable<String>> outputs = ObservableExt.demultiplex(
                Observable.<String>never().timeout(1, TimeUnit.MILLISECONDS),
                2
        );

        outputs.get(0).subscribe(subscriber1);
        outputs.get(1).subscribe(subscriber2);

        await().until(() -> subscriber1.isUnsubscribed() && subscriber2.isUnsubscribed());
        assertThat(subscriber1.getError()).isInstanceOf(TimeoutException.class);
        assertThat(subscriber2.getError()).isInstanceOf(TimeoutException.class);
    }

    private ConditionFactory await() {
        return Awaitility.await().timeout(30, TimeUnit.SECONDS);
    }
}