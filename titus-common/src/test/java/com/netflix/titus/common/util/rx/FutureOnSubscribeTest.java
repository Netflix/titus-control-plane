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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class FutureOnSubscribeTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final CompletableFuture<String> future = new CompletableFuture<>();

    private final ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();

    private Subscription subscription;

    @Before
    public void setUp() throws Exception {
        this.subscription = ObservableExt.toObservable(future, testScheduler).subscribe(testSubscriber);
    }

    @Test
    public void testOk() throws Exception {
        testScheduler.triggerActions();
        assertThat(testSubscriber.isUnsubscribed()).isFalse();
        assertThat(testSubscriber.takeNext()).isNull();

        future.complete("Hello!");
        advance();

        assertThat(testSubscriber.isUnsubscribed()).isTrue();
        assertThat(subscription.isUnsubscribed()).isTrue();
        assertThat(testSubscriber.takeNext()).isEqualTo("Hello!");
    }

    @Test
    public void testCancelled() throws Exception {
        future.cancel(true);
        advance();

        assertThat(testSubscriber.isUnsubscribed()).isTrue();
        assertThat(testSubscriber.isError()).isTrue();
    }

    @Test
    public void testException() throws Exception {
        future.completeExceptionally(new RuntimeException("simulated error"));
        advance();

        assertThat(testSubscriber.isUnsubscribed()).isTrue();
        assertThat(testSubscriber.isError()).isTrue();
    }

    @Test
    public void testUnsubscribe() throws Exception {
        subscription.unsubscribe();
        advance();

        assertThat(testSubscriber.isUnsubscribed()).isTrue();
        assertThat(testSubscriber.isError()).isFalse();
    }

    private void advance() {
        testScheduler.advanceTimeBy(FutureOnSubscribe.INITIAL_DELAY_MS, TimeUnit.MILLISECONDS);
    }
}