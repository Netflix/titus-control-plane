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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.jayway.awaitility.Awaitility;
import org.junit.Test;
import rx.Producer;
import rx.Subscriber;
import rx.Subscription;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueGeneratorTest {

    @Test
    public void testSimpleValueGeneration() throws Exception {
        final int limit = 100;
        AtomicInteger valueSource = new AtomicInteger();
        AtomicInteger emmitCounter = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);
        Subscription subscription = ObservableExt.generatorFrom(valueSource::getAndIncrement).subscribe(new Subscriber<Integer>() {
            private Producer producer;

            @Override
            public void setProducer(Producer p) {
                this.producer = p;
                p.request(1);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                latch.countDown();
                e.printStackTrace();
            }

            @Override
            public void onNext(Integer integer) {
                if (emmitCounter.incrementAndGet() < limit) {
                    producer.request(1);
                } else {
                    unsubscribe();
                    latch.countDown();
                }
            }
        });
        latch.await();
        assertThat(emmitCounter.get()).isEqualTo(limit);
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(subscription::isUnsubscribed);
    }

    @Test
    public void testFailureInValueProviderIsPropagatedToSubscriber() throws Exception {
        Supplier<Integer> failingSupplier = () -> {
            throw new RuntimeException("simulated error");
        };
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Subscription subscription = ObservableExt.generatorFrom(failingSupplier).subscribe(new Subscriber<Integer>() {

            @Override
            public void setProducer(Producer p) {
                p.request(1);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                errorRef.set(e);
                latch.countDown();
            }

            @Override
            public void onNext(Integer integer) {
            }
        });
        latch.await();
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(subscription::isUnsubscribed);
        assertThat(errorRef.get()).hasMessageContaining("simulated error");
    }
}