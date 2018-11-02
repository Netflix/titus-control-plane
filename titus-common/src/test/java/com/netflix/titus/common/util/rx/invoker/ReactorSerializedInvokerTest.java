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

package com.netflix.titus.common.util.rx.invoker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.util.time.Clocks;
import org.junit.After;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class ReactorSerializedInvokerTest {

    private static final Duration QUEUEING_TIMEOUT = Duration.ofMillis(500);
    private static final Duration EXCESSIVE_RUNNING_TIME = Duration.ofMillis(1_000);

    private ReactorSerializedInvoker<String> reactorSerializedInvoker = ReactorSerializedInvoker.<String>newBuilder()
            .withName("test")
            .withScheduler(Schedulers.parallel())
            .withMaxQueueSize(10)
            .withExcessiveRunningTime(EXCESSIVE_RUNNING_TIME)
            .withRegistry(new DefaultRegistry())
            .withClock(Clocks.system())
            .build();

    @After
    public void tearDown() {
        reactorSerializedInvoker.shutdown(Duration.ofMillis(1));
    }

    @Test(timeout = 5_000)
    public void testInlineAction() {
        List<String> result = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 5; i++) {
            int index = i;
            reactorSerializedInvoker.submit(Mono.just("#" + index).map(tick -> "#" + index)).subscribe(result::add);
        }
        await().until(() -> result.size() == 5);
        assertThat(result).contains("#0", "#1", "#2", "#3", "#4");
    }

    @Test(timeout = 5_000)
    public void testLongAsynchronousAction() {
        List<String> result = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 5; i++) {
            int index = i;
            reactorSerializedInvoker.submit(Mono.delay(Duration.ofMillis(10)).map(tick -> "#" + index)).subscribe(result::add);
        }
        await().until(() -> result.size() == 5);
        assertThat(result).contains("#0", "#1", "#2", "#3", "#4");
    }

    @Test(expected = RuntimeException.class, timeout = 5_000)
    public void testErrorAction() {
        reactorSerializedInvoker.submit(Mono.error(new RuntimeException("Simulated error"))).block();
    }

    @Test(timeout = 5_000)
    public void testQueueingTimeOut() {
        List<String> result = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        reactorSerializedInvoker.submit(Mono.delay(Duration.ofHours(1)).timeout(QUEUEING_TIMEOUT).map(tick -> "First")).subscribe(
                result::add,
                errorRef::set
        );
        reactorSerializedInvoker.submit(Mono.just("Second")).subscribe(result::add, errorRef::set);

        await().until(() -> result.size() == 1 && errorRef.get() != null);
        assertThat(result.get(0)).isEqualTo("Second");
    }

    @Test(timeout = 5_000)
    public void testExcessiveRunningTime() {
        AtomicReference<Object> resultRef = new AtomicReference<>();
        Disposable disposable = reactorSerializedInvoker.submit(Mono.delay(Duration.ofHours(1)).map(tick -> "First")).subscribe(
                resultRef::set,
                resultRef::set
        );
        await().until(disposable::isDisposed);
        assertThat(resultRef.get()).isInstanceOf(TimeoutException.class);
    }

    @Test(timeout = 5_000)
    public void testCancellation() {
        AtomicReference<Object> resultRef = new AtomicReference<>();
        Disposable disposable = reactorSerializedInvoker.submit(Mono.delay(Duration.ofHours(1)).map(tick -> "First"))
                .doOnCancel(() -> resultRef.set("CANCELLED"))
                .subscribe(
                        resultRef::set,
                        resultRef::set
                );
        disposable.dispose();
        await().until(disposable::isDisposed);
        assertThat(resultRef.get()).isEqualTo("CANCELLED");
    }

    @Test(timeout = 5_000)
    public void testShutdown() {
        List<Disposable> disposables = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            int index = i;
            Disposable disposable = reactorSerializedInvoker.submit(Mono.delay(Duration.ofHours(1)).map(tick -> "#" + index))
                    .doFinally(signalType -> errors.incrementAndGet())
                    .subscribe(
                            next -> {
                            },
                            e -> {
                            }
                    );
            disposables.add(disposable);
        }
        reactorSerializedInvoker.shutdown(Duration.ofSeconds(5));
        await().until(() -> disposables.stream().map(Disposable::isDisposed).reduce(true, (a, b) -> a && b));
        assertThat(errors.get()).isEqualTo(5);
    }
}