/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconcilerEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.After;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DefaultManyReconcilerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final Scheduler reconcilerScheduler = Schedulers.newSingle("reconciler");
    private final Scheduler notificationScheduler = Schedulers.newSingle("notification");

    private DefaultManyReconciler<String> reconciler;
    private TitusRxSubscriber<List<SimpleReconcilerEvent<String>>> changesSubscriber;

    @After
    public void tearDown() {
        ReactorExt.safeDispose(changesSubscriber);
        Evaluators.acceptNotNull(reconciler, r -> r.close().block(TIMEOUT));
        ReactorExt.safeDispose(reconcilerScheduler, notificationScheduler);
    }

    @Test
    public void testExternalAction() throws InterruptedException {
        newReconcilerWithRegistrations(data -> Collections.emptyList());

        // Register
        reconciler.add("r1", "a").block();
        assertThat(reconciler.findById("r1")).contains("a");
        expectAddEvent("r1", "a");

        // Make changes
        assertThat(reconciler.apply("r1", current -> Mono.just(current.toUpperCase())).block()).isEqualTo("A");
        expectUpdateEvent("r1", "A");

        // Close
        reconciler.remove("r1").block();
        assertThat(reconciler.findById("r1")).isEmpty();
        expectRemovedEvent("r1", "A");
    }

    @Test
    public void testExternalActionCancel() throws InterruptedException {
        newReconcilerWithRegistrations(data -> Collections.emptyList(), "r1", "a");

        CountDownLatch subscribeLatch = new CountDownLatch(1);
        AtomicBoolean canceled = new AtomicBoolean();
        Mono<String> action = Mono.<String>never()
                .doOnSubscribe(s -> subscribeLatch.countDown())
                .doOnCancel(() -> canceled.set(true));

        Disposable disposable = reconciler.apply("r1", data -> action).subscribe();
        subscribeLatch.await();

        assertThat(disposable.isDisposed()).isFalse();
        assertThat(canceled.get()).isFalse();

        disposable.dispose();
        await().until(canceled::get);

        assertThat(changesSubscriber.takeNext()).isNull();

        // Now run regular action
        assertThat(reconciler.apply("r1", anything -> Mono.just("Regular")).block(TIMEOUT)).isEqualTo("Regular");
    }

    @Test
    public void testExternalActionMonoError() throws InterruptedException {
        newReconcilerWithRegistrations(current -> Collections.emptyList(), "r1", "a");

        try {
            reconciler.apply("r1", anything -> Mono.error(new RuntimeException("simulated error"))).block();
            fail("Expected error");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RuntimeException.class);
            assertThat(e.getMessage()).isEqualTo("simulated error");
        }

        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test
    public void testExternalActionFunctionError() throws InterruptedException {
        newReconcilerWithRegistrations(current -> Collections.emptyList(), "r1", "a");

        try {
            reconciler.apply("r1", c -> {
                throw new RuntimeException("simulated error");
            }).block();
            fail("Expected error");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RuntimeException.class);
            assertThat(e.getMessage()).isEqualTo("simulated error");
        }

        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test
    public void testReconcilerAction() throws InterruptedException {
        newReconcilerWithRegistrations(
                dataBefore -> dataBefore.equals("a") ? Collections.singletonList(Mono.just(dataAfter -> "b")) : Collections.emptyList(),
                "r1", "a"
        );
        await().until(() -> reconciler.findById("r1").orElse("").equals("b"));
    }

    @Test
    public void testReconcilerActionMonoError() throws InterruptedException {
        AtomicBoolean failedRef = new AtomicBoolean();
        newReconcilerWithRegistrations(
                data -> Collections.singletonList(failedRef.getAndSet(true)
                        ? Mono.just(v -> "A")
                        : Mono.error(new RuntimeException("simulated error"))
                ),
                "r1", "a"
        );
        expectUpdateEvent("r1", "A");
    }

    @Test
    public void testReconcilerActionFunctionError() throws InterruptedException {
        AtomicBoolean failedRef = new AtomicBoolean();
        newReconcilerWithRegistrations(
                dataBefore -> Collections.singletonList(Mono.just(dataAfter -> {
                    if (failedRef.getAndSet(true)) {
                        return "A";
                    }
                    throw new RuntimeException("simulated error");
                })),
                "r1", "a"
        );
        expectUpdateEvent("r1", "A");
    }

    @Test
    public void testReconcilerCloseWithRunningExternalAction() throws InterruptedException {
        newReconcilerWithRegistrations(
                current -> Collections.emptyList(),
                "r1", "a",
                "r2", "b"
        );

        TitusRxSubscriber<String> subscriber1 = new TitusRxSubscriber<>();
        TitusRxSubscriber<String> subscriber2 = new TitusRxSubscriber<>();
        reconciler.apply("r1", c -> Mono.never()).subscribe(subscriber1);
        reconciler.apply("r2", c -> Mono.never()).subscribe(subscriber2);

        assertThat(subscriber1.isOpen()).isTrue();
        assertThat(subscriber2.isOpen()).isTrue();

        reconciler.close().subscribe();

        await().until(() -> !subscriber1.isOpen());
        await().until(() -> !subscriber2.isOpen());
        assertThat(subscriber1.getError().getMessage()).isEqualTo("cancelled");
        assertThat(subscriber2.getError().getMessage()).isEqualTo("cancelled");

        await().until(() -> !changesSubscriber.isOpen());
    }

    @Test
    public void testReconcilerCloseWithRunningReconciliationAction() throws InterruptedException {
        // Start reconciliation action first
        CountDownLatch ready = new CountDownLatch(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        Mono<Function<String, String>> action = Mono.<Function<String, String>>never()
                .doOnSubscribe(s -> ready.countDown())
                .doOnCancel(() -> cancelled.set(true));
        newReconcilerWithRegistrations(current -> Collections.singletonList(action), "r1", "a");

        ready.await();

        TitusRxSubscriber<String> subscriber = new TitusRxSubscriber<>();
        reconciler.apply("r1", c -> Mono.never()).subscribe(subscriber);
        assertThat(subscriber.isOpen()).isTrue();

        reconciler.close().subscribe();

        await().until(() -> cancelled.get() && !subscriber.isOpen());
        assertThat(subscriber.getError().getMessage()).isEqualTo("cancelled");

        await().until(() -> !changesSubscriber.isOpen());
    }

    private void newReconcilerWithRegistrations(Function<String, List<Mono<Function<String, String>>>> reconcilerActionsProvider,
                                                String... idAndInitialValuePairs) throws InterruptedException {
        Preconditions.checkArgument((idAndInitialValuePairs.length % 2) == 0, "Expected pairs of id/value");

        reconciler = new DefaultManyReconciler<>(
                Duration.ofMillis(1),
                Duration.ofMillis(2),
                reconcilerActionsProvider,
                reconcilerScheduler,
                notificationScheduler,
                titusRuntime
        );

        for (int i = 0; i < idAndInitialValuePairs.length; i += 2) {
            reconciler.add(idAndInitialValuePairs[i], idAndInitialValuePairs[i + 1]).block(TIMEOUT);
        }

        reconciler.changes().subscribe(changesSubscriber = new TitusRxSubscriber<>());
        List<SimpleReconcilerEvent<String>> snapshot = changesSubscriber.takeNext(TIMEOUT);
        assertThat(snapshot).hasSize(idAndInitialValuePairs.length / 2);
    }

    private void expectAddEvent(String id, String data) throws InterruptedException {
        expectEvent(id, data, SimpleReconcilerEvent.Kind.Added);
    }

    private void expectUpdateEvent(String id, String data) throws InterruptedException {
        expectEvent(id, data, SimpleReconcilerEvent.Kind.Updated);
    }

    private void expectRemovedEvent(String id, String data) throws InterruptedException {
        expectEvent(id, data, SimpleReconcilerEvent.Kind.Removed);
    }

    private void expectEvent(String id, String data, SimpleReconcilerEvent.Kind kind) throws InterruptedException {
        List<SimpleReconcilerEvent<String>> addEvent = changesSubscriber.takeNext(TIMEOUT);
        assertThat(addEvent).hasSize(1);
        assertThat(addEvent.get(0).getKind()).isEqualTo(kind);
        assertThat(addEvent.get(0).getId()).isEqualTo(id);
        assertThat(addEvent.get(0).getData()).isEqualTo(data);
    }
}