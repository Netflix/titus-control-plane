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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProvider;
import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy;
import com.netflix.titus.common.framework.simplereconciler.internal.provider.ActionProviderSelectorFactory;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.After;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DefaultOneOffReconcilerTest {

    private static final Duration QUICK_CYCLE = Duration.ofMillis(1);
    private static final Duration LONG_CYCLE = Duration.ofMillis(2);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final String INITIAL = "initial";
    private static final String RECONCILED = "Reconciled";

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private DefaultOneOffReconciler<String> reconciler;
    private TitusRxSubscriber<String> changesSubscriber;

    @After
    public void tearDown() {
        ReactorExt.safeDispose(changesSubscriber);
        Evaluators.acceptNotNull(reconciler, r -> r.close().block(TIMEOUT));
    }

    @Test(timeout = 30_000)
    public void testExternalAction() {
        newReconciler(current -> Collections.emptyList());

        assertThat(reconciler.apply(c -> Mono.just("update")).block()).isEqualTo("update");
        assertThat(changesSubscriber.takeNext()).isEqualTo("update");
        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test(timeout = 30_000)
    public void testExternalActionCancel() throws Exception {
        newReconciler(current -> Collections.emptyList());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean canceled = new AtomicBoolean();
        Mono<String> action = Mono.<String>never()
                .doOnSubscribe(s -> latch.countDown())
                .doOnCancel(() -> canceled.set(true));

        Disposable disposable = reconciler.apply(c -> action).subscribe();
        latch.await();

        assertThat(disposable.isDisposed()).isFalse();
        assertThat(canceled.get()).isFalse();

        disposable.dispose();
        await().until(canceled::get);

        assertThat(changesSubscriber.takeNext()).isNull();

        // Now run regular action
        assertThat(reconciler.apply(anything -> Mono.just("Regular")).block(TIMEOUT)).isEqualTo("Regular");
    }

    @Test(timeout = 30_000)
    public void testExternalActionMonoError() {
        newReconciler(current -> Collections.emptyList());

        try {
            reconciler.apply(anything -> Mono.error(new RuntimeException("simulated error"))).block();
            fail("Expected error");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RuntimeException.class);
            assertThat(e.getMessage()).isEqualTo("simulated error");
        }

        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test(timeout = 30_000)
    public void testExternalActionFunctionError() {
        newReconciler(current -> Collections.emptyList());

        try {
            reconciler.apply(c -> {
                throw new RuntimeException("simulated error");
            }).block();
            fail("Expected error");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RuntimeException.class);
            assertThat(e.getMessage()).isEqualTo("simulated error");
        }

        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test(timeout = 30_000)
    public void testReconcilerAction() throws InterruptedException {
        AtomicInteger idx = new AtomicInteger();
        newReconciler(current -> Collections.singletonList(Mono.just(v -> RECONCILED + idx.getAndIncrement())));
        String expected = RECONCILED + (idx.get() + 1);
        assertThat(changesSubscriber.takeUntil(expected::equals, TIMEOUT)).isNotNull();
    }

    @Test(timeout = 30_000)
    public void testReconcilerActionMonoError() throws InterruptedException {
        AtomicBoolean failedRef = new AtomicBoolean();
        newReconciler(current -> Collections.singletonList(failedRef.getAndSet(true)
                ? Mono.just(v -> RECONCILED) :
                Mono.error(new RuntimeException("simulated error"))
        ));
        assertThat(changesSubscriber.takeNext(TIMEOUT)).isEqualTo(RECONCILED);
    }

    @Test(timeout = 30_000)
    public void testReconcilerActionFunctionError() throws InterruptedException {
        AtomicBoolean failedRef = new AtomicBoolean();
        newReconciler(current -> Collections.singletonList(Mono.just(v -> {
            if (failedRef.getAndSet(true)) {
                return RECONCILED;
            }
            throw new RuntimeException("simulated error");
        })));
        assertThat(changesSubscriber.takeNext(TIMEOUT)).isEqualTo(RECONCILED);
    }

    @Test(timeout = 30_000)
    public void testReconcilerCloseWithRunningExternalAction() {
        newReconciler(current -> Collections.emptyList());

        TitusRxSubscriber<String> subscriber1 = new TitusRxSubscriber<>();
        TitusRxSubscriber<String> subscriber2 = new TitusRxSubscriber<>();
        reconciler.apply(c -> Mono.never()).subscribe(subscriber1);
        reconciler.apply(c -> Mono.never()).subscribe(subscriber2);

        assertThat(subscriber1.isOpen()).isTrue();
        assertThat(subscriber2.isOpen()).isTrue();

        reconciler.close().subscribe();

        await().until(() -> !subscriber1.isOpen());
        await().until(() -> !subscriber2.isOpen());
        assertThat(subscriber1.getError().getMessage()).isEqualTo("cancelled");
        assertThat(subscriber2.getError().getMessage()).isEqualTo("cancelled");

        await().until(() -> !changesSubscriber.isOpen());
    }

    @Test(timeout = 30_000)
    public void testReconcilerCloseWithRunningReconciliationAction() throws InterruptedException {
        // Start reconciliation action first
        CountDownLatch ready = new CountDownLatch(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        Mono<Function<String, String>> action = Mono.<Function<String, String>>never()
                .doOnSubscribe(s -> ready.countDown())
                .doOnCancel(() -> cancelled.set(true));
        newReconciler(current -> Collections.singletonList(action));

        ready.await();

        TitusRxSubscriber<String> subscriber = new TitusRxSubscriber<>();
        reconciler.apply(c -> Mono.never()).subscribe(subscriber);
        assertThat(subscriber.isOpen()).isTrue();

        reconciler.close().subscribe();

        await().until(() -> cancelled.get() && !subscriber.isOpen());
        assertThat(subscriber.getError().getMessage()).isEqualTo("cancelled");

        await().until(() -> !changesSubscriber.isOpen());
    }

    private void newReconciler(Function<String, List<Mono<Function<String, String>>>> reconcilerActionsProvider) {
        ActionProviderSelectorFactory<String> selectorFactory = new ActionProviderSelectorFactory<>("test", Arrays.asList(
                new ReconcilerActionProvider<>(ReconcilerActionProviderPolicy.getDefaultExternalPolicy(), true, data -> Collections.emptyList()),
                new ReconcilerActionProvider<>(ReconcilerActionProviderPolicy.getDefaultInternalPolicy(), false, reconcilerActionsProvider)
        ), titusRuntime);
        this.reconciler = new DefaultOneOffReconciler<>(
                "junit",
                INITIAL,
                QUICK_CYCLE,
                LONG_CYCLE,
                selectorFactory,
                Schedulers.parallel(),
                titusRuntime
        );

        this.changesSubscriber = new TitusRxSubscriber<>();
        reconciler.changes().subscribe(changesSubscriber);

        String event = changesSubscriber.takeNext();
        if (!event.equals(INITIAL) && !event.startsWith(RECONCILED)) {
            fail("Unexpected event: " + event);
        }
    }
}