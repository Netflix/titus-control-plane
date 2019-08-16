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

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
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

public class DefaultSimpleReconciliationEngineTest {

    private static final Duration QUICK_CYCLE = Duration.ofMillis(1);
    private static final Duration LONG_CYCLE = Duration.ofMillis(2);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final String INITIAL = "initial";

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private DefaultSimpleReconciliationEngine<String> reconciliationEngine;
    private TitusRxSubscriber<String> changesSubscriber;

    @After
    public void tearDown() {
        ReactorExt.safeDispose(changesSubscriber);
        if (reconciliationEngine != null) {
            reconciliationEngine.close();
        }
    }

    @Test(timeout = 30_000)
    public void testExternalAction() {
        newReconciler(current -> Collections.emptyList());

        assertThat(reconciliationEngine.apply(Mono.just(c -> "update")).block()).isEqualTo("update");
        assertThat(changesSubscriber.takeNext()).isEqualTo("update");
        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test(timeout = 30_000)
    public void testExternalActionCancel() throws Exception {
        newReconciler(current -> Collections.emptyList());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean canceled = new AtomicBoolean();
        Mono<Function<String, String>> action = Mono.<Function<String, String>>never()
                .doOnSubscribe(s -> latch.countDown())
                .doOnCancel(() -> canceled.set(true));

        Disposable disposable = reconciliationEngine.apply(action).subscribe();
        latch.await();

        assertThat(disposable.isDisposed()).isFalse();
        assertThat(canceled.get()).isFalse();

        disposable.dispose();
        await().until(canceled::get);

        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test(timeout = 30_000)
    public void testExternalActionMonoError() {
        newReconciler(current -> Collections.emptyList());

        try {
            reconciliationEngine.apply(Mono.error(new RuntimeException("simulated error"))).block();
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
            reconciliationEngine.apply(Mono.just(c -> {
                throw new RuntimeException("simulated error");
            })).block();
            fail("Expected error");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RuntimeException.class);
            assertThat(e.getMessage()).isEqualTo("simulated error");
        }

        assertThat(changesSubscriber.takeNext()).isNull();
    }

    @Test(timeout = 30_000)
    public void testReconcilerAction() throws InterruptedException {
        newReconciler(current -> current.equals(INITIAL)
                ? Collections.singletonList(Mono.just(v -> "Reconciled"))
                : Collections.emptyList()
        );
        assertThat(changesSubscriber.takeNext(TIMEOUT)).isEqualTo("Reconciled");
    }

    @Test(timeout = 30_000)
    public void testReconcilerActionMonoError() throws InterruptedException {
        AtomicBoolean failedRef = new AtomicBoolean();
        newReconciler(current -> Collections.singletonList(failedRef.getAndSet(true)
                ? Mono.just(v -> "Reconciled") :
                Mono.error(new RuntimeException("simulated error"))
        ));
        assertThat(changesSubscriber.takeNext(TIMEOUT)).isEqualTo("Reconciled");
    }

    @Test(timeout = 30_000)
    public void testReconcilerActionFunctionError() throws InterruptedException {
        AtomicBoolean failedRef = new AtomicBoolean();
        newReconciler(current -> Collections.singletonList(Mono.just(v -> {
            if (failedRef.getAndSet(true)) {
                return "Reconciled";
            }
            throw new RuntimeException("simulated error");
        })));
        assertThat(changesSubscriber.takeNext(TIMEOUT)).isEqualTo("Reconciled");
    }

    @Test(timeout = 30_000)
    public void testReconcilerCloseWithRunningExternalAction() {
        newReconciler(current -> Collections.emptyList());

        TitusRxSubscriber<String> subscriber1 = new TitusRxSubscriber<>();
        TitusRxSubscriber<String> subscriber2 = new TitusRxSubscriber<>();
        reconciliationEngine.apply(Mono.never()).subscribe(subscriber1);
        reconciliationEngine.apply(Mono.never()).subscribe(subscriber2);

        assertThat(subscriber1.isOpen()).isTrue();
        assertThat(subscriber2.isOpen()).isTrue();

        reconciliationEngine.close();

        await().until(() -> !subscriber1.isOpen());
        await().until(() -> !subscriber2.isOpen());
        assertThat(subscriber1.getError().getMessage()).isEqualTo("Reconciliation engine closed");
        assertThat(subscriber2.getError().getMessage()).isEqualTo("Reconciliation engine closed");

        assertThat(changesSubscriber.isOpen()).isFalse();
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
        reconciliationEngine.apply(Mono.never()).subscribe(subscriber);
        assertThat(subscriber.isOpen()).isTrue();

        reconciliationEngine.close();

        await().until(() -> cancelled.get() && !subscriber.isOpen());
        assertThat(subscriber.getError().getMessage()).isEqualTo("Reconciliation engine closed");

        assertThat(changesSubscriber.isOpen()).isFalse();
    }

    private void newReconciler(Function<String, List<Mono<Function<String, String>>>> reconcilerActionsProvider) {
        this.reconciliationEngine = new DefaultSimpleReconciliationEngine<>(
                "junit",
                INITIAL,
                QUICK_CYCLE,
                LONG_CYCLE,
                reconcilerActionsProvider,
                Schedulers.parallel(),
                titusRuntime
        );

        this.changesSubscriber = new TitusRxSubscriber<>();
        reconciliationEngine.changes().subscribe(changesSubscriber);

        assertThat(changesSubscriber.takeNext()).isEqualTo("initial");
    }
}