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

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProvider;
import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconcilerEvent;
import com.netflix.titus.common.framework.simplereconciler.internal.provider.ActionProviderSelectorFactory;
import com.netflix.titus.common.framework.simplereconciler.internal.provider.SampleActionProviderCatalog;
import com.netflix.titus.common.framework.simplereconciler.internal.provider.SampleActionProviderCatalog.SampleProviderActionFunction;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.closeable.CloseableReference;
import com.netflix.titus.common.util.collections.index.IndexSetHolderBasic;
import com.netflix.titus.common.util.collections.index.IndexSpec;
import com.netflix.titus.common.util.collections.index.Indexes;
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

    private static final String INDEX_ID = "reversed";

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private DefaultManyReconciler<String> reconciler;
    private TitusRxSubscriber<List<SimpleReconcilerEvent<String>>> changesSubscriber;

    @After
    public void tearDown() {
        ReactorExt.safeDispose(changesSubscriber);
        Evaluators.acceptNotNull(reconciler, r -> r.close().block(TIMEOUT));
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

    /**
     * Internal reconciler runs periodically and emits events constantly. The event dispatcher does not do any
     * deduplication, so if run without any control, the subscriber would be getting different number of items
     * on each emit. To avoid that we instruct the reconciler here when to emit the items we want.
     */
    @Test
    public void testReconcilerActionMonoError() throws InterruptedException {
        AtomicInteger roundRef = new AtomicInteger(-1);
        newReconcilerWithRegistrations(
                dataBefore -> {
                    if (roundRef.get() == -1) {
                        return Collections.emptyList();
                    }
                    if (roundRef.getAndIncrement() == 0) {
                        return Collections.singletonList(Mono.error(new RuntimeException("simulated error")));
                    }
                    roundRef.set(-1);
                    return Collections.singletonList(Mono.just(v -> "A"));
                },
                "r1", "a"
        );
        roundRef.set(0);
        expectUpdateEvent("r1", "A");
    }

    /**
     * Check {@link #testReconcilerActionMonoError()} to understand the logic of this test.
     */
    @Test
    public void testReconcilerActionFunctionError() throws InterruptedException {
        AtomicInteger roundRef = new AtomicInteger(-1);
        newReconcilerWithRegistrations(
                dataBefore -> {
                    if (roundRef.get() == -1) {
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(Mono.just(dataAfter -> {
                        if (roundRef.getAndIncrement() == 0) {
                            throw new RuntimeException("simulated error");
                        }
                        roundRef.set(-1);
                        return "A";
                    }));
                },
                "r1", "a"
        );
        roundRef.set(0);
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

    @Test
    public void testActionProviderPriorities() throws InterruptedException {
        ReconcilerActionProvider<String> provider1 = SampleActionProviderCatalog.newInternalProvider("i1", 20);
        ReconcilerActionProvider<String> provider2 = SampleActionProviderCatalog.newInternalProvider("i2", 1_000, Duration.ZERO, Duration.ofMillis(10));
        ((SampleProviderActionFunction) provider1.getActionProvider()).enableEmit(true);
        ((SampleProviderActionFunction) provider2.getActionProvider()).enableEmit(true);

        ActionProviderSelectorFactory<String> selectorFactory = new ActionProviderSelectorFactory<>("test", Arrays.asList(
                new ReconcilerActionProvider<>(ReconcilerActionProviderPolicy.getDefaultExternalPolicy(), true, data -> Collections.emptyList()),
                provider1,
                provider2
        ), titusRuntime);
        newReconcilerWithRegistrations(selectorFactory, "a", "");
        await().timeout(com.jayway.awaitility.Duration.FOREVER).until(() -> reconciler.getAll().get("a").contains("i2"));
    }

    @Test
    public void testIndex() throws InterruptedException {
        newReconcilerWithRegistrations(data -> Collections.emptyList(), "i1", "i1_v1", "i2", "i2_v2");
        assertThat(reconciler.getIndexSet().getIndex(INDEX_ID).orderedList()).hasSize(2).containsSequence("i1_v1", "i2_v2");

        // Register
        reconciler.add("i3", "i3_v3").block();
        assertThat(reconciler.getIndexSet().getIndex(INDEX_ID).orderedList()).hasSize(3).containsSequence("i1_v1", "i2_v2", "i3_v3");

        // Make changes
        reconciler.apply("i3", current -> Mono.just("i3_v3a")).block();
        assertThat(reconciler.getIndexSet().getIndex(INDEX_ID).orderedList()).hasSize(3).containsSequence("i1_v1", "i2_v2", "i3_v3a");

        // Close
        reconciler.remove("i3").block();
        assertThat(reconciler.getIndexSet().getIndex(INDEX_ID).orderedList()).hasSize(2).containsSequence("i1_v1", "i2_v2");
    }

    private void newReconcilerWithRegistrations(Function<String, List<Mono<Function<String, String>>>> reconcilerActionsProvider,
                                                String... idAndInitialValuePairs) throws InterruptedException {
        ActionProviderSelectorFactory<String> selectorFactory = new ActionProviderSelectorFactory<>("test", Arrays.asList(
                new ReconcilerActionProvider<>(ReconcilerActionProviderPolicy.getDefaultExternalPolicy(), true, data -> Collections.emptyList()),
                new ReconcilerActionProvider<>(ReconcilerActionProviderPolicy.getDefaultInternalPolicy(), false, reconcilerActionsProvider)
        ), titusRuntime);
        newReconcilerWithRegistrations(selectorFactory, idAndInitialValuePairs);
    }

    private void newReconcilerWithRegistrations(ActionProviderSelectorFactory<String> selectorFactory,
                                                String... idAndInitialValuePairs) throws InterruptedException {

        Preconditions.checkArgument((idAndInitialValuePairs.length % 2) == 0, "Expected pairs of id/value");


        CloseableReference<Scheduler> reconcilerSchedulerRef = CloseableReference.referenceOf(Schedulers.newSingle("reconciler"), Scheduler::dispose);
        CloseableReference<Scheduler> notificationSchedulerRef = CloseableReference.referenceOf(Schedulers.newSingle("notification"), Scheduler::dispose);

        Function<String, String> keyExtractor = s -> s.substring(0, Math.min(s.length(), 2));
        reconciler = new DefaultManyReconciler<>(
                "junit",
                Duration.ofMillis(1),
                Duration.ofMillis(2),
                selectorFactory,
                reconcilerSchedulerRef,
                notificationSchedulerRef,
                new IndexSetHolderBasic<>(Indexes.<String, String>newBuilder()
                        .withIndex(INDEX_ID, IndexSpec.<String, String, String, String>newBuilder()
                                .withPrimaryKeyComparator(String::compareTo)
                                .withIndexKeyComparator(String::compareTo)
                                .withPrimaryKeyExtractor(keyExtractor)
                                .withIndexKeyExtractor(keyExtractor)
                                .withTransformer(Function.identity())
                                .build()
                        )
                        .build()
                ),
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