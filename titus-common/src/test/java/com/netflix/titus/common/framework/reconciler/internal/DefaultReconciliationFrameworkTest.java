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

package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.framework.reconciler.internal.SimpleReconcilerEvent.EventType;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultReconciliationFrameworkTest {

    private static final long IDLE_TIMEOUT_MS = 100;
    private static final long ACTIVE_TIMEOUT_MS = 20;
    private static final int STOP_TIMEOUT_MS = 1_000;

    private final TestScheduler testScheduler = Schedulers.test();

    private final Function<EntityHolder, ReconciliationEngine<SimpleReconcilerEvent>> engineFactory = mock(Function.class);

    private final ReconciliationEngine engine1 = mock(ReconciliationEngine.class);
    private final ReconciliationEngine engine2 = mock(ReconciliationEngine.class);

    private final PublishSubject<SimpleReconcilerEvent> engine1Events = PublishSubject.create();
    private final PublishSubject<SimpleReconcilerEvent> engine2Events = PublishSubject.create();

    private final Map<Object, Comparator<EntityHolder>> indexComparators = ImmutableMap.<Object, Comparator<EntityHolder>>builder()
            .put("ascending", Comparator.comparing(EntityHolder::getEntity))
            .put("descending", Comparator.<EntityHolder, String>comparing(EntityHolder::getEntity).reversed())
            .build();

    private final DefaultReconciliationFramework<SimpleReconcilerEvent> framework = new DefaultReconciliationFramework<>(
            Collections.emptyList(),
            engineFactory,
            IDLE_TIMEOUT_MS,
            ACTIVE_TIMEOUT_MS,
            indexComparators,
            new DefaultRegistry(),
            testScheduler
    );

    @Before
    public void setUp() {
        framework.start();
        when(engineFactory.apply(any())).thenReturn(engine1, engine2);
        when(engine1.triggerEvents()).thenReturn(true);
        when(engine1.getReferenceView()).thenReturn(EntityHolder.newRoot("myRoot1", "myEntity1"));
        when(engine1.events()).thenReturn(engine1Events.asObservable());
        when(engine2.triggerEvents()).thenReturn(true);
        when(engine2.getReferenceView()).thenReturn(EntityHolder.newRoot("myRoot2", "myEntity2"));
        when(engine2.events()).thenReturn(engine2Events.asObservable());
    }

    @After
    public void tearDown() {
        framework.stop(STOP_TIMEOUT_MS);
    }

    @Test
    public void testBootstrapEngineInitialization() {
        ReconciliationEngine<SimpleReconcilerEvent> bootstrapEngine = mock(ReconciliationEngine.class);
        PublishSubject<SimpleReconcilerEvent> eventSubject = PublishSubject.create();
        when(bootstrapEngine.events()).thenReturn(eventSubject);
        when(bootstrapEngine.triggerEvents()).thenReturn(true);

        DefaultReconciliationFramework<SimpleReconcilerEvent> framework = new DefaultReconciliationFramework<>(
                Collections.singletonList(bootstrapEngine),
                engineFactory,
                IDLE_TIMEOUT_MS,
                ACTIVE_TIMEOUT_MS,
                indexComparators,
                new DefaultRegistry(),
                testScheduler
        );
        framework.start();
        AssertableSubscriber<SimpleReconcilerEvent> eventSubscriber = framework.events().test();

        eventSubject.onNext(new SimpleReconcilerEvent(EventType.Changed, "test", Optional.empty()));
        eventSubscriber.assertValueCount(1);
    }

    @Test
    public void testEngineLifecycle() {
        ExtTestSubscriber<ReconciliationEngine> addSubscriber = new ExtTestSubscriber<>();
        framework.newEngine(EntityHolder.newRoot("myRoot", "myEntity")).subscribe(addSubscriber);
        testScheduler.triggerActions();

        ReconciliationEngine engine = addSubscriber.takeNext();
        verify(engine, times(1)).triggerEvents();

        ExtTestSubscriber<Void> removeSubscriber = new ExtTestSubscriber<>();
        framework.removeEngine(engine).subscribe(removeSubscriber);
        testScheduler.advanceTimeBy(IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        verify(engine, times(1)).triggerEvents();
    }

    @Test
    public void testIndexes() {
        framework.newEngine(EntityHolder.newRoot("myRoot1", "myEntity1")).subscribe();
        framework.newEngine(EntityHolder.newRoot("myRoot2", "myEntity2")).subscribe();
        testScheduler.triggerActions();

        assertThat(framework.orderedView("ascending").stream().map(EntityHolder::getEntity)).containsExactly("myEntity1", "myEntity2");
        assertThat(framework.orderedView("descending").stream().map(EntityHolder::getEntity)).containsExactly("myEntity2", "myEntity1");
    }

    @Test
    public void testEventsPublishing() {
        framework.newEngine(EntityHolder.newRoot("myRoot1", "myEntity1")).subscribe();
        framework.newEngine(EntityHolder.newRoot("myRoot2", "myEntity2")).subscribe();
        testScheduler.triggerActions();

        ExtTestSubscriber<SimpleReconcilerEvent> eventSubscriber = new ExtTestSubscriber<>();
        framework.events().subscribe(eventSubscriber);

        engine1Events.onNext(newEvent("event1"));
        assertThat(eventSubscriber.takeNext().getMessage()).isEqualTo("event1");
        engine1Events.onCompleted();
        assertThat(eventSubscriber.isUnsubscribed()).isFalse();

        engine2Events.onNext(newEvent("event2"));
        assertThat(eventSubscriber.takeNext().getMessage()).isEqualTo("event2");
    }

    private SimpleReconcilerEvent newEvent(String message) {
        return new SimpleReconcilerEvent(EventType.Changed, message, Optional.empty());
    }
}