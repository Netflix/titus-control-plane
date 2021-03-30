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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.MultiEngineChangeAction;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.framework.reconciler.internal.SimpleReconcilerEvent.EventType;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static com.jayway.awaitility.Awaitility.await;
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

    private final Function<EntityHolder, InternalReconciliationEngine<SimpleReconcilerEvent>> engineFactory = mock(Function.class);

    private final InternalReconciliationEngine engine1 = mock(InternalReconciliationEngine.class);
    private final InternalReconciliationEngine engine2 = mock(InternalReconciliationEngine.class);

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
            Optional.of(testScheduler)
    );

    @Before
    public void setUp() {
        framework.start();
        when(engineFactory.apply(any())).thenReturn(engine1, engine2);
        when(engine1.triggerActions()).thenReturn(true);
        when(engine1.getReferenceView()).thenReturn(EntityHolder.newRoot("myRoot1", "myEntity1"));
        when(engine1.events()).thenReturn(engine1Events.asObservable());
        when(engine1.changeReferenceModel(any())).thenAnswer(invocation -> {
            ChangeAction changeAction = invocation.getArgument(0);
            return changeAction.apply().ignoreElements().cast(Void.class);
        });
        when(engine2.triggerActions()).thenReturn(true);
        when(engine2.getReferenceView()).thenReturn(EntityHolder.newRoot("myRoot2", "myEntity2"));
        when(engine2.events()).thenReturn(engine2Events.asObservable());
        when(engine2.changeReferenceModel(any())).thenAnswer(invocation -> {
            ChangeAction changeAction = invocation.getArgument(0);
            return changeAction.apply().ignoreElements().cast(Void.class);
        });
    }

    @After
    public void tearDown() {
        framework.stop(STOP_TIMEOUT_MS);
    }

    @Test
    public void testBootstrapEngineInitialization() {
        InternalReconciliationEngine<SimpleReconcilerEvent> bootstrapEngine = mock(InternalReconciliationEngine.class);
        PublishSubject<SimpleReconcilerEvent> eventSubject = PublishSubject.create();
        when(bootstrapEngine.events()).thenReturn(eventSubject);
        when(bootstrapEngine.triggerActions()).thenReturn(true);
        when(bootstrapEngine.getReferenceView()).thenReturn(EntityHolder.newRoot("myRoot1", "myEntity1"));

        DefaultReconciliationFramework<SimpleReconcilerEvent> framework = new DefaultReconciliationFramework<>(
                Collections.singletonList(bootstrapEngine),
                engineFactory,
                IDLE_TIMEOUT_MS,
                ACTIVE_TIMEOUT_MS,
                indexComparators,
                new DefaultRegistry(),
                Optional.of(testScheduler)
        );
        framework.start();
        AssertableSubscriber<SimpleReconcilerEvent> eventSubscriber = framework.events().test();

        eventSubject.onNext(new SimpleReconcilerEvent(EventType.Changed, "test", Optional.empty()));
        await().timeout(30, TimeUnit.SECONDS).until(() -> eventSubscriber.assertValueCount(1));
    }

    @Test
    public void testEngineAddRemove() {
        ExtTestSubscriber<ReconciliationEngine> addSubscriber = new ExtTestSubscriber<>();
        framework.newEngine(EntityHolder.newRoot("myRoot1", "myEntity")).subscribe(
                next -> {
                    // When subscriber gets notified, the model must be already updated
                    assertThat(framework.findEngineByRootId("myRoot1")).isPresent();
                    addSubscriber.onNext(next);
                },
                addSubscriber::onError,
                addSubscriber::onCompleted
        );
        testScheduler.triggerActions();

        InternalReconciliationEngine engine = (InternalReconciliationEngine) addSubscriber.takeNext();
        verify(engine, times(1)).emitEvents();
        verify(engine, times(1)).closeFinishedTransactions();
        verify(engine, times(1)).triggerActions();

        // Model updates are performed only on the second iteration
        testScheduler.advanceTimeBy(ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        verify(engine, times(1)).applyModelUpdates();

        // Now remove the engine
        ExtTestSubscriber<Void> removeSubscriber = new ExtTestSubscriber<>();
        framework.removeEngine(engine).subscribe(
                () -> {
                    // When subscriber gets notified, the model must be already updated
                    assertThat(framework.findEngineByRootId("myRoot1")).isNotPresent();
                    addSubscriber.onCompleted();
                },
                removeSubscriber::onError
        );
        testScheduler.advanceTimeBy(IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        verify(engine, times(1)).triggerActions();
    }

    @Test
    public void testMultiEngineChangeAction() {
        EntityHolder root1 = EntityHolder.newRoot("myRoot1", "myEntity1");
        EntityHolder root2 = EntityHolder.newRoot("myRoot2", "myEntity2");

        framework.newEngine(root1).subscribe();
        framework.newEngine(root2).subscribe();
        testScheduler.triggerActions();

        MultiEngineChangeAction multiEngineChangeAction = () -> Observable.just(ImmutableMap.of(
                "myRoot1", ModelActionHolder.allModels(new SimpleModelUpdateAction(EntityHolder.newRoot("myRoot1", "myEntity1#v2"), true)),
                "myRoot2", ModelActionHolder.allModels(new SimpleModelUpdateAction(EntityHolder.newRoot("myRoot2", "myEntity2#v2"), true))
        ));
        Map<String, List<ModelActionHolder>> holders = new HashMap<>();
        Observable<Void> multiChangeObservable = framework.changeReferenceModel(
                multiEngineChangeAction,
                (id, modelUpdates) -> {
                    ChangeAction changeAction = () -> modelUpdates.doOnNext(next -> holders.put(id, next));
                    return changeAction;
                },
                "myRoot1", "myRoot2"
        );

        verify(engine1, times(0)).changeReferenceModel(any());
        verify(engine2, times(0)).changeReferenceModel(any());

        ExtTestSubscriber<Void> multiChangeSubscriber = new ExtTestSubscriber<>();
        multiChangeObservable.subscribe(multiChangeSubscriber);
        assertThat(multiChangeSubscriber.isUnsubscribed()).isTrue();

        verify(engine1, times(1)).changeReferenceModel(any());
        verify(engine2, times(1)).changeReferenceModel(any());

        // one action per view (Running, Store, Reference)
        assertThat(holders.get("myRoot1")).hasSize(3);
        SimpleModelUpdateAction modelAction1 = (SimpleModelUpdateAction) holders.get("myRoot1").get(0).getAction();
        assertThat((String) modelAction1.getEntityHolder().getEntity()).isEqualTo("myEntity1#v2");

        // one action per view (Running, Store, Reference)
        assertThat(holders.get("myRoot2")).hasSize(3);
        SimpleModelUpdateAction modelAction2 = (SimpleModelUpdateAction) holders.get("myRoot2").get(0).getAction();
        assertThat((String) modelAction2.getEntityHolder().getEntity()).isEqualTo("myEntity2#v2");
    }

    @Test
    public void testMultiEngineChangeActionWithInvalidEngineId() {
        EntityHolder root1 = EntityHolder.newRoot("myRoot1", "myEntity1");

        framework.newEngine(root1).subscribe();
        testScheduler.triggerActions();

        Observable<Void> multiChangeObservable = framework.changeReferenceModel(
                // Keep anonymous class instead of lambda for readability
                new MultiEngineChangeAction() {
                    @Override
                    public Observable<Map<String, List<ModelActionHolder>>> apply() {
                        return Observable.error(new IllegalStateException("invocation not expected"));
                    }
                },
                // Keep anonymous class instead of lambda for readability
                (id, modelUpdates) -> new ChangeAction() {
                    @Override
                    public Observable<List<ModelActionHolder>> apply() {
                        return Observable.error(new IllegalStateException("invocation not expected"));
                    }
                },
                "myRoot1",
                "badRootId"
        );

        ExtTestSubscriber<Void> multiChangeSubscriber = new ExtTestSubscriber<>();
        multiChangeObservable.subscribe(multiChangeSubscriber);
        assertThat(multiChangeSubscriber.isError()).isTrue();
        assertThat(multiChangeSubscriber.getError().getMessage()).contains("badRootId");
    }

    @Test
    public void testFailingMultiEngineChangeAction() {
        EntityHolder root1 = EntityHolder.newRoot("myRoot1", "myEntity1");
        EntityHolder root2 = EntityHolder.newRoot("myRoot2", "myEntity2");

        framework.newEngine(root1).subscribe();
        framework.newEngine(root2).subscribe();
        testScheduler.triggerActions();

        Observable<Void> multiChangeObservable = framework.changeReferenceModel(
                // Keep anonymous class instead of lambda for readability
                new MultiEngineChangeAction() {
                    @Override
                    public Observable<Map<String, List<ModelActionHolder>>> apply() {
                        return Observable.error(new RuntimeException("simulated error"));
                    }
                },
                // Keep anonymous class instead of lambda for readability
                (id, modelUpdates) -> new ChangeAction() {
                    @Override
                    public Observable<List<ModelActionHolder>> apply() {
                        return modelUpdates;
                    }
                },
                "myRoot1",
                "myRoot2"
        );

        ExtTestSubscriber<Void> multiChangeSubscriber = new ExtTestSubscriber<>();
        multiChangeObservable.subscribe(multiChangeSubscriber);
        assertThat(multiChangeSubscriber.isError()).isTrue();
        String errorMessage = ExceptionExt.toMessageChain(multiChangeSubscriber.getError());
        assertThat(errorMessage).contains("simulated error");
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
    public void testEventsPublishing() throws Exception {
        framework.newEngine(EntityHolder.newRoot("myRoot1", "myEntity1")).subscribe();
        framework.newEngine(EntityHolder.newRoot("myRoot2", "myEntity2")).subscribe();
        testScheduler.triggerActions();

        ExtTestSubscriber<SimpleReconcilerEvent> eventSubscriber = new ExtTestSubscriber<>();
        framework.events().subscribe(eventSubscriber);

        engine1Events.onNext(newEvent("event1"));
        assertThat(eventSubscriber.takeNext(30, TimeUnit.SECONDS).getMessage()).isEqualTo("event1");
        engine1Events.onCompleted();
        assertThat(eventSubscriber.isUnsubscribed()).isFalse();

        engine2Events.onNext(newEvent("event2"));
        assertThat(eventSubscriber.takeNext(30, TimeUnit.SECONDS).getMessage()).isEqualTo("event2");
    }

    private SimpleReconcilerEvent newEvent(String message) {
        return new SimpleReconcilerEvent(EventType.Changed, message, Optional.empty());
    }
}
