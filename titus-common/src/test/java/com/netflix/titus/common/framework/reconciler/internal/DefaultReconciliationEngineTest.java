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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelAction;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.framework.reconciler.internal.SimpleReconcilerEvent.EventType;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultReconciliationEngineTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final Map<Object, Comparator<EntityHolder>> indexComparators = ImmutableMap.<Object, Comparator<EntityHolder>>builder()
            .put("ascending", Comparator.comparing(EntityHolder::getEntity))
            .put("descending", Comparator.<EntityHolder, String>comparing(EntityHolder::getEntity).reversed())
            .build();

    private final DefaultReconciliationEngine<SimpleReconcilerEvent> engine = new DefaultReconciliationEngine<>(
            EntityHolder.newRoot("myRoot", "rootInitial"),
            true,
            this::difference,
            indexComparators,
            new SimpleReconcilerEventFactory(),
            changeAction -> Collections.emptyList(),
            event -> Collections.emptyList(),
            TitusRuntimes.test(testScheduler)
    );

    private final ExtTestSubscriber<SimpleReconcilerEvent> eventSubscriber = new ExtTestSubscriber<>();

    private final Queue<List<ChangeAction>> runtimeReconcileActions = new LinkedBlockingQueue<>();

    @Before
    public void setUp() {
        engine.events().cast(SimpleReconcilerEvent.class).subscribe(eventSubscriber);
    }

    @Test
    public void testReferenceModelChange() {
        ExtTestSubscriber<Void> testSubscriber = new ExtTestSubscriber<>();
        engine.changeReferenceModel(new SlowChangeAction()).subscribe(testSubscriber);

        // Trigger change event
        assertThat(engine.applyModelUpdates()).isFalse();
        engine.emitEvents();
        assertThat(engine.triggerActions()).isTrue();
        testSubscriber.assertOpen();
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ModelInitial);
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ChangeRequest);

        // Move time, and verify that model is updated
        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        engine.closeFinishedTransactions();
        assertThat(engine.triggerActions()).isFalse();

        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ModelUpdated);
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.Changed);
        assertThat(testSubscriber.isUnsubscribed()).isTrue();
    }

    @Test
    public void testChildAddRemove() {
        addChild("child1");
        assertThat(engine.getReferenceView().getId()).isEqualTo("myRoot");
        assertThat(engine.getReferenceView().getChildren()).hasSize(1);

        removeChild("child1");
        assertThat(engine.getReferenceView().getId()).isEqualTo("myRoot");
        assertThat(engine.getReferenceView().getChildren()).isEmpty();
    }

    @Test
    public void testReferenceModelChangeFailure() {
        RuntimeException failure = new RuntimeException("simulated ChangeAction error");
        testFailingChangeAction(() -> Observable.error(failure), failure);
    }

    @Test
    public void testReferenceModelChangeWithEscapedException() {
        RuntimeException failure = new RuntimeException("Escaped ChangeAction exception");
        testFailingChangeAction(() -> {
            throw failure;
        }, failure);
    }

    @Test
    public void testReferenceModelChangeFailureDuringModelUpdate() {
        RuntimeException failure = new RuntimeException("ModelUpdate exception");
        testFailingChangeAction(() -> Observable.just(singletonList(ModelActionHolder.reference(rootHolder -> {
            throw failure;
        }))), failure);
    }

    private void testFailingChangeAction(ChangeAction failingChangeAction, Throwable expectedError) {
        ExtTestSubscriber<Void> testSubscriber = new ExtTestSubscriber<>();
        engine.changeReferenceModel(failingChangeAction).subscribe(testSubscriber);

        // Consume initial events
        engine.emitEvents();
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ModelInitial);
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ChangeRequest);

        // Trigger failing action
        engine.triggerActions();
        engine.applyModelUpdates();
        engine.emitEvents();

        SimpleReconcilerEvent changeErrorEvent = eventSubscriber.takeNext();
        assertThat(changeErrorEvent.getEventType()).isIn(EventType.ChangeError, EventType.ModelUpdateError);
        assertThat(changeErrorEvent.getError()).contains(expectedError);

        assertThat(eventSubscriber.takeNext()).isNull();
        assertThat(engine.closeFinishedTransactions()).isTrue();

        testSubscriber.assertOnError(expectedError);
    }

    @Test
    public void testParallelExecutionOfNonOverlappingTasks() {
        addChild("child1");
        addChild("child2");

        ExtTestSubscriber<Void> child1Subscriber = new ExtTestSubscriber<>();
        ExtTestSubscriber<Void> child2Subscriber = new ExtTestSubscriber<>();
        engine.changeReferenceModel(new UpdateChildAction("child1", "update1"), "child1").subscribe(child1Subscriber);
        engine.changeReferenceModel(new UpdateChildAction("child2", "update2"), "child2").subscribe(child2Subscriber);

        engine.triggerActions();
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        assertThat(engine.closeFinishedTransactions()).isTrue();

        child1Subscriber.assertOnCompleted();
        child2Subscriber.assertOnCompleted();

        assertThat(engine.getReferenceView().findChildById("child1").get().<String>getEntity()).isEqualTo("update1");
        assertThat(engine.getReferenceView().findChildById("child2").get().<String>getEntity()).isEqualTo("update2");
    }

    @Test
    public void testParallelExecutionOfNonOverlappingTasksWithOneTaskFailing() {
        addChild("child1");
        addChild("child2");

        ExtTestSubscriber<Void> child1Subscriber = new ExtTestSubscriber<>();
        ExtTestSubscriber<Void> child2Subscriber = new ExtTestSubscriber<>();
        engine.changeReferenceModel(new UpdateChildAction("child1", "update1"), "child1").subscribe(child1Subscriber);

        RuntimeException failure = new RuntimeException("ModelUpdate exception");
        engine.changeReferenceModel(() -> {
            throw failure;
        }, "child2").subscribe(child2Subscriber);

        engine.triggerActions();
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        assertThat(engine.closeFinishedTransactions()).isTrue();

        child1Subscriber.assertOnCompleted();
        child2Subscriber.assertOnError(failure);

        assertThat(engine.getReferenceView().findChildById("child1").get().<String>getEntity()).isEqualTo("update1");
        assertThat(engine.getReferenceView().findChildById("child2").get().<String>getEntity()).isEqualTo("child2");
    }

    @Test
    public void testOverlappingTasksAreExecutedSequentially() {
        addChild("child1");
        addChild("child2");

        ExtTestSubscriber<Void> child1Subscriber = new ExtTestSubscriber<>();
        ExtTestSubscriber<Void> rootSubscriber = new ExtTestSubscriber<>();
        ExtTestSubscriber<Void> child2Subscriber = new ExtTestSubscriber<>();
        engine.changeReferenceModel(new UpdateChildAction("child1", "update1"), "child1").subscribe(child1Subscriber);
        engine.changeReferenceModel(new RootChangeAction("rootUpdate")).subscribe(rootSubscriber);
        engine.changeReferenceModel(new UpdateChildAction("child2", "update2"), "child2").subscribe(child2Subscriber);

        // Child 1
        engine.triggerActions();
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        assertThat(engine.closeFinishedTransactions()).isTrue();

        child1Subscriber.assertOnCompleted();
        rootSubscriber.assertOpen();

        assertThat(engine.getReferenceView().findChildById("child1").get().<String>getEntity()).isEqualTo("update1");
        assertThat(engine.getReferenceView().<String>getEntity()).isEqualTo("rootInitial");
        assertThat(engine.getReferenceView().findChildById("child2").get().<String>getEntity()).isEqualTo("child2");

        // Root
        engine.triggerActions();
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        assertThat(engine.closeFinishedTransactions()).isTrue();

        rootSubscriber.assertOnCompleted();
        child2Subscriber.assertOpen();

        assertThat(engine.getReferenceView().<String>getEntity()).isEqualTo("rootUpdate");
        assertThat(engine.getReferenceView().findChildById("child2").get().<String>getEntity()).isEqualTo("child2");

        // Child 2
        engine.triggerActions();
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        assertThat(engine.closeFinishedTransactions()).isTrue();

        child2Subscriber.assertOnCompleted();

        assertThat(engine.getReferenceView().findChildById("child2").get().<String>getEntity()).isEqualTo("update2");
    }

    @Test
    public void testReconciliationActions() {
        Subscription setupSubscription = engine.changeReferenceModel(new RootChangeAction("rootValue")).subscribe();
        assertThat(engine.applyModelUpdates()).isFalse();
        assertThat(engine.triggerActions()).isTrue();

        runtimeReconcileActions.add(singletonList(new SlowChangeAction()));

        // Complete first change action
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        engine.closeFinishedTransactions();
        assertThat(setupSubscription.isUnsubscribed()).isTrue();

        // Create the reconciler action
        assertThat(engine.triggerActions()).isTrue();
        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);

        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        engine.closeFinishedTransactions();
        assertThat(engine.triggerActions()).isFalse();

        List<EventType> emittedEvents = eventSubscriber.takeNext(7).stream().map(SimpleReconcilerEvent::getEventType).collect(Collectors.toList());
        assertThat(emittedEvents).contains(
                // From root setup
                EventType.ChangeRequest,
                EventType.ModelUpdated,
                EventType.ModelUpdated,
                EventType.Changed,

                // From reconciler
                EventType.ChangeRequest,
                EventType.ModelUpdated,
                EventType.Changed
        );
    }

    @Test
    public void testChangeActionCancellation() {
        SlowChangeAction action = new SlowChangeAction();
        Subscription subscription = engine.changeReferenceModel(action).subscribe();
        engine.triggerActions();

        assertThat(action.unsubscribed).isFalse();
        subscription.unsubscribe();
        assertThat(action.unsubscribed).isTrue();
    }

    @Test
    public void testIndexes() {
        addChild("child1");
        addChild("child2");

        assertThat(engine.orderedView("ascending").stream().map(EntityHolder::getEntity)).containsExactly("child1", "child2");
        assertThat(engine.orderedView("descending").stream().map(EntityHolder::getEntity)).containsExactly("child2", "child1");
    }

    @Test
    public void testEventStreamConsumersThrowingException() {
        RuntimeException failure = new RuntimeException("Simulated event handler error");
        engine.events().subscribe(
                event -> {
                    throw failure;
                },
                e -> assertThat(e).isEqualTo(failure));
        ExtTestSubscriber<Void> rootSubscriber = new ExtTestSubscriber<>();
        engine.changeReferenceModel(new RootChangeAction("rootUpdate")).subscribe(rootSubscriber);

        engine.emitEvents();
        assertThat(engine.triggerActions()).isTrue();
        assertThat(engine.applyModelUpdates()).isTrue();
        engine.emitEvents();
        assertThat(engine.closeFinishedTransactions()).isTrue();
    }

    private void addChild(String childId) {
        engine.changeReferenceModel(new AddChildAction(childId)).subscribe();
        engine.triggerActions();

        assertThat(engine.applyModelUpdates()).isTrue();

        engine.emitEvents();
        engine.closeFinishedTransactions();
    }

    private void removeChild(String childId) {
        engine.changeReferenceModel(new RemoveChildAction(childId)).subscribe();
        engine.triggerActions();

        assertThat(engine.applyModelUpdates()).isTrue();

        engine.emitEvents();
        engine.closeFinishedTransactions();
    }

    private List<ChangeAction> difference(ReconciliationEngine<SimpleReconcilerEvent> engine) {
        List<ChangeAction> next = runtimeReconcileActions.poll();
        return next == null ? Collections.emptyList() : next;
    }

    class RootChangeAction implements ChangeAction {

        private final String rootValue;

        RootChangeAction(String rootValue) {
            this.rootValue = rootValue;
        }

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            return Observable.just(ModelActionHolder.referenceAndRunning(new SimpleModelUpdateAction(EntityHolder.newRoot("root#0", rootValue), true)));
        }
    }

    class SlowChangeAction implements ChangeAction {

        private volatile boolean unsubscribed;

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            return Observable.timer(1, TimeUnit.SECONDS, testScheduler).map(tick -> {
                        ModelAction updateAction = new SimpleModelUpdateAction(EntityHolder.newRoot("root#0", "ROOT"), true);
                        return ModelActionHolder.referenceList(updateAction);
                    }
            ).doOnUnsubscribe(() -> unsubscribed = true);
        }
    }

    class AddChildAction implements ChangeAction {

        private final String childId;

        AddChildAction(String childId) {
            this.childId = childId;
        }

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            SimpleModelUpdateAction updateAction = new SimpleModelUpdateAction(EntityHolder.newRoot(childId, childId), false);
            return Observable.just(ModelActionHolder.referenceList(updateAction));
        }
    }

    class RemoveChildAction implements ChangeAction {

        private final String childId;

        RemoveChildAction(String childId) {
            this.childId = childId;
        }

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            ModelAction updateAction = rootHolder -> {
                Pair<EntityHolder, Optional<EntityHolder>> result = rootHolder.removeChild(childId);
                return result.getRight().map(removed -> Pair.of(result.getLeft(), removed));
            };
            return Observable.just(ModelActionHolder.referenceList(updateAction));
        }
    }

    class UpdateChildAction implements ChangeAction {

        private final String childId;
        private final String value;

        UpdateChildAction(String childId, String value) {
            this.childId = childId;
            this.value = value;
        }

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            SimpleModelUpdateAction updateAction = new SimpleModelUpdateAction(EntityHolder.newRoot(childId, value), false);
            return Observable.just(ModelActionHolder.referenceList(updateAction));
        }
    }
}