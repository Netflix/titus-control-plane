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

package io.netflix.titus.common.framework.reconciler.internal;


import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelAction;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconcilerEvent.EventType;
import io.netflix.titus.common.framework.reconciler.ReconcilerUtil;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultReconciliationEngineTest {

    private static final Logger logger = LoggerFactory.getLogger(DefaultReconciliationEngineTest.class);

    private final TestScheduler testScheduler = Schedulers.test();

    private final Map<Object, Comparator<EntityHolder>> indexComparators = ImmutableMap.<Object, Comparator<EntityHolder>>builder()
            .put("ascending", Comparator.comparing(EntityHolder::getEntity))
            .put("descending", Comparator.<EntityHolder, String>comparing(EntityHolder::getEntity).reversed())
            .build();

    private final DefaultReconciliationEngine<String> engine = new DefaultReconciliationEngine(
            EntityHolder.newRoot("myRoot", "myEntity"), this::difference, indexComparators, new SimpleReconcilerEventFactory()
    );

    private final ExtTestSubscriber<SimpleReconcilerEvent> eventSubscriber = new ExtTestSubscriber<>();

    private final Queue<List<ChangeAction>> runtimeReconcileActions = new LinkedBlockingQueue<>();

    @Before
    public void setUp() throws Exception {
        engine.events().cast(SimpleReconcilerEvent.class).subscribe(eventSubscriber);
        ReconcilerUtil.logEvents(engine.events(), logger);
    }

    @Test
    public void testReferenceModelChange() throws Exception {
        ExtTestSubscriber<Void> testSubscriber = new ExtTestSubscriber<>();
        engine.changeReferenceModel(new SlowChangeAction()).subscribe(testSubscriber);

        // Trigger change event
        assertThat(engine.triggerEvents().isRunningChangeActions()).isTrue();
        testSubscriber.assertOpen();
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ChangeRequest);

        // Move time, and verify that model is updated
        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        assertThat(engine.triggerEvents().isRunningChangeActions()).isFalse();

        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ModelInitial);
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.Changed);
        assertThat(eventSubscriber.takeNext().getEventType()).isEqualTo(EventType.ModelUpdated);
        assertThat(testSubscriber.isUnsubscribed()).isTrue();
    }

    @Test
    public void testChildAddRemove() throws Exception {
        addChild("child1");
        assertThat(engine.getReferenceView().getId()).isEqualTo("myRoot");
        assertThat(engine.getReferenceView().getChildren()).hasSize(1);

        removeChild("child1");
        assertThat(engine.getReferenceView().getId()).isEqualTo("myRoot");
        assertThat(engine.getReferenceView().getChildren()).isEmpty();
    }

    @Test
    public void testReconciliation() throws Exception {
        engine.changeReferenceModel(new RootSetupChangeAction()).subscribe();
        engine.triggerEvents();

        runtimeReconcileActions.add(singletonList(new SlowChangeAction()));
        engine.triggerEvents();
        assertThat(engine.triggerEvents().isRunningChangeActions()).isTrue();

        // Move time, and verify that model is updated
        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        assertThat(engine.triggerEvents().isRunningChangeActions()).isFalse();

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
    public void testChangeActionCancellation() throws Exception {
        SlowChangeAction action = new SlowChangeAction();
        Subscription subscription = engine.changeReferenceModel(action).subscribe();
        engine.triggerEvents();

        assertThat(action.unsubscribed).isFalse();
        subscription.unsubscribe();
        assertThat(action.unsubscribed).isTrue();
    }

    @Test
    public void testIndexes() throws Exception {
        addChild("child1");
        addChild("child2");

        assertThat(engine.orderedView("ascending").stream().map(EntityHolder::getEntity)).containsExactly("child1", "child2");
        assertThat(engine.orderedView("descending").stream().map(EntityHolder::getEntity)).containsExactly("child2", "child1");
    }

    private void addChild(String childId) {
        engine.changeReferenceModel(new AddChildAction(childId)).subscribe();
        engine.triggerEvents();
        assertThat(engine.triggerEvents().hasModelUpdates()).isTrue();
    }

    private void removeChild(String childId) {
        engine.changeReferenceModel(new RemoveChildAction(childId)).subscribe();
        engine.triggerEvents();
        assertThat(engine.triggerEvents().hasModelUpdates()).isTrue();
    }

    private List<ChangeAction> difference(EntityHolder referenceHolder, EntityHolder runtimeHolder, EntityHolder storeHolder) {
        List<ChangeAction> next = runtimeReconcileActions.poll();
        return next == null ? Collections.emptyList() : next;
    }

    class RootSetupChangeAction implements ChangeAction<String> {

        @Override
        public Observable<Pair<String, List<ModelActionHolder>>> apply() {
            return Observable.just(Pair.of(
                    "Root setup",
                    ModelActionHolder.referenceAndRunning(new SimpleModelUpdateAction(EntityHolder.newRoot("root#0", "ROOT"), true))
            ));
        }
    }

    class SlowChangeAction implements ChangeAction<String> {

        private volatile boolean unsubscribed;

        @Override
        public Observable<Pair<String, List<ModelActionHolder>>> apply() {
            return Observable.timer(1, TimeUnit.SECONDS, testScheduler).map(tick -> {
                        ModelAction updateAction = new SimpleModelUpdateAction(EntityHolder.newRoot("root#0", "ROOT"), true);
                        return Pair.of("Slow change", ModelActionHolder.referenceList(updateAction));
                    }
            ).doOnUnsubscribe(() -> unsubscribed = true);
        }
    }

    class AddChildAction implements ChangeAction<String> {

        private final String childId;

        AddChildAction(String childId) {
            this.childId = childId;
        }

        @Override
        public Observable<Pair<String, List<ModelActionHolder>>> apply() {
            SimpleModelUpdateAction updateAction = new SimpleModelUpdateAction(EntityHolder.newRoot(childId, childId), false);
            return Observable.just(Pair.of("Child added: " + childId, ModelActionHolder.referenceList(updateAction)));
        }
    }

    class RemoveChildAction implements ChangeAction<String> {

        private final String childId;

        RemoveChildAction(String childId) {
            this.childId = childId;
        }

        @Override
        public Observable<Pair<String, List<ModelActionHolder>>> apply() {
            ModelAction updateAction = rootHolder -> rootHolder.removeChild(childId);
            return Observable.just(Pair.of("Child removed: " + childId, ModelActionHolder.referenceList(updateAction)));
        }
    }
}