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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Stopwatch;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subscriptions.Subscriptions;

/**
 */
public class DefaultReconciliationEngine<CHANGE, EVENT> implements ReconciliationEngine<CHANGE, EVENT> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultReconciliationEngine.class);

    private final AtomicLong nextTransactionId = new AtomicLong();

    private final ReconcileEventFactory<CHANGE, EVENT> eventFactory;
    private final ModelHolder<CHANGE, EVENT> modelHolder;

    private final BlockingQueue<Pair<ChangeActionHolder<CHANGE>, Subscriber<Void>>> referenceChangeActions = new LinkedBlockingQueue<>();
    private final BlockingQueue<Pair<ChangeActionHolder<CHANGE>, List<ModelActionHolder>>> modelActionHolders = new LinkedBlockingQueue<>();

    private IndexSet<EntityHolder> indexSet;

    private Optional<Subscription> startedReferenceChangeActionSubscription = Optional.empty();
    private List<Subscription> startedReconciliationActionSubscriptions = Collections.emptyList();

    private final PublishSubject<EVENT> eventSubject = PublishSubject.create();
    private final Observable<EVENT> eventObservable = eventSubject.asObservable();

    private boolean firstTrigger;

    public DefaultReconciliationEngine(EntityHolder bootstrapModel,
                                       DifferenceResolver<CHANGE, EVENT> runningDifferenceResolver,
                                       Map<Object, Comparator<EntityHolder>> indexComparators,
                                       ReconcileEventFactory<CHANGE, EVENT> eventFactory) {
        this.eventFactory = eventFactory;
        this.indexSet = IndexSet.newIndexSet(indexComparators);
        this.modelHolder = new ModelHolder<>(this, bootstrapModel, runningDifferenceResolver);
        this.firstTrigger = true;
        indexEntityHolder(bootstrapModel);
    }

    @Override
    public TriggerStatus triggerEvents() {
        /*
          We need to emit first holder state after initialization, but we can do this only after {@link ReconcileEventFactory}
          has a chance to subscribe. Alternatively we could shift the logic to {@link ReconcileEventFactory}, but this
          would create two sources of events for an engine.
         */
        if (firstTrigger) {
            firstTrigger = false;
            eventSubject.onNext(eventFactory.newModelEvent(this, modelHolder.getReference()));
        }

        // Always apply known runtime state changes first.
        boolean modelUpdates = !modelActionHolders.isEmpty();
        if (modelUpdates) {
            applyModelUpdates();
        }

        // If there are pending changes (user triggered or reconciliation) do nothing.
        if (hasRunningReferenceStateUpdate() || hasRunningReconciliationActions()) {
            return new TriggerStatus(true, modelUpdates);
        }

        // Start next reference change action, if present and exit.
        if (startNextReferenceChangeAction()) {
            return new TriggerStatus(true, modelUpdates);
        }

        // Compute the current difference between the reference and persistent/runtime models, and create a list
        // of actions to correct that. The returned action set can be run in parallel.
        List<ChangeAction<CHANGE>> reconcileActions = modelHolder.resolveDifference();
        if (!reconcileActions.isEmpty()) {
            startReconcileAction(reconcileActions);
            return new TriggerStatus(true, modelUpdates);
        }
        return new TriggerStatus(false, modelUpdates);
    }

    @Override
    public Observable<Void> changeReferenceModel(ChangeAction<CHANGE> referenceUpdate) {
        return Observable.unsafeCreate(subscriber -> {
            long transactionId = nextTransactionId.getAndIncrement();
            eventSubject.onNext(eventFactory.newBeforeChangeEvent(this, referenceUpdate, transactionId));
            referenceChangeActions.add(Pair.of(new ChangeActionHolder<>(referenceUpdate, transactionId), (Subscriber<Void>) subscriber));
        });
    }

    @Override
    public EntityHolder getReferenceView() {
        return modelHolder.getReference();
    }

    @Override
    public EntityHolder getStoreView() {
        return modelHolder.getStore();
    }

    @Override
    public <ORDER_BY> List<EntityHolder> orderedView(ORDER_BY orderingCriteria) {
        return indexSet.getOrdered(orderingCriteria);
    }

    @Override
    public EntityHolder getRunningView() {
        return modelHolder.getRunning();
    }

    @Override
    public Observable<EVENT> events() {
        return eventObservable;
    }

    void shutdown() {
        startedReferenceChangeActionSubscription.ifPresent(Subscription::unsubscribe);
        startedReferenceChangeActionSubscription = Optional.empty();

        startedReconciliationActionSubscriptions.forEach(Subscription::unsubscribe);
        startedReconciliationActionSubscriptions = Collections.emptyList();

        eventSubject.onCompleted();
    }

    private void applyModelUpdates() {
        Pair<ChangeActionHolder<CHANGE>, List<ModelActionHolder>> next;
        while ((next = modelActionHolders.poll()) != null) {
            for (ModelActionHolder updateAction : next.getRight()) {
                EntityHolder rootHolder;
                switch (updateAction.getModel()) {
                    case Reference:
                        rootHolder = modelHolder.getReference();
                        break;
                    case Running:
                        rootHolder = modelHolder.getRunning();
                        break;
                    case Store:
                        rootHolder = modelHolder.getStore();
                        break;
                    default:
                        return;
                }
                final Pair<ChangeActionHolder<CHANGE>, List<ModelActionHolder>> finalNext = next;
                try {
                    updateAction.getAction().apply(rootHolder).ifPresent(newRootAndChangedItem -> {
                        EntityHolder newRoot = newRootAndChangedItem.getLeft();
                        EntityHolder changedItem = newRootAndChangedItem.getRight();
                        Optional<EntityHolder> previousHolder = Optional.empty();
                        switch (updateAction.getModel()) {
                            case Reference:
                                previousHolder = getPrevious(modelHolder.getReference(), changedItem);
                                modelHolder.setReference(newRoot);
                                if (changedItem != newRoot) {
                                    indexEntityHolder(newRoot);
                                }
                                break;
                            case Running:
                                previousHolder = getPrevious(modelHolder.getRunning(), changedItem);
                                modelHolder.setRunning(newRoot);
                                break;
                            case Store:
                                previousHolder = getPrevious(modelHolder.getStore(), changedItem);
                                modelHolder.setStore(newRoot);
                                break;
                        }
                        eventSubject.onNext(eventFactory.newModelUpdateEvent(this, finalNext.getLeft().getChangeAction(), updateAction, changedItem, previousHolder, finalNext.getLeft().getTransactionId()));
                    });
                } catch (Exception e) {
                    eventSubject.onNext(eventFactory.newModelUpdateErrorEvent(this, finalNext.getLeft().getChangeAction(), updateAction, rootHolder, e, next.getLeft().getTransactionId()));
                    logger.warn("Failed to update running state of {} ({})", next.getClass().getSimpleName(), e.toString());
                }
            }
        }
    }

    private Optional<EntityHolder> getPrevious(EntityHolder root, EntityHolder changedHolder) {
        return root.findById(changedHolder.getId());
    }

    private boolean hasRunningReferenceStateUpdate() {
        if (!startedReferenceChangeActionSubscription.isPresent()) {
            return false;
        }
        if (startedReferenceChangeActionSubscription.get().isUnsubscribed()) {
            startedReferenceChangeActionSubscription = Optional.empty();
            return false;
        }
        return true;
    }

    private boolean startNextReferenceChangeAction() {
        Pair<ChangeActionHolder<CHANGE>, Subscriber<Void>> next;
        while ((next = referenceChangeActions.poll()) != null) {
            Subscriber<Void> subscriber = next.getRight();
            if (!subscriber.isUnsubscribed()) {
                ChangeActionHolder<CHANGE> actionHolder = next.getLeft();
                Stopwatch timer = Stopwatch.createStarted();

                final Pair<ChangeActionHolder<CHANGE>, Subscriber<Void>> finalNext = next;
                Subscription subscription = actionHolder.getChangeAction().apply()
                        .doOnUnsubscribe(subscriber::unsubscribe)
                        .subscribe(
                                resultPair -> {
                                    eventSubject.onNext(eventFactory.newAfterChangeEvent(this, actionHolder.getChangeAction(), resultPair.getLeft(), timer.elapsed(TimeUnit.MILLISECONDS), actionHolder.getTransactionId()));
                                    registerModelUpdateRequest(finalNext.getLeft(), resultPair.getRight());
                                },
                                e -> {
                                    eventSubject.onNext(eventFactory.newChangeErrorEvent(this, actionHolder.getChangeAction(), e, timer.elapsed(TimeUnit.MILLISECONDS), actionHolder.getTransactionId()));
                                    subscriber.onError(e);
                                },
                                // TODO Make sure always one element is emitted
                                subscriber::onCompleted
                        );
                subscriber.add(Subscriptions.create(subscription::unsubscribe));
                startedReferenceChangeActionSubscription = Optional.of(subscription);
                return true;
            }
        }
        return false;
    }

    private void registerModelUpdateRequest(ChangeActionHolder<CHANGE> changeActionHolder, List<ModelActionHolder> stateChange) {
        modelActionHolders.add(Pair.of(changeActionHolder, stateChange));
    }

    private boolean hasRunningReconciliationActions() {
        if (startedReconciliationActionSubscriptions.isEmpty()) {
            return false;
        }
        boolean finished = startedReconciliationActionSubscriptions.stream().filter(a -> !a.isUnsubscribed()).count() == 0;
        if (finished) {
            startedReconciliationActionSubscriptions = Collections.emptyList();
        }
        return !finished;
    }

    private void startReconcileAction(List<ChangeAction<CHANGE>> reconcileActions) {
        List<Subscription> subscriptions = new ArrayList<>(reconcileActions.size());
        for (ChangeAction<CHANGE> action : reconcileActions) {
            long transactionId = nextTransactionId.getAndIncrement();
            Stopwatch timer = Stopwatch.createStarted();
            eventSubject.onNext(eventFactory.newBeforeChangeEvent(this, action, transactionId));
            Subscription subscription = action.apply().subscribe(
                    resultPair -> {
                        registerModelUpdateRequest(new ChangeActionHolder<>(action, transactionId), resultPair.getRight());
                        eventSubject.onNext(eventFactory.newAfterChangeEvent(this, action, resultPair.getLeft(), timer.elapsed(TimeUnit.MILLISECONDS), transactionId));
                    },
                    e -> {
                        eventSubject.onNext(eventFactory.newChangeErrorEvent(this, action, e, timer.elapsed(TimeUnit.MILLISECONDS), transactionId));
                        logger.debug("Action execution error", e);
                    }
            );
            subscriptions.add(subscription);
        }
        this.startedReconciliationActionSubscriptions = subscriptions;
    }

    private void indexEntityHolder(EntityHolder entityHolder) {
        indexSet = indexSet.apply(entityHolder.getChildren());
    }
}
