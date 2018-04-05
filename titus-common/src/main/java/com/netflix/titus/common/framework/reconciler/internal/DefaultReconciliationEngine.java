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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subscriptions.Subscriptions;

import static com.netflix.titus.common.util.code.CodeInvariants.codeInvariants;


/**
 * TODO Catch model update exceptions and propagate them as change actions error result.
 */
public class DefaultReconciliationEngine<EVENT> implements ReconciliationEngine<EVENT> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultReconciliationEngine.class);

    private final AtomicLong nextTransactionId = new AtomicLong();

    private final ReconcileEventFactory<EVENT> eventFactory;
    private final ModelHolder<EVENT> modelHolder;

    private final BlockingQueue<EVENT> changeActionEventQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Pair<ChangeActionHolder, Subscriber<Void>>> referenceChangeActions = new LinkedBlockingQueue<>();
    private final BlockingQueue<Pair<ChangeActionHolder, List<ModelActionHolder>>> modelActionHolders = new LinkedBlockingQueue<>();
    private final ReconciliationEngineMetrics<EVENT> metrics;
    private final Clock clock;

    private IndexSet<EntityHolder> indexSet;

    private Optional<Subscription> startedReferenceChangeActionSubscription = Optional.empty();
    private List<Subscription> startedReconciliationActionSubscriptions = Collections.emptyList();

    private final PublishSubject<EVENT> eventSubject = PublishSubject.create();
    private final Observable<EVENT> eventObservable;

    private boolean firstTrigger;

    public DefaultReconciliationEngine(EntityHolder bootstrapModel,
                                       boolean newlyCreated,
                                       DifferenceResolver<EVENT> runningDifferenceResolver,
                                       Map<Object, Comparator<EntityHolder>> indexComparators,
                                       ReconcileEventFactory<EVENT> eventFactory,
                                       Function<ChangeAction, List<Tag>> extraChangeActionTags,
                                       Function<EVENT, List<Tag>> extraModelActionTags,
                                       Registry registry,
                                       Clock clock) {
        this.eventFactory = eventFactory;
        this.indexSet = IndexSet.newIndexSet(indexComparators);
        this.clock = clock;
        this.eventObservable = ObservableExt.protectFromMissingExceptionHandlers(eventSubject, logger);
        this.modelHolder = new ModelHolder<>(this, bootstrapModel, runningDifferenceResolver);
        this.firstTrigger = newlyCreated;
        this.metrics = new ReconciliationEngineMetrics<>(bootstrapModel.getId(), extraChangeActionTags, extraModelActionTags, registry, clock);
        indexEntityHolder(bootstrapModel);
    }

    @Override
    public boolean applyModelUpdates() {
        if (modelActionHolders.isEmpty()) {
            return false;
        }

        long startTimeNs = clock.nanoTime();
        try {
            applyModelUpdatesInternal();
        } catch (Exception e) {
            metrics.eventsAndModelUpdates(clock.nanoTime() - startTimeNs, e);
            codeInvariants().unexpectedError("Unexpected error in ReconciliationEngine", e);
            return false;
        } finally {
            metrics.eventsAndModelUpdates(clock.nanoTime() - startTimeNs);
        }
        return true;
    }

    @Override
    public boolean triggerEvents() {
        long startTimeNs = clock.nanoTime();
        try {
            /*
              We need to emit first holder state after initialization, but we can do this only after {@link ReconcileEventFactory}
              has a chance to subscribe. Alternatively we could shift the logic to {@link ReconcileEventFactory}, but this
              would create two sources of events for an engine.
             */
            if (firstTrigger) {
                firstTrigger = false;
                emitEvent(eventFactory.newModelEvent(this, modelHolder.getReference()));
            }
            if (!changeActionEventQueue.isEmpty()) {
                List<EVENT> drainQueue = new ArrayList<>();
                changeActionEventQueue.drainTo(drainQueue);

                // Due to concurrent updates, we have to drain the queue first, and only after check if there are any
                // new model updates. If there are we have to abandon this iteration, and start from the beginning later.
                if (!isOkToProcessNextChangeAction()) {
                    changeActionEventQueue.addAll(drainQueue);
                    return false;
                }

                drainQueue.forEach(this::emitEvent);
            }

            // If there are pending changes (user triggered or reconciliation) do nothing.
            if (hasRunningReferenceStateUpdate() || hasRunningReconciliationActions()) {
                return true;
            }

            // In this place we know that all pending actions are done. We do the last check in case any new model updates arrived.
            if (!isOkToProcessNextChangeAction() || !changeActionEventQueue.isEmpty()) {
                return false;
            }

            // Start next reference change action, if present and exit.
            if (startNextReferenceChangeAction()) {
                return true;
            }

            // Compute the current difference between the reference and persistent/runtime models, and create a list
            // of actions to correct that. The returned action set can be run in parallel.
            List<ChangeAction> reconcileActions = modelHolder.resolveDifference();
            if (!reconcileActions.isEmpty()) {
                startReconcileAction(reconcileActions);
                return true;
            }
            return false;
        } catch (Exception e) {
            metrics.evaluated(clock.nanoTime() - startTimeNs, e);
            codeInvariants().unexpectedError("Unexpected error in ReconciliationEngine", e);
            return true;
        } finally {
            metrics.evaluated(clock.nanoTime() - startTimeNs);
        }
    }

    /**
     * We expect all model actions to be executed before a new event is triggered. It is however possible that
     * after {@link #applyModelUpdates} is executed, and before this method is called, another action completes, and
     * adds new model updates to the queue. We cannot proceed in this case, as we would reconcile from the outdated
     * model view. By exiting here, we will process the model updates in the next iteration, and move on with the consistent
     * view.
     */
    private boolean isOkToProcessNextChangeAction() {
        if (modelActionHolders.isEmpty()) {
            return true;
        }
        // Ideally we do not want this to happen. For now, we will just report these occurrences.
        // If it becomes a performance problem we may have to better optimize the code.
        metrics.abandonedIteration();
        logger.debug("Not all model updates applied for: id={}, pendingModelUpdates={}", modelHolder.getReference().getId(), modelActionHolders.size());
        return false;
    }

    @Override
    public Observable<Void> changeReferenceModel(ChangeAction referenceUpdate) {
        return Observable.unsafeCreate(subscriber -> {
            long transactionId = nextTransactionId.getAndIncrement();
            changeActionEventQueue.add(eventFactory.newBeforeChangeEvent(this, referenceUpdate, transactionId));
            referenceChangeActions.add(Pair.of(new ChangeActionHolder(referenceUpdate, transactionId, clock.wallTime()), (Subscriber<Void>) subscriber));
        });
    }

    @Override
    public EntityHolder getReferenceView() {
        return modelHolder.getReference();
    }

    @Override
    public EntityHolder getRunningView() {
        return modelHolder.getRunning();
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
    public Observable<EVENT> events() {
        return eventObservable;
    }

    void shutdown() {
        startedReferenceChangeActionSubscription.ifPresent(Subscription::unsubscribe);
        startedReferenceChangeActionSubscription = Optional.empty();

        startedReconciliationActionSubscriptions.forEach(Subscription::unsubscribe);
        startedReconciliationActionSubscriptions = Collections.emptyList();

        eventSubject.onCompleted();

        metrics.shutdown();
    }

    private void applyModelUpdatesInternal() {
        Pair<ChangeActionHolder, List<ModelActionHolder>> next;
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
                final Pair<ChangeActionHolder, List<ModelActionHolder>> finalNext = next;
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
                        emitEvent(eventFactory.newModelUpdateEvent(this, finalNext.getLeft().getChangeAction(), updateAction, changedItem, previousHolder, finalNext.getLeft().getTransactionId()));
                    });
                } catch (Exception e) {
                    emitEvent(eventFactory.newModelUpdateErrorEvent(this, finalNext.getLeft().getChangeAction(), updateAction, rootHolder, e, next.getLeft().getTransactionId()));
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
        Pair<ChangeActionHolder, Subscriber<Void>> next;
        while ((next = referenceChangeActions.poll()) != null) {
            Subscriber<Void> subscriber = next.getRight();
            if (!subscriber.isUnsubscribed()) {
                ChangeActionHolder actionHolder = next.getLeft();
                long startTimeNs = clock.nanoTime();
                metrics.changeActionStarted(actionHolder);

                final Pair<ChangeActionHolder, Subscriber<Void>> finalNext = next;
                AtomicBoolean metricsNotUpdated = new AtomicBoolean(true);
                Subscription subscription = actionHolder.getChangeAction().apply()
                        .doOnUnsubscribe(() -> {
                            if (metricsNotUpdated.getAndSet(false)) {
                                metrics.changeActionUnsubscribed(actionHolder, clock.nanoTime() - startTimeNs);
                            }
                            subscriber.unsubscribe();
                        })
                        .subscribe(
                                modelActionHolderList -> {
                                    changeActionEventQueue.add(eventFactory.newAfterChangeEvent(this, actionHolder.getChangeAction(), passedMs(startTimeNs), actionHolder.getTransactionId()));
                                    registerModelUpdateRequest(finalNext.getLeft(), modelActionHolderList);
                                },
                                e -> {
                                    if (metricsNotUpdated.getAndSet(false)) {
                                        metrics.changeActionFinished(actionHolder, clock.nanoTime() - startTimeNs, e);
                                    }
                                    changeActionEventQueue.add(eventFactory.newChangeErrorEvent(this, actionHolder.getChangeAction(), e, passedMs(startTimeNs), actionHolder.getTransactionId()));
                                    subscriber.onError(e);
                                },
                                // TODO Make sure always one element is emitted
                                () -> {
                                    if (metricsNotUpdated.getAndSet(false)) {
                                        metrics.changeActionFinished(actionHolder, clock.nanoTime() - startTimeNs);
                                    }
                                    subscriber.onCompleted();
                                }
                        );
                subscriber.add(Subscriptions.create(subscription::unsubscribe));
                startedReferenceChangeActionSubscription = Optional.of(subscription);
                return true;
            }
        }
        return false;
    }

    private void registerModelUpdateRequest(ChangeActionHolder changeActionHolder, List<ModelActionHolder> stateChange) {
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

    private void startReconcileAction(List<ChangeAction> reconcileActions) {
        List<Subscription> subscriptions = new ArrayList<>(reconcileActions.size());
        for (ChangeAction action : reconcileActions) {
            long transactionId = nextTransactionId.getAndIncrement();
            ChangeActionHolder changeActionHolder = new ChangeActionHolder(action, transactionId, clock.wallTime());

            long startTimeNs = clock.nanoTime();
            metrics.reconcileActionStarted(changeActionHolder);

            emitEvent(eventFactory.newBeforeChangeEvent(this, action, transactionId));
            AtomicBoolean metricsNotUpdated = new AtomicBoolean(true);
            Subscription subscription = action.apply()
                    .doOnUnsubscribe(() -> {
                        if (metricsNotUpdated.getAndSet(false)) {
                            metrics.reconcileActionUnsubscribed(changeActionHolder, clock.nanoTime() - startTimeNs);
                        }
                    })
                    .subscribe(
                            modelActionHolders -> {
                                registerModelUpdateRequest(changeActionHolder, modelActionHolders);
                                changeActionEventQueue.add(eventFactory.newAfterChangeEvent(this, action, passedMs(startTimeNs), transactionId));
                            },
                            e -> {
                                if (metricsNotUpdated.getAndSet(false)) {
                                    metrics.reconcileActionFinished(changeActionHolder, clock.nanoTime() - startTimeNs, e);
                                }
                                changeActionEventQueue.add(eventFactory.newChangeErrorEvent(this, action, e, passedMs(startTimeNs), transactionId));
                                logger.debug("Action execution error", e);
                            },
                            () -> {
                                if (metricsNotUpdated.getAndSet(false)) {
                                    metrics.reconcileActionFinished(changeActionHolder, clock.nanoTime() - startTimeNs);
                                }
                            }
                    );
            subscriptions.add(subscription);
        }
        this.startedReconciliationActionSubscriptions = subscriptions;
    }

    private void indexEntityHolder(EntityHolder entityHolder) {
        indexSet = indexSet.apply(entityHolder.getChildren());
    }

    private void emitEvent(EVENT event) {
        long startTimeNs = clock.nanoTime();
        try {
            eventSubject.onNext(event);
            metrics.emittedEvent(event, clock.nanoTime() - startTimeNs);
        } catch (Exception e) {
            metrics.emittedEvent(event, clock.nanoTime() - startTimeNs, e);
            logger.error("Bad subscriber", e);
        }
    }

    private long passedMs(long startTimeNs) {
        return TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - startTimeNs);
    }
}
