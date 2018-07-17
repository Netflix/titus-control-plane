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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.netflix.spectator.api.Tag;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

public class DefaultReconciliationEngine<EVENT> implements InternalReconciliationEngine<EVENT> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultReconciliationEngine.class);

    private final AtomicLong nextTransactionId = new AtomicLong();

    private final DifferenceResolver<EVENT> runningDifferenceResolver;
    private final ReconcileEventFactory<EVENT> eventFactory;

    private volatile ModelHolder modelHolder;

    private final BlockingQueue<EVENT> changeActionEventQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<ChangeActionHolder> referenceChangeActions = new LinkedBlockingQueue<>();
    private final ReconciliationEngineMetrics<EVENT> metrics;
    private final TitusRuntime titusRuntime;
    private final Clock clock;

    private IndexSet<EntityHolder> indexSet;

    private Transaction pendingTransaction = EmptyTransaction.EMPTY;

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
                                       TitusRuntime titusRuntime) {
        this.runningDifferenceResolver = runningDifferenceResolver;
        this.eventFactory = eventFactory;
        this.indexSet = IndexSet.newIndexSet(indexComparators);
        this.titusRuntime = titusRuntime;
        this.clock = titusRuntime.getClock();
        this.eventObservable = ObservableExt.protectFromMissingExceptionHandlers(eventSubject, logger);
        this.modelHolder = new ModelHolder(bootstrapModel, bootstrapModel, bootstrapModel);
        this.firstTrigger = newlyCreated;
        this.metrics = new ReconciliationEngineMetrics<>(extraChangeActionTags, extraModelActionTags, titusRuntime.getRegistry(), clock);
        indexEntityHolder(bootstrapModel);
    }

    @Override
    public boolean applyModelUpdates() {
        return pendingTransaction.applyModelUpdates(modelHolder)
                .map(newModelHolder -> {
                    boolean isReferenceModelChanged = newModelHolder != modelHolder && newModelHolder.getReference() != modelHolder.getReference();
                    this.modelHolder = newModelHolder;
                    if (isReferenceModelChanged) {
                        indexEntityHolder(modelHolder.getReference());
                    }
                    return isReferenceModelChanged;
                })
                .orElse(false);
    }

    @Override
    public boolean hasPendingTransactions() {
        return !pendingTransaction.isClosed() || !referenceChangeActions.isEmpty();
    }

    @Override
    public void emitEvents() {
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
            List<EVENT> eventsToEmit = new ArrayList<>();
            changeActionEventQueue.drainTo(eventsToEmit);
            eventsToEmit.forEach(this::emitEvent);
        }

        pendingTransaction.emitEvents();
    }

    @Override
    public boolean closeFinishedTransactions() {
        return pendingTransaction.completeSubscribers();
    }

    @Override
    public boolean triggerActions() {
        if (!pendingTransaction.isClosed()) {
            return true;
        }

        long startTimeNs = clock.nanoTime();
        try {
            // Start next reference change action, if present and exit.
            if (startNextReferenceChangeAction()) {
                return true;
            }

            // Compute the current difference between the reference and persistent/runtime models, and create a list
            // of actions to correct that. The returned action set can be run in parallel.
            List<ChangeAction> reconcileActions = runningDifferenceResolver.apply(this);
            if (!reconcileActions.isEmpty()) {
                startReconcileAction(reconcileActions);
                return true;
            }
            return false;
        } catch (Exception e) {
            metrics.evaluated(clock.nanoTime() - startTimeNs, e);
            titusRuntime.getCodeInvariants().unexpectedError("Unexpected error in ReconciliationEngine", e);
            return true;
        } finally {
            metrics.evaluated(clock.nanoTime() - startTimeNs);
        }
    }

    @Override
    public Observable<Void> changeReferenceModel(ChangeAction referenceUpdate) {
        return changeReferenceModel(referenceUpdate, modelHolder.getReference().getId());
    }

    @Override
    public Observable<Void> changeReferenceModel(ChangeAction referenceUpdate, String entityHolderId) {
        return Observable.unsafeCreate(subscriber -> {
            String transactionId = Long.toString(nextTransactionId.getAndIncrement());
            changeActionEventQueue.add(eventFactory.newBeforeChangeEvent(this, referenceUpdate, transactionId));
            referenceChangeActions.add(new ChangeActionHolder(entityHolderId, referenceUpdate, subscriber, transactionId, clock.wallTime()));
            metrics.updateChangeActionQueueSize(referenceChangeActions.size());
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
        pendingTransaction.close();
        eventSubject.onCompleted();
        metrics.shutdown();
    }

    ReconcileEventFactory<EVENT> getEventFactory() {
        return eventFactory;
    }

    PublishSubject<EVENT> getEventSubject() {
        return eventSubject;
    }

    ReconciliationEngineMetrics<EVENT> getMetrics() {
        return metrics;
    }

    TitusRuntime getTitusRuntime() {
        return titusRuntime;
    }

    private boolean startNextReferenceChangeAction() {
        try {
            ChangeActionHolder actionHolder;
            List<Transaction> transactions = new ArrayList<>();
            List<EntityHolder> changePoints = new ArrayList<>();
            while ((actionHolder = referenceChangeActions.peek()) != null) {
                // Ignore all unsubscribed actions
                Subscriber<Void> subscriber = actionHolder.getSubscriber();
                if (subscriber.isUnsubscribed()) {
                    referenceChangeActions.poll();
                    continue;
                }

                // Emit errors if the change point (EntityHolder for the action) not found
                Optional<EntityHolder> changePointOpt = modelHolder.getReference().findById(actionHolder.getEntityHolderId());
                if (!changePointOpt.isPresent()) {
                    referenceChangeActions.poll();
                    transactions.add(new FailedTransaction<>(this, actionHolder, new IllegalStateException("Entity holder not found: id=" + actionHolder.getEntityHolderId())));
                    continue;
                }

                // Check if the current item overlaps with the already taken actions
                EntityHolder changePoint = changePointOpt.get();
                if (!changePoints.isEmpty() && isOverlapping(changePoint, changePoints)) {
                    break;
                }

                // Create transaction
                changePoints.add(changePoint);
                Transaction transaction;
                try {
                    transaction = new SingleTransaction<>(this, actionHolder.getChangeAction(), actionHolder.getCreateTimestamp(), Optional.of(actionHolder.getSubscriber()), actionHolder.getTransactionId(), false);
                } catch (Exception e) {
                    transaction = new FailedTransaction<>(this, actionHolder, e);
                }
                transactions.add(transaction);
                referenceChangeActions.poll();
            }

            if (transactions.isEmpty()) {
                return false;
            }

            pendingTransaction = transactions.size() == 1 ? transactions.get(0) : new CompositeTransaction(transactions);
            return true;
        } finally {
            metrics.updateChangeActionQueueSize(referenceChangeActions.size());
        }
    }

    private boolean isOverlapping(EntityHolder changePoint, List<EntityHolder> changePoints) {
        for (EntityHolder next : changePoints) {
            if (next.findById(changePoint.getId()).isPresent()) {
                return true;
            }
            if (changePoint.findChildById(next.getId()).isPresent()) {
                return true;
            }
        }
        return false;
    }

    private void startReconcileAction(List<ChangeAction> reconcileActions) {
        long transactionId = nextTransactionId.getAndIncrement();
        long nestedId = 0;
        long now = clock.wallTime();

        List<Transaction> transactions = new ArrayList<>();
        for (ChangeAction changeAction : reconcileActions) {
            String compositeTransactionId = reconcileActions.size() == 1
                    ? Long.toString(transactionId)
                    : transactionId + "." + nestedId;
            nestedId++;

            emitEvent(eventFactory.newBeforeChangeEvent(this, changeAction, compositeTransactionId));

            transactions.add(new SingleTransaction<>(this, changeAction, now, Optional.empty(), compositeTransactionId, true));
        }

        pendingTransaction = transactions.size() == 1 ? transactions.get(0) : new CompositeTransaction(transactions);
    }

    private void indexEntityHolder(EntityHolder entityHolder) {
        indexSet = indexSet.apply(entityHolder.getChildren());
    }

    void emitEvent(EVENT event) {
        long startTimeNs = clock.nanoTime();
        try {
            eventSubject.onNext(event);
            metrics.emittedEvent(event, clock.nanoTime() - startTimeNs);
        } catch (Exception e) {
            metrics.emittedEvent(event, clock.nanoTime() - startTimeNs, e);
            logger.error("Bad subscriber", e);
        }
    }
}
