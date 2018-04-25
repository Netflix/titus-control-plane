package com.netflix.titus.common.framework.reconciler.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.subjects.PublishSubject;

class SingleTransaction<EVENT> extends Transaction {

    private static final Logger logger = LoggerFactory.getLogger(SingleTransaction.class);

    enum TransactionStep {
        ChangeActionStarted,
        ChangeActionCompleted,
        ChangeActionFailed,
        ChangeActionUnsubscribed,
        ModelsUpdated,
        EventsEmitted,
        ErrorEventsEmitted,
        SubscribersCompleted
    }

    private final DefaultReconciliationEngine<EVENT> engine;
    private final ReconcileEventFactory<EVENT> eventFactory;
    private final PublishSubject<EVENT> eventSubject;
    private final ReconciliationEngineMetrics<EVENT> metrics;
    private final Clock clock;

    private final ChangeAction changeAction;
    private final Optional<Subscriber<Void>> changeActionSubscriber;
    private final String transactionId;
    private final long changeActionWaitTimeMs;
    private final Subscription changeActionSubscription;

    private TransactionStep transactionStep = TransactionStep.ChangeActionStarted;

    // Set by change action, which my be executed by a different thread.
    private volatile List<ModelActionHolder> modelActionHolders;
    private volatile Throwable changeActionError;
    private volatile long changeActionExecutionTimeMs;

    private final List<EVENT> modelEventQueue = new ArrayList<>();

    SingleTransaction(DefaultReconciliationEngine<EVENT> engine,
                      ChangeAction changeAction,
                      long changeActionCreateTimestamp,
                      Optional<Subscriber<Void>> changeActionSubscriber,
                      String transactionId,
                      boolean byReconciler) {
        this.engine = engine;
        this.eventFactory = engine.getEventFactory();
        this.eventSubject = engine.getEventSubject();
        this.metrics = engine.getMetrics();
        this.clock = engine.getTitusRuntime().getClock();

        this.changeAction = changeAction;
        this.changeActionSubscriber = changeActionSubscriber;
        this.transactionId = transactionId;
        this.changeActionWaitTimeMs = clock.wallTime() - changeActionCreateTimestamp;

        long startTimeNs = clock.nanoTime();
        metrics.changeActionStarted(changeAction, changeActionCreateTimestamp, byReconciler);

        AtomicBoolean metricsNotUpdated = new AtomicBoolean(true);
        this.changeActionSubscription = changeAction.apply()
                .doOnUnsubscribe(() -> {
                    if (metricsNotUpdated.getAndSet(false)) {
                        metrics.changeActionUnsubscribed(changeAction, clock.nanoTime() - startTimeNs, byReconciler);
                    }
                    if (transactionStep == TransactionStep.ChangeActionStarted) {
                        this.transactionStep = TransactionStep.ChangeActionUnsubscribed;
                    }
                })
                .subscribe(
                        modelActionHolders -> {
                            this.modelActionHolders = modelActionHolders;
                        },
                        e -> {
                            if (metricsNotUpdated.getAndSet(false)) {
                                metrics.changeActionFinished(changeAction, clock.nanoTime() - startTimeNs, e, byReconciler);
                            }
                            this.changeActionError = e;
                            this.changeActionExecutionTimeMs = passedMs(startTimeNs);
                            this.transactionStep = TransactionStep.ChangeActionFailed;
                            logger.debug("Action execution error", e);
                        },
                        () -> {
                            if (metricsNotUpdated.getAndSet(false)) {
                                metrics.changeActionFinished(changeAction, clock.nanoTime() - startTimeNs, byReconciler);
                            }
                            if (modelActionHolders == null) {
                                // TODO invariant violation
                                this.modelActionHolders = Collections.emptyList();
                            }
                            this.changeActionExecutionTimeMs = passedMs(startTimeNs);
                            this.transactionStep = TransactionStep.ChangeActionCompleted;
                        }
                );

        changeActionSubscriber.ifPresent(subscriber -> subscriber.add(changeActionSubscription));
    }

    @Override
    void close() {
        this.changeActionSubscription.unsubscribe();
        this.transactionStep = TransactionStep.SubscribersCompleted;
    }

    @Override
    boolean isClosed() {
        return transactionStep == TransactionStep.SubscribersCompleted;
    }

    @Override
    Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        if (transactionStep != TransactionStep.ChangeActionCompleted) {
            return Optional.empty();
        }

        this.transactionStep = TransactionStep.ModelsUpdated;
        if (modelActionHolders.isEmpty()) {
            return Optional.empty();
        }

        EntityHolder referenceRootHolder = modelHolder.getReference();
        EntityHolder runningRootHolder = modelHolder.getRunning();
        EntityHolder storeRootHolder = modelHolder.getStore();

        try {
            for (ModelActionHolder updateAction : modelActionHolders) {
                switch (updateAction.getModel()) {
                    case Reference:
                        referenceRootHolder = applyModelUpdate(updateAction, referenceRootHolder).orElse(referenceRootHolder);
                        break;
                    case Running:
                        runningRootHolder = applyModelUpdate(updateAction, runningRootHolder).orElse(runningRootHolder);
                        break;
                    case Store:
                        storeRootHolder = applyModelUpdate(updateAction, storeRootHolder).orElse(storeRootHolder);
                        break;
                }
            }
        } catch (Exception e) {
            String message = String.format("Change action failure during model update for %s (%s)", referenceRootHolder.getId(), e.toString());
            logger.warn(message, e);
            engine.getTitusRuntime().getCodeInvariants().unexpectedError(message, e);

            this.changeActionError = e;
            this.transactionStep = TransactionStep.ChangeActionFailed;
            return Optional.empty();
        }

        return Optional.of(new ModelHolder(referenceRootHolder, runningRootHolder, storeRootHolder));
    }

    @Override
    void emitEvents() {
        if (transactionStep == TransactionStep.ModelsUpdated) {
            modelEventQueue.forEach(this::emitEvent);
            emitEvent(eventFactory.newAfterChangeEvent(engine, changeAction, changeActionWaitTimeMs, changeActionExecutionTimeMs, transactionId));
            this.transactionStep = TransactionStep.EventsEmitted;
        } else if (transactionStep == TransactionStep.ChangeActionFailed) {
            emitEvent(eventFactory.newChangeErrorEvent(engine, changeAction, changeActionError, changeActionWaitTimeMs, changeActionExecutionTimeMs, transactionId));
            this.transactionStep = TransactionStep.ErrorEventsEmitted;
        }
    }

    @Override
    boolean completeSubscribers() {
        if (transactionStep == TransactionStep.EventsEmitted) {
            changeActionSubscriber.ifPresent(Observer::onCompleted);
            this.transactionStep = TransactionStep.SubscribersCompleted;
            return true;
        } else if (transactionStep == TransactionStep.ErrorEventsEmitted) {
            changeActionSubscriber.ifPresent(subscriber -> subscriber.onError(changeActionError));
            this.transactionStep = TransactionStep.SubscribersCompleted;
            return true;
        } else if (transactionStep == TransactionStep.ChangeActionUnsubscribed) {
            this.transactionStep = TransactionStep.SubscribersCompleted;
            return true;
        }
        return false;
    }

    private Optional<EntityHolder> applyModelUpdate(ModelActionHolder updateAction, EntityHolder rootHolder) {
        ReconcileEventFactory<EVENT> eventFactory = engine.getEventFactory();

        try {
            return updateAction.getAction().apply(rootHolder).map(newRootAndChangedItem -> {
                EntityHolder newRoot = newRootAndChangedItem.getLeft();

                EntityHolder changedItem = newRootAndChangedItem.getRight();
                Optional<EntityHolder> previousHolder = rootHolder.findById(changedItem.getId());

                modelEventQueue.add(eventFactory.newModelUpdateEvent(engine, changeAction, updateAction, changedItem, previousHolder, transactionId));

                return newRoot;
            });
        } catch (Exception e) {
            modelEventQueue.add(eventFactory.newModelUpdateErrorEvent(engine, changeAction, updateAction, rootHolder, e, transactionId));
            logger.warn("Failed to update state of {} ({})", rootHolder.getId(), e.toString());
            throw e;
        }
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
