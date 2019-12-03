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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.internal.transaction.FailedTransaction;
import com.netflix.titus.common.framework.simplereconciler.internal.transaction.SingleTransaction;
import com.netflix.titus.common.framework.simplereconciler.internal.transaction.Transaction;
import com.netflix.titus.common.framework.simplereconciler.internal.transaction.TransactionStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

class ReconcilerEngine<DATA> {

    private static final Logger logger = LoggerFactory.getLogger(ReconcilerEngine.class);

    static final IllegalStateException EXCEPTION_CLOSED = new IllegalStateException("Reconciler closed");
    static final IllegalStateException EXCEPTION_CANCELLED = new IllegalStateException("cancelled");

    private final String id;
    private final Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider;
    private final Clock clock;

    private final BlockingQueue<ChangeActionHolder<DATA>> referenceChangeActions = new LinkedBlockingQueue<>();
    private final BlockingQueue<Runnable> closeCallbacks = new LinkedBlockingQueue<>();
    private final ReconcilerExecutorMetrics metrics;

    private AtomicReference<ReconcilerState> state = new AtomicReference<>(ReconcilerState.Running);

    private volatile DATA current;

    /**
     * We start the transaction numbering from 1, as 0 is reserved for the initial engine creation step.
     */
    private final AtomicLong nextTransactionId = new AtomicLong(1);

    private volatile Transaction<DATA> pendingTransaction;

    ReconcilerEngine(String id,
                     DATA initial,
                     Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider,
                     ReconcilerExecutorMetrics metrics,
                     TitusRuntime titusRuntime) {
        this.id = id;
        this.current = initial;
        this.reconcilerActionsProvider = reconcilerActionsProvider;
        this.metrics = metrics;
        this.clock = titusRuntime.getClock();
    }

    void addOnCloseListener(Runnable action) {
        closeCallbacks.add(action);
        if (state.get() == ReconcilerState.Closed) {
            action.run();
        }
    }

    String getId() {
        return id;
    }

    DATA getCurrent() {
        return current;
    }

    long getNextTransactionId() {
        return nextTransactionId.get();
    }

    public ReconcilerState getState() {
        return state.get();
    }

    Transaction<DATA> getPendingTransaction() {
        return pendingTransaction;
    }

    Mono<DATA> apply(Function<DATA, Mono<DATA>> action) {
        return Mono.create(sink -> {
            if (state.get() != ReconcilerState.Running) {
                sink.error(EXCEPTION_CLOSED);
                return;
            }

            String transactionId = "" + nextTransactionId.getAndIncrement();
            Function<DATA, Mono<Function<DATA, DATA>>> internalAction = data -> action.apply(data).map(d -> dd -> d);
            referenceChangeActions.add(new ChangeActionHolder<>(internalAction, transactionId, clock.wallTime(), sink));

            // Check again, as it may not be cleaned up by the worker process if the shutdown was in progress.
            if (state.get() != ReconcilerState.Running) {
                sink.error(EXCEPTION_CLOSED);
            }

            metrics.updateExternalActionQueueSize(id, referenceChangeActions.size());
        });
    }

    void close() {
        state.compareAndSet(ReconcilerState.Running, ReconcilerState.Closing);
        Evaluators.acceptNotNull(pendingTransaction, Transaction::cancel);
    }

    void processDataUpdates() {
        if (pendingTransaction != null) {
            if (pendingTransaction.getStatus().getState() == TransactionStatus.State.ResultReady) {
                pendingTransaction.readyToClose(current);
                if (pendingTransaction.getStatus().getState() == TransactionStatus.State.Completed) {
                    this.current = pendingTransaction.getStatus().getResult();
                } else {
                    Throwable error = pendingTransaction.getStatus().getError();
                    logger.warn("Reconciliation action failure during data merging: status={}, error={}", pendingTransaction.getStatus(), error.getMessage());
                    logger.debug("Stack trace", error);
                }
            }
        }
    }

    /**
     * Returns function, so evaluation which notifies a subscriber may be run on a different thread.
     * <p>
     * TODO Support concurrent transactions in the reconciliation loop.
     */
    Optional<Pair<Optional<DATA>, Runnable>> closeFinishedTransaction() {
        if (pendingTransaction == null) {
            return Optional.empty();
        }

        TransactionStatus<DATA> status = pendingTransaction.getStatus();
        TransactionStatus.State state = status.getState();

        // Still running
        if (state == TransactionStatus.State.Started || state == TransactionStatus.State.ResultReady) {
            if (this.state.get() != ReconcilerState.Running) {
                pendingTransaction.cancel();
            }
            return Optional.empty();
        }

        // If sink is null, it is a reconciliation action
        MonoSink<DATA> sink = pendingTransaction.getActionHolder().getSubscriberSink();

        try {
            // Completed
            if (state == TransactionStatus.State.Completed) {
                DATA result = status.getResult();
                if (sink == null) {
                    return Optional.of(Pair.of(Optional.ofNullable(result), Evaluators::doNothing));
                }
                if (result == null) {
                    return Optional.of(Pair.of(Optional.empty(), () -> ExceptionExt.silent(sink::success)));
                }
                return Optional.of(Pair.of(Optional.of(result), () -> ExceptionExt.silent(() -> sink.success(result))));
            }

            // Failed
            if (state == TransactionStatus.State.Failed) {
                if (sink == null) {
                    logger.warn("Reconciler action failure: {}", pendingTransaction.getStatus().getError().getMessage());
                    return Optional.empty();
                }
                return Optional.of(Pair.of(Optional.empty(), () -> ExceptionExt.silent(() -> sink.error(status.getError()))));
            }

            // Cancelled
            if (state == TransactionStatus.State.Cancelled) {
                if (sink == null) {
                    return Optional.empty();
                }
                return Optional.of(Pair.of(Optional.empty(), () -> ExceptionExt.silent(() -> sink.error(new IllegalStateException("cancelled")))));
            }

            // Not reachable unless there is a serious bug.
            logger.error("Unexpected state: {}", state);
            return Optional.empty();
        } finally {
            pendingTransaction = null;
        }
    }

    Optional<Runnable> tryToClose() {
        if (pendingTransaction != null || !referenceChangeActions.isEmpty() || !state.compareAndSet(ReconcilerState.Closing, ReconcilerState.Closed)) {
            return Optional.empty();
        }
        return Optional.of(() -> closeCallbacks.forEach(c -> ExceptionExt.silent(c::run)));
    }

    boolean startNextExternalChangeAction() {
        try {
            if (pendingTransaction != null) {
                return false;
            }

            ChangeActionHolder<DATA> actionHolder;
            Transaction<DATA> transaction = null;
            while (transaction == null && (actionHolder = referenceChangeActions.poll()) != null) {
                // Ignore all unsubscribed actions
                if (actionHolder.isCancelled()) {
                    continue;
                }

                // Create transaction
                if (state.get() == ReconcilerState.Running) {
                    try {
                        transaction = new SingleTransaction<>(current, actionHolder);
                    } catch (Exception e) {
                        transaction = new FailedTransaction<>(actionHolder, e);
                    }
                } else {
                    transaction = new FailedTransaction<>(actionHolder, EXCEPTION_CANCELLED);
                }
            }

            return (pendingTransaction = transaction) != null;
        } finally {
            metrics.updateExternalActionQueueSize(id, referenceChangeActions.size());
        }
    }

    boolean startReconcileAction() {
        if (state.get() != ReconcilerState.Running) {
            return false;
        }

        List<Mono<Function<DATA, DATA>>> reconcilerActions = reconcilerActionsProvider.apply(current);
        if (CollectionsExt.isNullOrEmpty(reconcilerActions)) {
            return false;
        }

        // TODO We process first transaction only, as composite transactions are not implemented yet.
        Mono<Function<DATA, DATA>> action = reconcilerActions.get(0);
        Function<DATA, Mono<Function<DATA, DATA>>> internalAction = data -> action;

        String transactionId = "" + nextTransactionId.getAndIncrement();
        ChangeActionHolder<DATA> actionHolder = new ChangeActionHolder<>(
                internalAction,
                transactionId,
                clock.wallTime(),
                null
        );
        pendingTransaction = new SingleTransaction<>(current, actionHolder);

        return true;
    }
}
