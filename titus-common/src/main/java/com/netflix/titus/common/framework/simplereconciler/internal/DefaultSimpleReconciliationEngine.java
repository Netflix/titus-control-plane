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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.SimpleReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;

public class DefaultSimpleReconciliationEngine<DATA> implements SimpleReconciliationEngine<DATA> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSimpleReconciliationEngine.class);

    private final String name;
    private final Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider;
    private final Clock clock;
    private final TitusRuntime titusRuntime;

    private final long quickCycleMs;
    private final long longCycleMs;
    private volatile long lastLongCycleTimestamp;

    private final BlockingQueue<ChangeActionHolder<DATA>> referenceChangeActions = new LinkedBlockingQueue<>();
    private final SimpleReconciliationEngineMetrics metrics;

    private volatile boolean runnable = true;
    private final Scheduler.Worker worker;

    private volatile DATA current;
    private final AtomicLong nextTransactionId = new AtomicLong();
    private volatile Transaction<DATA> pendingTransaction = EmptyTransaction.empty();

    private final ReplayProcessor<DATA> eventProcessor = ReplayProcessor.create(1);
    private final Flux<DATA> eventStream = eventProcessor.compose(ReactorExt.badSubscriberHandler(logger));

    public DefaultSimpleReconciliationEngine(String name,
                                             DATA initial,
                                             Duration quickCycle,
                                             Duration longCycle,
                                             Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider,
                                             Scheduler scheduler,
                                             TitusRuntime titusRuntime) {
        this.name = name;
        this.current = initial;
        this.quickCycleMs = quickCycle.toMillis();
        this.longCycleMs = longCycle.toMillis();
        this.reconcilerActionsProvider = reconcilerActionsProvider;
        this.metrics = new SimpleReconciliationEngineMetrics(titusRuntime);
        this.worker = scheduler.createWorker();
        this.clock = titusRuntime.getClock();
        this.titusRuntime = titusRuntime;

        eventProcessor.onNext(initial);
        doSchedule(0);
    }

    @Override
    public void close() {
        this.runnable = false;
    }

    @Override
    public DATA getCurrent() {
        return current;
    }

    @Override
    public Mono<DATA> apply(Mono<Function<DATA, DATA>> action) {
        return Mono.create(sink -> {
            if (!runnable) {
                sink.error(new IllegalStateException("Reconciler closed"));
                return;
            }
            String transactionId = "" + nextTransactionId.getAndIncrement();
            referenceChangeActions.add(new ChangeActionHolder<>(action, transactionId, clock.wallTime(), sink));

            // Check again, as it may not be cleaned up by the worker process if the shutdown was in progress.
            if (!runnable) {
                sink.error(new IllegalStateException("Reconciler closed"));
            }

            metrics.updateExternalActionQueueSize(referenceChangeActions.size());
        });
    }

    @Override
    public Flux<DATA> changes() {
        return eventStream;
    }

    private void doSchedule(long delayMs) {
        worker.schedule(() -> {
            if (!runnable) {
                cancelPendingActions();
                return;
            }

            long startTimeMs = clock.wallTime();
            long startTimeNs = clock.nanoTime();
            try {
                boolean fullCycle = (startTimeMs - lastLongCycleTimestamp) >= longCycleMs;
                if (fullCycle) {
                    lastLongCycleTimestamp = startTimeMs;
                }

                doLoop(fullCycle);
                doSchedule(quickCycleMs);
            } catch (Exception e) {
                metrics.evaluated(clock.nanoTime() - startTimeNs, e);
                logger.warn("Unexpected error in the reconciliation loop", e);
                doSchedule(longCycleMs);
            } finally {
                metrics.evaluated(clock.nanoTime() - startTimeNs);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void doLoop(boolean fullReconciliationCycle) {
        if (pendingTransaction.getState() == Transaction.State.Running) {
            return;
        }

        boolean completedNow = false;
        if (pendingTransaction.getState() == Transaction.State.ResultReady) {
            // Apply data update
            Either<DATA, Throwable> result = pendingTransaction.applyDataChanges(current);

            if (result.hasValue()) {
                current = result.getValue();
                eventProcessor.onNext(current);
            }

            // Complete the external caller.
            pendingTransaction.complete();
            completedNow = true;
        }

        // Trigger actions on engines.
        if (fullReconciliationCycle || completedNow) {
            try {
                // Start next reference change action, if present and exit.
                if (startNextExternalChangeAction()) {
                    return;
                }

                // Run reconciler
                startReconcileAction(reconcilerActionsProvider.apply(current));
            } catch (Exception e) {
                logger.warn("[{}] Unexpected error from reconciliation engine 'triggerActions' method", name, e);
                titusRuntime.getCodeInvariants().unexpectedError("Unexpected error in ReconciliationEngine", e);
            }
        }
    }

    private void cancelPendingActions() {
        if (pendingTransaction.getState() == Transaction.State.Running) {
            pendingTransaction.close();
        }
        ChangeActionHolder<DATA> action;
        while ((action = referenceChangeActions.poll()) != null) {
            if (action.getSubscriberSink() != null && !action.isCancelled()) {
                try {
                    action.getSubscriberSink().error(new IllegalStateException("Reconciliation engine closed"));
                } catch (Exception ignore) {
                }
            }
        }

        eventProcessor.onError(new IllegalStateException("Reconciliation engine closed"));

        ReactorExt.safeDispose(worker);
    }

    private boolean startNextExternalChangeAction() {
        try {
            ChangeActionHolder<DATA> actionHolder;
            Transaction<DATA> transaction = null;
            while (transaction == null && (actionHolder = referenceChangeActions.peek()) != null) {
                // Ignore all unsubscribed actions
                if (actionHolder.isCancelled()) {
                    referenceChangeActions.poll();
                    continue;
                }

                // Create transaction
                try {
                    transaction = new SingleTransaction<>(actionHolder);
                } catch (Exception e) {
                    transaction = new FailedTransaction<>(actionHolder, e);
                }
                referenceChangeActions.poll();
            }

            if (transaction == null) {
                return false;
            }

            pendingTransaction = transaction;
            return true;
        } finally {
            metrics.updateExternalActionQueueSize(referenceChangeActions.size());
        }
    }

    private void startReconcileAction(List<Mono<Function<DATA, DATA>>> reconcileActions) {
        if (reconcileActions.isEmpty()) {
            return;
        }

        // TODO We process first transaction only, as composite transactions are not implemented yet.
        Mono<Function<DATA, DATA>> action = reconcileActions.get(0);
        String transactionId = "" + nextTransactionId.getAndIncrement();

        ChangeActionHolder<DATA> actionHolder = new ChangeActionHolder<>(
                action,
                transactionId,
                clock.wallTime(),
                null
        );
        pendingTransaction = new SingleTransaction<>(actionHolder);
    }
}
