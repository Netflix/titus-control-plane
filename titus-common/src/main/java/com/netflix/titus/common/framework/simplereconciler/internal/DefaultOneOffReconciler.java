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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.OneOffReconciler;
import com.netflix.titus.common.framework.simplereconciler.internal.transaction.Transaction;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;

public class DefaultOneOffReconciler<DATA> implements OneOffReconciler<DATA> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultOneOffReconciler.class);

    private final long quickCycleMs;
    private final long longCycleMs;
    private final Clock clock;
    private final TitusRuntime titusRuntime;

    private final ReconcilerExecutorMetrics metrics;
    private final ReconcilerEngine<DATA> executor;

    private final Scheduler.Worker worker;

    private volatile long lastLongCycleTimestamp;

    private final ReplayProcessor<DATA> eventProcessor = ReplayProcessor.create(1);
    private final Flux<DATA> eventStream = eventProcessor.compose(ReactorExt.badSubscriberHandler(logger));

    public DefaultOneOffReconciler(String id,
                                   DATA initial,
                                   Duration quickCycle,
                                   Duration longCycle,
                                   Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider,
                                   Scheduler scheduler,
                                   TitusRuntime titusRuntime) {
        this.quickCycleMs = quickCycle.toMillis();
        this.longCycleMs = longCycle.toMillis();
        this.worker = scheduler.createWorker();
        this.clock = titusRuntime.getClock();
        this.titusRuntime = titusRuntime;

        this.metrics = new ReconcilerExecutorMetrics(id, titusRuntime);
        this.executor = new ReconcilerEngine<>(id, initial, reconcilerActionsProvider, metrics, titusRuntime);

        eventProcessor.onNext(initial);
        doSchedule(0);
    }

    @Override
    public Mono<Void> close() {
        return Mono.create(sink -> {
            if (executor.getState() == ReconcilerState.Closed) {
                sink.success();
            } else {
                executor.addOnCloseListener(() -> {
                    sink.success();
                    worker.dispose();
                });
                executor.close();
            }
        });
    }

    @Override
    public DATA getCurrent() {
        return executor.getCurrent();
    }

    @Override
    public Mono<DATA> apply(Function<DATA, Mono<DATA>> action) {
        return executor.apply(action);
    }

    @Override
    public Flux<DATA> changes() {
        return eventStream;
    }

    private void doSchedule(long delayMs) {
        worker.schedule(() -> {
            if (executor.getState() == ReconcilerState.Closing) {
                executor.tryToClose().ifPresent(Runnable::run);
            }

            // Self-terminate
            if (executor.getState() == ReconcilerState.Closed) {
                eventProcessor.onComplete();
                worker.dispose();
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
        Transaction<DATA> transactionBefore = executor.getPendingTransaction();

        boolean completedNow;
        if (transactionBefore == null) {
            completedNow = false;
        } else {
            executor.processDataUpdates();
            executor.closeFinishedTransaction().ifPresent(pair -> {
                Optional<DATA> data = pair.getLeft();
                Runnable action = pair.getRight();

                // Emit event first, and run action after, so subscription completes only after events were propagated.
                data.ifPresent(eventProcessor::onNext);
                action.run();
            });

            Transaction<DATA> pendingTransaction = executor.getPendingTransaction();
            if (pendingTransaction != null) {
                return;
            }
            completedNow = true;
        }

        // Trigger actions on engines.
        if (fullReconciliationCycle || completedNow) {
            try {
                // Start next reference change action, if present and exit.
                if (executor.startNextExternalChangeAction()) {
                    return;
                }

                // Run reconciler
                executor.startReconcileAction();
            } catch (Exception e) {
                logger.warn("[{}] Unexpected error from reconciliation engine 'triggerActions' method", executor.getId(), e);
                titusRuntime.getCodeInvariants().unexpectedError("Unexpected error in ReconciliationEngine", e);
            }
        }
    }
}
