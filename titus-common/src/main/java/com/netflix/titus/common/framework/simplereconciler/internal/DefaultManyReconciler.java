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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.ManyReconciler;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconcilerEvent;
import com.netflix.titus.common.framework.simplereconciler.internal.transaction.Transaction;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

public class DefaultManyReconciler<DATA> implements ManyReconciler<DATA> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultManyReconciler.class);

    private static final IllegalStateException EXCEPTION_CLOSED = new IllegalStateException("Reconciler closed");

    private final Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider;
    private final long quickCycleMs;
    private final long longCycleMs;
    private final Scheduler notificationScheduler;
    private final Clock clock;
    private final TitusRuntime titusRuntime;

    private final BlockingQueue<AddHolder> addHolders = new LinkedBlockingQueue<>();
    private final Map<String, ReconcilerEngine<DATA>> executors = new ConcurrentHashMap<>();

    private BlockingQueue<EventListenerHolder> eventListenerHolders = new LinkedBlockingQueue<>();

    private final AtomicReference<ReconcilerState> stateRef = new AtomicReference<>(ReconcilerState.Running);
    private final Scheduler.Worker reconcilerWorker;

    private final ReconcilerExecutorMetrics metrics;

    private volatile long lastLongCycleTimestamp;

    private final DirectProcessor<List<SimpleReconcilerEvent<DATA>>> eventProcessor;
    private final Flux<List<SimpleReconcilerEvent<DATA>>> eventStream;
    private final MonoProcessor<Void> closedProcessor = MonoProcessor.create();

    public DefaultManyReconciler(
            Duration quickCycle,
            Duration longCycle,
            Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider,
            Scheduler reconcilerScheduler,
            Scheduler notificationScheduler,
            TitusRuntime titusRuntime) {
        this.quickCycleMs = quickCycle.toMillis();
        this.longCycleMs = longCycle.toMillis();
        this.reconcilerActionsProvider = reconcilerActionsProvider;
        this.notificationScheduler = notificationScheduler;
        this.clock = titusRuntime.getClock();
        this.titusRuntime = titusRuntime;

        this.reconcilerWorker = reconcilerScheduler.createWorker();
        this.metrics = new ReconcilerExecutorMetrics(titusRuntime);

        eventProcessor = DirectProcessor.create();
        eventStream = eventProcessor.compose(ReactorExt.badSubscriberHandler(logger))
                .subscribeOn(notificationScheduler)
                .publishOn(notificationScheduler);

        doSchedule(0);
    }

    @Override
    public Mono<Void> add(String id, DATA initial) {
        return Mono.<Void>create(sink -> {
            if (stateRef.get() != ReconcilerState.Running) {
                sink.error(EXCEPTION_CLOSED);
            } else {
                AddHolder holder = new AddHolder(id, initial, sink);
                addHolders.add(holder);

                // Check again to deal with race condition during shutdown process
                if (stateRef.get() != ReconcilerState.Running) {
                    sink.error(EXCEPTION_CLOSED);
                }
            }
        }).publishOn(notificationScheduler);
    }

    @Override
    public Mono<Void> remove(String id) {
        return Mono.<Void>create(sink -> {
            if (stateRef.get() != ReconcilerState.Running) {
                sink.error(EXCEPTION_CLOSED);
            } else {
                ReconcilerEngine<DATA> executor = executors.get(id);
                if (executor == null) {
                    sink.error(new IllegalArgumentException("Reconciler not found for data item " + id));
                } else if (executor.getState() == ReconcilerState.Closed) {
                    // If already closed, terminate immediately
                    sink.success();
                } else {
                    executor.addOnCloseListener(sink::success);
                    executor.close();
                }
            }
        }).publishOn(notificationScheduler);
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            stateRef.compareAndSet(ReconcilerState.Running, ReconcilerState.Closing);
            return closedProcessor;
        });
    }

    @Override
    public Map<String, DATA> getAll() {
        Map<String, DATA> all = new HashMap<>();
        executors.forEach((id, executor) -> all.put(id, executor.getCurrent()));
        return all;
    }

    @Override
    public Optional<DATA> findById(String id) {
        return Optional.ofNullable(Evaluators.applyNotNull(executors.get(id), ReconcilerEngine::getCurrent));
    }

    @Override
    public Mono<DATA> apply(String id, Function<DATA, Mono<DATA>> action) {
        return Mono.defer(() -> {
            ReconcilerEngine<DATA> executor = executors.get(id);
            if (executor == null) {
                return Mono.error(new IllegalArgumentException("Reconciler not found for data item " + id));
            }
            return executor.apply(action);
        });
    }

    @Override
    public Flux<List<SimpleReconcilerEvent<DATA>>> changes() {
        return Flux.<List<SimpleReconcilerEvent<DATA>>>create(sink -> {
            if (stateRef.get() != ReconcilerState.Running) {
                sink.error(EXCEPTION_CLOSED);
            } else {
                EventListenerHolder holder = new EventListenerHolder(sink);
                eventListenerHolders.add(holder);

                // Check again to deal with race condition during shutdown process
                if (stateRef.get() != ReconcilerState.Running) {
                    sink.error(EXCEPTION_CLOSED);
                }
            }
        }).publishOn(notificationScheduler);
    }

    private void doSchedule(long delayMs) {
        reconcilerWorker.schedule(() -> {
            // Self-terminate
            if (stateRef.get() == ReconcilerState.Closed) {
                reconcilerWorker.dispose();
                return;
            }

            if (stateRef.get() == ReconcilerState.Closing && tryClose()) {
                reconcilerWorker.dispose();
                return;
            }

            connectEventListeners();

            long startTimeMs = clock.wallTime();
            long startTimeNs = clock.nanoTime();
            boolean fullCycle = (startTimeMs - lastLongCycleTimestamp) >= longCycleMs;

            if (fullCycle) {
                lastLongCycleTimestamp = startTimeMs;
            }

            try {
                doProcess(fullCycle);
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

    private boolean tryClose() {
        // Cancel newly added
        for (AddHolder holder; (holder = addHolders.poll()) != null; ) {
            holder.getSink().error(EXCEPTION_CLOSED);
        }

        // Cancel newly added event listeners
        for (EventListenerHolder holder; (holder = eventListenerHolders.poll()) != null; ) {
            holder.getSink().error(EXCEPTION_CLOSED);
        }

        // Close active executors.
        executors.forEach((id, executor) -> {
            if (executor.getState() == ReconcilerState.Running) {
                executor.close();
            }
        });

        if (addHolders.isEmpty() && executors.isEmpty()) {
            stateRef.set(ReconcilerState.Closed);
            eventProcessor.onError(EXCEPTION_CLOSED);
            closedProcessor.onComplete();
        }

        return stateRef.get() == ReconcilerState.Closed;
    }

    private void connectEventListeners() {
        if (eventListenerHolders.isEmpty()) {
            return;
        }

        for (EventListenerHolder holder; (holder = eventListenerHolders.poll()) != null; ) {
            FluxSink<List<SimpleReconcilerEvent<DATA>>> sink = holder.getSink();

            // We build snapshot only after the subscription to 'eventStream' happens, otherwise we might loose events
            // due to fact that subscription happening on the 'notification' thread may take some time.
            Disposable disposable = eventStream
                    .compose(ReactorExt.head(() -> Collections.singleton(buildSnapshot())))
                    .subscribe(
                            sink::next,
                            sink::error
                    );
            sink.onCancel(disposable);
        }
    }

    private List<SimpleReconcilerEvent<DATA>> buildSnapshot() {
        List<SimpleReconcilerEvent<DATA>> allState = new ArrayList<>();
        executors.forEach((id, executor) -> {
            // We emit event with the id of the last completed transaction.
            String transactionId = "" + (executor.getNextTransactionId() - 1);
            SimpleReconcilerEvent<DATA> event = new SimpleReconcilerEvent<>(
                    SimpleReconcilerEvent.Kind.Added,
                    executor.getId(),
                    executor.getCurrent(),
                    transactionId
            );
            allState.add(event);
        });
        return allState;
    }

    private void doProcess(boolean fullReconciliationCycle) {
        // Data update
        executors.forEach((id, executor) -> executor.processDataUpdates());

        // Complete subscribers
        Set<String> justChanged = new HashSet<>();
        executors.forEach((id, executor) -> {
            Transaction<DATA> transactionBefore = executor.getPendingTransaction();
            if (transactionBefore != null) {
                executor.closeFinishedTransaction().ifPresent(pair -> {
                    Optional<DATA> data = pair.getLeft();
                    Runnable action = pair.getRight();

                    // Emit event first, and run action after, so subscription completes only after events were propagated.
                    data.ifPresent(t -> eventProcessor.onNext(
                            Collections.singletonList(new SimpleReconcilerEvent<>(
                                    SimpleReconcilerEvent.Kind.Updated,
                                    executor.getId(),
                                    t,
                                    transactionBefore.getActionHolder().getTransactionId()
                            ))
                    ));
                    action.run();
                });
                if (executor.getPendingTransaction() == null) {
                    justChanged.add(executor.getId());
                }
            }
        });

        // Remove closed executors.
        for (Iterator<ReconcilerEngine<DATA>> it = executors.values().iterator(); it.hasNext(); ) {
            ReconcilerEngine<DATA> executor = it.next();
            if (executor.getState() == ReconcilerState.Closing) {
                Optional<Runnable> runnable = executor.tryToClose();
                if (executor.getState() == ReconcilerState.Closed) {
                    it.remove();

                    // This is the very last action, so we can take safely the next transaction id without progressing it.
                    String transactionId = "" + executor.getNextTransactionId();
                    eventProcessor.onNext(Collections.singletonList(
                            new SimpleReconcilerEvent<>(SimpleReconcilerEvent.Kind.Removed, executor.getId(), executor.getCurrent(), transactionId)
                    ));

                    runnable.ifPresent(Runnable::run);
                }
            }
        }

        // Add new executors
        for (AddHolder holder; (holder = addHolders.poll()) != null; ) {
            ReconcilerEngine<DATA> executor = holder.getExecutor();
            executors.put(holder.getId(), executor);

            // We set transaction id "0" for the newly added executors.
            eventProcessor.onNext(Collections.singletonList(
                    new SimpleReconcilerEvent<>(SimpleReconcilerEvent.Kind.Added, executor.getId(), executor.getCurrent(), "0")
            ));

            holder.getSink().success();
            justChanged.add(holder.getId());
        }

        // Trigger actions on executors.
        executors.forEach((id, executor) -> {
            if (executor.getPendingTransaction() == null) {
                if (fullReconciliationCycle || justChanged.contains(id)) {
                    try {
                        // Start next reference change action, if present and exit.
                        if (executor.startNextExternalChangeAction()) {
                            return;
                        }

                        // Run reconciler
                        executor.startReconcileAction();
                    } catch (Exception e) {
                        logger.warn("[{}] Unexpected error from reconciliation executor", executor.getId(), e);
                        titusRuntime.getCodeInvariants().unexpectedError("Unexpected error in reconciliation executor", e);
                    }
                }
            }
        });
    }

    private class AddHolder {

        private final ReconcilerEngine<DATA> executor;
        private final MonoSink<Void> sink;

        private AddHolder(String id, DATA initial, MonoSink<Void> sink) {
            this.sink = sink;
            this.executor = new ReconcilerEngine<>(
                    id,
                    initial,
                    reconcilerActionsProvider,
                    metrics,
                    titusRuntime
            );
        }

        private String getId() {
            return executor.getId();
        }

        private ReconcilerEngine<DATA> getExecutor() {
            return executor;
        }

        private MonoSink<Void> getSink() {
            return sink;
        }
    }

    private class EventListenerHolder {

        private final FluxSink<List<SimpleReconcilerEvent<DATA>>> sink;

        private EventListenerHolder(FluxSink<List<SimpleReconcilerEvent<DATA>>> sink) {
            this.sink = sink;
        }

        private FluxSink<List<SimpleReconcilerEvent<DATA>>> getSink() {
            return sink;
        }
    }
}
