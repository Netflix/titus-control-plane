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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.MultiEngineChangeAction;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

public class DefaultReconciliationFramework<EVENT> implements ReconciliationFramework<EVENT> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultReconciliationFramework.class);

    private static final String ROOT_METRIC_NAME = "titus.reconciliation.framework.";
    private static final String LOOP_EXECUTION_TIME_METRIC = ROOT_METRIC_NAME + "executionTime";
    private static final String LAST_EXECUTION_TIME_METRIC = ROOT_METRIC_NAME + "lastExecutionTime";
    private static final String LAST_FULL_CYCLE_EXECUTION_TIME_METRIC = ROOT_METRIC_NAME + "lastFullCycleExecutionTime";

    private final Function<EntityHolder, InternalReconciliationEngine<EVENT>> engineFactory;
    private final long idleTimeoutMs;
    private final long activeTimeoutMs;

    private final ExecutorService executor;
    private final Scheduler scheduler;

    private final Set<InternalReconciliationEngine<EVENT>> engines = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final BlockingQueue<Pair<InternalReconciliationEngine<EVENT>, Subscriber<ReconciliationEngine>>> enginesAdded = new LinkedBlockingQueue<>();
    private final BlockingQueue<Pair<InternalReconciliationEngine<EVENT>, Subscriber<Void>>> enginesToRemove = new LinkedBlockingQueue<>();

    private final AtomicReference<Map<String, InternalReconciliationEngine<EVENT>>> idToEngineMapRef = new AtomicReference<>(Collections.emptyMap());
    private IndexSet<EntityHolder> indexSet;

    private final Scheduler.Worker worker;

    private volatile boolean runnable = true;
    private volatile boolean started = false;

    private final PublishSubject<Observable<EVENT>> eventsMergeSubject = PublishSubject.create();
    private final Observable<EVENT> eventsObservable;
    private final Subscription internalEventSubscription;

    private final Timer loopExecutionTime;
    private volatile long lastFullCycleExecutionTimeMs; // Probed by a polled meter.
    private volatile long lastExecutionTimeMs; // Probed by a polled meter.

    private final Object multiEngineChangeLock = new Object();

    public DefaultReconciliationFramework(List<InternalReconciliationEngine<EVENT>> bootstrapEngines,
                                          Function<EntityHolder, InternalReconciliationEngine<EVENT>> engineFactory,
                                          long idleTimeoutMs,
                                          long activeTimeoutMs,
                                          Map<Object, Comparator<EntityHolder>> indexComparators,
                                          Registry registry,
                                          Optional<Scheduler> optionalScheduler) {
        Preconditions.checkArgument(idleTimeoutMs > 0, "idleTimeout <= 0 (%s)", idleTimeoutMs);
        Preconditions.checkArgument(activeTimeoutMs <= idleTimeoutMs, "activeTimeout(%s) > idleTimeout(%s)", activeTimeoutMs, idleTimeoutMs);

        this.engineFactory = engineFactory;
        this.indexSet = IndexSet.newIndexSet(indexComparators);

        this.idleTimeoutMs = idleTimeoutMs;
        this.activeTimeoutMs = activeTimeoutMs;

        if (optionalScheduler.isPresent()) {
            this.scheduler = optionalScheduler.get();
            this.executor = null;
        } else {
            this.executor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "TitusReconciliationFramework");
                thread.setDaemon(true);
                return thread;
            });
            this.scheduler = Schedulers.from(executor);
        }

        this.worker = scheduler.createWorker();
        this.eventsObservable = Observable.merge(eventsMergeSubject).share();

        // To keep eventsObservable permanently active.
        this.internalEventSubscription = eventsObservable.subscribe(ObservableExt.silentSubscriber());

        this.loopExecutionTime = registry.timer(LOOP_EXECUTION_TIME_METRIC);
        this.lastFullCycleExecutionTimeMs = scheduler.now() - idleTimeoutMs;
        this.lastExecutionTimeMs = scheduler.now();
        PolledMeter.using(registry).withName(LAST_EXECUTION_TIME_METRIC).monitorValue(this, self -> scheduler.now() - self.lastExecutionTimeMs);
        PolledMeter.using(registry).withName(LAST_FULL_CYCLE_EXECUTION_TIME_METRIC).monitorValue(this, self -> scheduler.now() - self.lastFullCycleExecutionTimeMs);

        engines.addAll(bootstrapEngines);
        bootstrapEngines.forEach(engine -> eventsMergeSubject.onNext(engine.events()));

        updateIndexSet();
    }

    @Override
    public void start() {
        Preconditions.checkArgument(!started, "Framework already started");
        started = true;
        doSchedule(0);
    }

    @Override
    public boolean stop(long timeoutMs) {
        runnable = false;

        // In the test code when we use the TestScheduler we would always block here. One way to solve this is to return
        // Completable as a result, but this makes the API inconvenient. Instead we chose to look at the worker type,
        // and handle this differently for TestScheduler.
        if (worker.getClass().getName().contains("TestScheduler")) {
            stopEngines();
            return true;
        }

        // Run this on internal thread, just like other actions.
        CountDownLatch latch = new CountDownLatch(1);
        worker.schedule(() -> {
            stopEngines();
            latch.countDown();
        });
        ExceptionExt.silent(() -> latch.await(timeoutMs, TimeUnit.MILLISECONDS));

        internalEventSubscription.unsubscribe();

        if (executor != null) {
            executor.shutdownNow();
        }

        return latch.getCount() == 0;
    }

    private void stopEngines() {
        engines.forEach(e -> {
            if (e instanceof DefaultReconciliationEngine) {
                ((DefaultReconciliationEngine) e).shutdown();
            }
        });
        engines.clear();
    }

    @Override
    public Observable<ReconciliationEngine<EVENT>> newEngine(EntityHolder bootstrapModel) {
        return Observable.unsafeCreate(subscriber -> {
            if (!runnable) {
                subscriber.onError(new IllegalStateException("Reconciliation engine is stopped"));
                return;
            }
            InternalReconciliationEngine newEngine = engineFactory.apply(bootstrapModel);
            enginesAdded.add(Pair.of(newEngine, (Subscriber<ReconciliationEngine>) subscriber));
        });
    }

    @Override
    public Completable removeEngine(ReconciliationEngine engine) {
        Preconditions.checkArgument(engine instanceof InternalReconciliationEngine, "Unexpected ReconciliationEngine implementation");

        return Observable.<Void>unsafeCreate(subscriber -> {
            if (!runnable) {
                subscriber.onError(new IllegalStateException("Reconciliation engine is stopped"));
                return;
            }
            enginesToRemove.add(Pair.of((InternalReconciliationEngine<EVENT>) engine, (Subscriber<Void>) subscriber));
        }).toCompletable();
    }

    @Override
    public Observable<Void> changeReferenceModel(MultiEngineChangeAction multiEngineChangeAction,
                                                 BiFunction<String, Observable<List<ModelActionHolder>>, ChangeAction> engineChangeActionFactory,
                                                 String... rootEntityHolderIds) {
        Preconditions.checkArgument(rootEntityHolderIds.length > 1,
                "Change action for multiple engines requested, but %s root id holders provided", rootEntityHolderIds.length
        );

        return Observable.create(emitter -> {

            List<ReconciliationEngine<EVENT>> engines = new ArrayList<>();
            for (String id : rootEntityHolderIds) {
                ReconciliationEngine<EVENT> engine = findEngineByRootId(id).orElseThrow(() -> new IllegalArgumentException("Reconciliation engine not found: rootId=" + id));
                engines.add(engine);
            }

            List<Observable<Map<String, List<ModelActionHolder>>>> outputs = ObservableExt.demultiplex(multiEngineChangeAction.apply(), engines.size());
            List<Observable<Void>> engineActions = new ArrayList<>();
            for (int i = 0; i < engines.size(); i++) {
                ReconciliationEngine<EVENT> engine = engines.get(i);
                String rootId = engine.getReferenceView().getId();
                ChangeAction engineAction = engineChangeActionFactory.apply(rootId, outputs.get(i).map(r -> r.get(rootId)));
                engineActions.add(engine.changeReferenceModel(engineAction));
            }

            // Synchronize on subscription to make sure that this operation is not interleaved with concurrent
            // subscriptions for the same set or subset of the reconciliation engines. The interleaving might result
            // in a deadlock. For example with two engines engineA and engineB:
            // - multi-engine change action M1 for engineA and engineB is scheduled
            // - M1/engineA is added to its queue
            // - another multi-engine change action M2 for engineA and engineB is scheduled
            // - M2/engineB is added to its queue
            // - M1/engineB is added to its queue, and next M2/engineA
            // Executing M1 requires that both M1/engineA and M1/engineB are at the top of the queue, but in this case
            // M2/engineB is ahead of the M1/engineB. On the other hand, M1/engineA is ahead of M2/engineB. Because
            // of that we have deadlock. Please, note that we can ignore here the regular (engine scoped) change actions.
            Subscription subscription;
            synchronized (multiEngineChangeLock) {
                subscription = Observable.mergeDelayError(engineActions).subscribe(
                        emitter::onNext,
                        emitter::onError,
                        emitter::onCompleted
                );
            }
            emitter.setSubscription(subscription);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Observable<EVENT> events() {
        return ObservableExt.protectFromMissingExceptionHandlers(eventsObservable, logger);
    }

    @Override
    public Optional<ReconciliationEngine<EVENT>> findEngineByRootId(String id) {
        InternalReconciliationEngine<EVENT> engine = idToEngineMapRef.get().get(id);
        if (engine == null) {
            return Optional.empty();
        }
        return engine.getReferenceView().getId().equals(id) ? Optional.of(engine) : Optional.empty();
    }

    @Override
    public Optional<Pair<ReconciliationEngine<EVENT>, EntityHolder>> findEngineByChildId(String childId) {
        InternalReconciliationEngine<EVENT> engine = idToEngineMapRef.get().get(childId);
        if (engine == null) {
            return Optional.empty();
        }
        EntityHolder rootHolder = engine.getReferenceView();
        if (rootHolder.getId().equals(childId)) {
            return Optional.empty();
        }
        return rootHolder.findChildById(childId).map(c -> Pair.of(engine, c));
    }

    @Override
    public <ORDER_BY> List<EntityHolder> orderedView(ORDER_BY orderingCriteria) {
        return indexSet.getOrdered(orderingCriteria);
    }

    private void doSchedule(long delayMs) {
        if (!runnable) {
            return;
        }
        worker.schedule(() -> {
            long startTimeMs = worker.now();
            try {
                boolean fullCycle = (startTimeMs - lastFullCycleExecutionTimeMs) >= idleTimeoutMs;
                if (fullCycle) {
                    lastFullCycleExecutionTimeMs = startTimeMs;
                }

                doLoop(fullCycle);

                doSchedule(activeTimeoutMs);
            } catch (Exception e) {
                logger.warn("Unexpected error in the reconciliation loop", e);
                doSchedule(idleTimeoutMs);
            } finally {
                long now = worker.now();
                lastExecutionTimeMs = now;
                loopExecutionTime.record(now - startTimeMs, TimeUnit.MILLISECONDS);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void doLoop(boolean fullReconciliationCycle) {
        Set<InternalReconciliationEngine<EVENT>> mustRunEngines = new HashSet<>();

        // Apply pending model updates/send events
        boolean modelUpdates = false;
        for (InternalReconciliationEngine engine : engines) {
            try {
                boolean anyChange = engine.applyModelUpdates();
                modelUpdates = modelUpdates || anyChange;
            } catch (Exception e) {
                logger.warn("Unexpected error from reconciliation engine 'applyModelUpdates' method", e);
            }
        }

        // Add new engines.
        List<Pair<InternalReconciliationEngine<EVENT>, Subscriber<ReconciliationEngine>>> recentlyAdded = new ArrayList<>();
        enginesAdded.drainTo(recentlyAdded);
        recentlyAdded.forEach(pair -> {
            InternalReconciliationEngine<EVENT> newEngine = pair.getLeft();
            engines.add(newEngine);
            mustRunEngines.add(newEngine);
            eventsMergeSubject.onNext(newEngine.events());
        });

        // Remove engines.
        List<Pair<InternalReconciliationEngine<EVENT>, Subscriber<Void>>> recentlyRemoved = new ArrayList<>();
        enginesToRemove.drainTo(recentlyRemoved);
        shutdownEnginesToRemove(recentlyRemoved);

        boolean engineSetUpdate = !recentlyAdded.isEmpty() || !recentlyRemoved.isEmpty();

        // Update indexes if there are model changes.
        if (modelUpdates || engineSetUpdate) {
            updateIndexSet();
        }

        // Complete engine add/remove subscribers.
        // We want to complete the subscribers that create new engines, before the first event is emitted.
        // Otherwise the initial event would be emitted immediately, before the subscriber has a chance to subscriber to the event stream.
        recentlyAdded.forEach(pair -> {
            Subscriber<ReconciliationEngine> subscriber = pair.getRight();
            if (!subscriber.isUnsubscribed()) {
                subscriber.onNext(pair.getLeft());
                subscriber.onCompleted();
            }
        });
        recentlyRemoved.forEach(pair -> pair.getRight().onCompleted());

        // Emit events
        for (InternalReconciliationEngine engine : engines) {
            try {
                engine.emitEvents();
            } catch (Exception e) {
                logger.warn("Unexpected error from reconciliation engine 'emitEvents' method", e);
            }
        }

        // Complete ChangeAction subscribers
        for (InternalReconciliationEngine<EVENT> engine : engines) {
            try {
                if (engine.closeFinishedTransactions()) {
                    mustRunEngines.add(engine);
                }
            } catch (Exception e) {
                logger.warn("Unexpected error from reconciliation engine 'closeFinishedTransactions' method", e);
            }
        }

        // Trigger actions on engines.
        for (InternalReconciliationEngine engine : engines) {
            if (fullReconciliationCycle || engine.hasPendingTransactions() || mustRunEngines.contains(engine)) {
                try {
                    engine.triggerActions();
                } catch (Exception e) {
                    logger.warn("Unexpected error from reconciliation engine 'triggerActions' method", e);
                }
            }
        }
    }

    private void shutdownEnginesToRemove(List<Pair<InternalReconciliationEngine<EVENT>, Subscriber<Void>>> toRemove) {
        toRemove.forEach(pair -> {
            InternalReconciliationEngine e = pair.getLeft();
            if (e instanceof DefaultReconciliationEngine) {
                ((DefaultReconciliationEngine) e).shutdown();
            }
            engines.remove(e);
        });
    }

    private void updateIndexSet() {
        Map<String, InternalReconciliationEngine<EVENT>> idToEngineMap = new HashMap<>();
        engines.forEach(engine -> engine.getReferenceView().visit(h -> idToEngineMap.put(h.getId(), engine)));
        this.idToEngineMapRef.set(idToEngineMap);

        indexSet = indexSet.apply(engines.stream().map(ReconciliationEngine::getReferenceView).collect(Collectors.toList()));
    }
}
