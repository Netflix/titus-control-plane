/*
 * Copyright 2021 Netflix, Inc.
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.util.ExceptionExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Emitter;
import rx.Subscription;

/**
 * {@link EventDistributor} takes over event notifications from the internal reconciler stream, and distributes them
 * to registered stream observers. Events notifications are processed by a dedicated thread.
 */
class EventDistributor<EVENT> {

    private static final Logger logger = LoggerFactory.getLogger(EventDistributor.class);

    private static final String ROOT_METRIC_NAME = DefaultReconciliationFramework.ROOT_METRIC_NAME + "eventDistributor.";

    // This collection is observed by Spectator poller, so we have to use ConcurrentMap.
    private final ConcurrentMap<String, EngineHolder> engineHolders = new ConcurrentHashMap<>();

    private final LinkedBlockingQueue<EmitterHolder> emitterQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<EVENT> eventQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger eventQueueDepth = new AtomicInteger();

    // This is modified only by the internal thread. No need to sync. We use map here for fast removal.
    // This collection is observed by Spectator poller, so we have to use ConcurrentMap.
    private final ConcurrentMap<String, EmitterHolder> activeEmitters = new ConcurrentHashMap<>();

    private volatile Thread eventLoopThread;
    private volatile boolean shutdown;

    // Metrics
    private final Timer metricLoopExecutionTime;
    private final Counter metricEmittedEvents;

    EventDistributor(Registry registry) {
        PolledMeter.using(registry).withName(ROOT_METRIC_NAME + "connectedEngines").monitorSize(engineHolders);
        this.metricLoopExecutionTime = registry.timer(ROOT_METRIC_NAME + "executionTime");
        PolledMeter.using(registry).withName(ROOT_METRIC_NAME + "eventQueue").monitorValue(this, self -> self.eventQueueDepth.get());
        PolledMeter.using(registry).withName(ROOT_METRIC_NAME + "activeSubscribers").monitorSize(activeEmitters);
        this.metricEmittedEvents = registry.counter(ROOT_METRIC_NAME + "emittedEvents");
    }

    void start() {
        Preconditions.checkState(!shutdown, "Already shutdown");
        if (eventLoopThread == null) {
            this.eventLoopThread = new Thread(EventDistributor.class.getSimpleName()) {
                @Override
                public void run() {
                    doLoop();
                }
            };
            eventLoopThread.setDaemon(true);
            eventLoopThread.start();
        }
    }

    void stop(long timeoutMs) {
        this.shutdown = true;
        if (eventLoopThread != null) {
            try {
                eventLoopThread.interrupt();
                try {
                    eventLoopThread.join(timeoutMs);
                } catch (InterruptedException ignore) {
                }
            } finally {
                eventLoopThread = null;
            }
        }
    }

    void connectEmitter(Emitter<EVENT> emitter) {
        String id = UUID.randomUUID().toString();
        // That really should not be needed, but it does not hurt to check.
        while (activeEmitters.containsKey(id)) {
            id = UUID.randomUUID().toString();
        }
        emitterQueue.add(new EmitterHolder(id, emitter));
    }

    // Called only by the reconciliation framework thread.
    void connectReconciliationEngine(ReconciliationEngine<EVENT> engine) {
        EngineHolder holder = new EngineHolder(engine);
        engineHolders.put(holder.getId(), holder);
    }

    // Called only by the reconciliation framework thread.
    void removeReconciliationEngine(ReconciliationEngine<EVENT> engine) {
        EngineHolder holder = engineHolders.remove(engine.getReferenceView().getId());
        if (holder != null) {
            holder.unsubscribe();
        }
    }

    private void doLoop() {
        while (true) {
            if (shutdown) {
                completeEmitters();
                return;
            }

            Stopwatch start = Stopwatch.createStarted();

            // Block on the event queue. It is possible that new emitters will be added to the emitterQueue, but we
            // have to drain them only when there are actually events to send.
            List<EVENT> events = new ArrayList<>();
            try {
                EVENT event = eventQueue.poll(1, TimeUnit.MILLISECONDS);
                if (event != null) {
                    events.add(event);
                }
            } catch (InterruptedException ignore) {
            }
            eventQueue.drainTo(events);
            eventQueueDepth.accumulateAndGet(events.size(), (current, delta) -> current - delta);

            addNewEmitters();
            processEvents(events);
            metricEmittedEvents.increment(events.size());

            metricLoopExecutionTime.record(start.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        }
    }

    private void addNewEmitters() {
        ArrayList<EmitterHolder> newEmitters = new ArrayList<>();
        emitterQueue.drainTo(newEmitters);
        newEmitters.forEach(holder -> activeEmitters.put(holder.getId(), holder));
    }

    private void processEvents(List<EVENT> events) {
        events.forEach(event -> {
            Set<String> cancelled = new HashSet<>();
            activeEmitters.forEach((id, holder) -> {
                if (!holder.onNext(event)) {
                    cancelled.add(id);
                }
            });
            activeEmitters.keySet().removeAll(cancelled);
        });
    }

    private void completeEmitters() {
        addNewEmitters();
        Throwable error = new IllegalStateException("Reconciler framework stream closed");
        activeEmitters.forEach((id, holder) -> holder.onError(error));
        activeEmitters.clear();
    }

    private class EmitterHolder {

        private final String id;
        private final Emitter<EVENT> emitter;
        private volatile boolean cancelled;

        private EmitterHolder(String id, Emitter<EVENT> emitter) {
            this.id = id;
            this.emitter = emitter;
            emitter.setCancellation(() -> cancelled = true);
        }

        private String getId() {
            return id;
        }

        private boolean onNext(EVENT event) {
            if (cancelled) {
                return false;
            }

            try {
                emitter.onNext(event);
            } catch (Throwable e) {
                ExceptionExt.silent(() -> emitter.onError(e));
                this.cancelled = true;
                return false;
            }
            return true;
        }

        private void onError(Throwable error) {
            try {
                emitter.onError(error);
            } catch (Throwable ignore) {
            } finally {
                cancelled = true;
            }
        }
    }

    private class EngineHolder {

        private final ReconciliationEngine<EVENT> engine;
        private final Subscription subscription;

        private EngineHolder(ReconciliationEngine<EVENT> engine) {
            this.engine = engine;
            this.subscription = engine.events().subscribe(
                    event -> {
                        eventQueue.add(event);
                        eventQueueDepth.incrementAndGet();
                    },
                    error -> logger.error("Event stream broken: id={}, error={}", getId(), error.getMessage()),
                    () -> logger.info("Event stream completed: id={}", getId())
            );
        }

        private String getId() {
            return engine.getReferenceView().getId();
        }

        private void unsubscribe() {
            subscription.unsubscribe();
        }
    }
}
