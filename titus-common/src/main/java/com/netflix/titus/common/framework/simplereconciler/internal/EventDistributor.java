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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconcilerEvent;
import com.netflix.titus.common.util.ExceptionExt;
import reactor.core.publisher.FluxSink;

/**
 * {@link EventDistributor} takes over event notifications from the internal reconciler stream, and distributes them
 * to registered stream observers. Events notifications are processed by a dedicated thread.
 */
class EventDistributor<DATA> {

    private static final String ROOT_METRIC_NAME = "titus.simpleReconciliation.eventDistributor.";

    private final Supplier<List<SimpleReconcilerEvent<DATA>>> snapshotSupplier;
    private final Registry registry;

    private final LinkedBlockingQueue<EmitterHolder> sinkQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<SimpleReconcilerEvent<DATA>> eventQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger eventQueueDepth = new AtomicInteger();

    // This is modified only by the internal thread. No need to sync. We use map here for fast removal.
    // This collection is observed by Spectator poller, so we have to use ConcurrentMap.
    private final ConcurrentMap<String, EmitterHolder> activeSinks = new ConcurrentHashMap<>();

    private volatile Thread eventLoopThread;
    private volatile boolean shutdown;

    // Metrics
    private final Timer metricLoopExecutionTime;
    private final Counter metricEmittedEvents;

    EventDistributor(Supplier<List<SimpleReconcilerEvent<DATA>>> snapshotSupplier, Registry registry) {
        this.snapshotSupplier = snapshotSupplier;
        this.metricLoopExecutionTime = registry.timer(ROOT_METRIC_NAME + "executionTime");
        this.registry = registry;
        PolledMeter.using(registry).withName(ROOT_METRIC_NAME + "eventQueue").monitorValue(this, self -> self.eventQueueDepth.get());
        PolledMeter.using(registry).withName(ROOT_METRIC_NAME + "activeSubscribers").monitorSize(activeSinks);
        this.metricEmittedEvents = registry.counter(ROOT_METRIC_NAME + "emittedEvents");
    }

    void start() {
        Preconditions.checkState(!shutdown, "Already shutdown");
        if (eventLoopThread == null) {
            this.eventLoopThread = new Thread("event-distributor") {
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
                if (timeoutMs > 0) {
                    try {
                        eventLoopThread.join(timeoutMs);
                    } catch (InterruptedException ignore) {
                    }
                }
            } finally {
                eventLoopThread = null;
            }
        }
    }

    void connectSink(String clientId, FluxSink<List<SimpleReconcilerEvent<DATA>>> sink) {
        // That really should not be needed, but it does not hurt to check.
        String id = clientId;
        while (activeSinks.containsKey(id)) {
            id = clientId + "#" + UUID.randomUUID();
        }
        sinkQueue.add(new EmitterHolder(id, sink));
    }

    void addEvents(List<SimpleReconcilerEvent<DATA>> events) {
        eventQueue.addAll(events);
        eventQueueDepth.accumulateAndGet(events.size(), (current, delta) -> current + delta);
    }

    private void doLoop() {
        while (true) {
            if (shutdown) {
                completeEmitters();
                return;
            }

            Stopwatch start = Stopwatch.createStarted();

            List<SimpleReconcilerEvent<DATA>> events = new ArrayList<>();
            try {
                SimpleReconcilerEvent<DATA> event = eventQueue.poll(1, TimeUnit.MILLISECONDS);
                if (event != null) {
                    events.add(event);
                }
            } catch (InterruptedException ignore) {
            }
            // Build snapshot early before draining the event queue
            List<SimpleReconcilerEvent<DATA>> snapshot = null;
            if (!sinkQueue.isEmpty()) {
                snapshot = snapshotSupplier.get();
            }
            eventQueue.drainTo(events);
            eventQueueDepth.accumulateAndGet(events.size(), (current, delta) -> current - delta);

            // We emit events to already connected sinks first. For new sinks, we build snapshot and merge it with the
            // pending event queue in addNewSinks method.
            processEvents(events);
            removeUnsubscribedSinks();
            if (snapshot != null) {
                addNewSinks(snapshot, events);
            }
            metricEmittedEvents.increment(events.size());

            metricLoopExecutionTime.record(start.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        }
    }

    private void addNewSinks(List<SimpleReconcilerEvent<DATA>> snapshot, List<SimpleReconcilerEvent<DATA>> events) {
        ArrayList<EmitterHolder> newEmitters = new ArrayList<>();
        sinkQueue.drainTo(newEmitters);
        if (!newEmitters.isEmpty()) {
            List<SimpleReconcilerEvent<DATA>> merged = mergeSnapshotAndPendingEvents(snapshot, events);
            newEmitters.forEach(holder -> {
                activeSinks.put(holder.getId(), holder);
                holder.onNext(merged);
            });
        }
    }

    private void removeUnsubscribedSinks() {
        Set<String> cancelled = new HashSet<>();
        activeSinks.forEach((id, holder) -> {
            if (holder.isCancelled()) {
                cancelled.add(id);
            }
        });
        activeSinks.keySet().removeAll(cancelled);
    }

    private List<SimpleReconcilerEvent<DATA>> mergeSnapshotAndPendingEvents(List<SimpleReconcilerEvent<DATA>> snapshot,
                                                                            List<SimpleReconcilerEvent<DATA>> events) {
        if (events.isEmpty()) {
            return snapshot;
        }
        Map<String, SimpleReconcilerEvent<DATA>> eventsById = new HashMap<>();
        events.forEach(event -> eventsById.put(event.getId(), event));
        List<SimpleReconcilerEvent<DATA>> mergeResult = new ArrayList<>();
        for (SimpleReconcilerEvent<DATA> event : snapshot) {
            SimpleReconcilerEvent<DATA> fromQueue = eventsById.get(event.getId());
            if (fromQueue == null || fromQueue.getTransactionId() < event.getTransactionId()) {
                mergeResult.add(event);
            } else if (fromQueue.getKind() != SimpleReconcilerEvent.Kind.Removed) {
                mergeResult.add(new SimpleReconcilerEvent<>(
                        SimpleReconcilerEvent.Kind.Added,
                        fromQueue.getId(),
                        fromQueue.getData(),
                        fromQueue.getTransactionId()
                ));
            }
        }
        return mergeResult;
    }

    private void processEvents(List<SimpleReconcilerEvent<DATA>> events) {
        if (events.isEmpty()) {
            return;
        }
        Set<String> cancelled = new HashSet<>();
        activeSinks.forEach((id, holder) -> {
            if (!holder.onNext(events)) {
                cancelled.add(id);
            }
        });
        activeSinks.keySet().removeAll(cancelled);
    }

    private void completeEmitters() {
        removeUnsubscribedSinks();
        Throwable error = new IllegalStateException("Reconciler framework stream closed");
        activeSinks.forEach((id, holder) -> holder.onError(error));
        activeSinks.clear();
        for (EmitterHolder next; (next = sinkQueue.poll()) != null; ) {
            next.onError(error);
        }
    }

    private class EmitterHolder {

        private final String id;
        private final FluxSink<List<SimpleReconcilerEvent<DATA>>> emitter;
        private volatile boolean cancelled;

        private final Timer metricOnNext;
        private final Timer metricOnError;

        private EmitterHolder(String id, FluxSink<List<SimpleReconcilerEvent<DATA>>> emitter) {
            this.id = id;
            this.emitter = emitter;
            this.metricOnNext = registry.timer(ROOT_METRIC_NAME + "sink", "action", "next", "clientId", id);
            this.metricOnError = registry.timer(ROOT_METRIC_NAME + "sink", "action", "error", "clientId", id);
            emitter.onCancel(() -> cancelled = true);
        }

        private String getId() {
            return id;
        }

        private boolean isCancelled() {
            return cancelled;
        }

        private boolean onNext(List<SimpleReconcilerEvent<DATA>> event) {
            if (cancelled) {
                return false;
            }

            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                emitter.next(event);
                metricOnNext.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                ExceptionExt.silent(() -> emitter.error(e));
                this.cancelled = true;
                metricOnError.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                return false;
            }
            return true;
        }

        private void onError(Throwable error) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                emitter.error(error);
            } catch (Throwable ignore) {
            } finally {
                metricOnError.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                cancelled = true;
            }
        }
    }
}
