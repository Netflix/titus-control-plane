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

package com.netflix.titus.common.util.rx.invoker;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;

/**
 * A simple queue for Spring Reactor {@link Mono} actions, which execution order must be serialized.
 */
public class ReactorSerializedInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(ReactorSerializedInvoker.class);

    private final Duration excessiveRunningTime;
    private final Worker worker;
    private final Scheduler scheduler;
    private final Clock clock;
    private final ReactorSerializedInvokerMetrics metrics;

    private final BlockingQueue<ActionHandler> actionHandlers;
    private ActionHandler pendingAction;

    private volatile boolean shutdownFlag;

    private ReactorSerializedInvoker(String name,
                                     int size,
                                     Duration excessiveRunningTime,
                                     Scheduler scheduler,
                                     Registry registry,
                                     Clock clock) {
        this.excessiveRunningTime = excessiveRunningTime;
        this.worker = scheduler.createWorker();
        this.scheduler = scheduler;
        this.metrics = new ReactorSerializedInvokerMetrics(name, registry);

        this.actionHandlers = new LinkedBlockingQueue<>(size);
        this.clock = clock;
    }

    public void shutdown(Duration timeout) {
        if (shutdownFlag) {
            return;
        }

        this.shutdownFlag = true;
        MonoProcessor<Void> marker = MonoProcessor.create();
        worker.schedule(this::drainOnShutdown);
        worker.schedule(marker::onComplete);
        try {
            marker.block(timeout);
        } finally {
            worker.dispose();
        }
    }

    public Mono<T> submit(Mono<T> action) {
        Preconditions.checkState(!shutdownFlag, "ReactorQueue has been shutdown");
        Preconditions.checkNotNull(action);

        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

        return Mono.create(sink -> {
            metrics.onSubmit();

            ActionHandler actionHandler = new ActionHandler(action, sink, stackTrace);
            if (!actionHandlers.offer(actionHandler)) {
                metrics.onQueueFull();
                sink.error(actionHandler.newException(new IllegalStateException("Queue is full")));
                return;
            }
            metrics.setQueueSize(actionHandlers.size());

            if (shutdownFlag) {
                actionHandler.terminate();
            } else {
                worker.schedule(this::drain);
            }
        });
    }

    /**
     * Draining is performed by the internal event loop worker.
     */
    private void drain() {
        if (shutdownFlag) {
            return;
        }

        if (pendingAction != null && !pendingAction.isTerminated()) {
            return;
        }

        ActionHandler nextAction;
        while ((nextAction = actionHandlers.poll()) != null) {
            if (nextAction.startExecution()) {
                this.pendingAction = nextAction;
                metrics.setQueueSize(actionHandlers.size());
                return;
            }
        }
        metrics.setQueueSize(actionHandlers.size());
        pendingAction = null;
    }

    private void drainOnShutdown() {
        if (pendingAction != null) {
            pendingAction.terminate();
        }
        ActionHandler nextAction;
        while ((nextAction = actionHandlers.poll()) != null) {
            nextAction.terminate();
        }
        metrics.setQueueSize(0);
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<T> {

        private String name;
        private int size;
        private Duration excessiveRunningTime = Duration.ofHours(1);
        private Scheduler scheduler;
        private Registry registry;
        private Clock clock;

        public Builder<T> withName(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> withMaxQueueSize(int size) {
            this.size = size;
            return this;
        }

        public Builder<T> withExcessiveRunningTime(Duration excessiveRunningTime) {
            this.excessiveRunningTime = excessiveRunningTime;
            return this;
        }

        public Builder<T> withScheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder<T> withRegistry(Registry registry) {
            this.registry = registry;
            return this;
        }

        public Builder<T> withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public ReactorSerializedInvoker<T> build() {
            return new ReactorSerializedInvoker<>(name, size, excessiveRunningTime, scheduler, registry, clock);
        }
    }

    private class ActionHandler {

        private final Mono<T> action;
        private final MonoSink<T> sink;
        private final StackTraceElement[] stackTrace;

        private volatile boolean terminated;
        private volatile Disposable.Composite disposable;

        private final long queueTimestamp;
        private long startTimestamp;

        private ActionHandler(Mono<T> action, MonoSink<T> sink, StackTraceElement[] stackTrace) {
            this.action = action;
            this.sink = sink;
            this.stackTrace = stackTrace;
            this.queueTimestamp = clock.wallTime();
        }

        boolean isTerminated() {
            return terminated;
        }

        boolean startExecution() {
            startTimestamp = clock.wallTime();
            metrics.onActionExecutionStarted(startTimestamp - queueTimestamp);

            this.disposable = Disposables.composite();
            sink.onCancel(disposable);

            // Check early in case the sink is already disposed/cancelled.
            if (disposable.isDisposed()) {
                metrics.onActionExecutionDisposed(0);
                return false;
            }

            Disposable actionDisposable = action
                    .timeout(excessiveRunningTime, Mono.error(newException(new TimeoutException("Excessive running time"))))
                    .subscribeOn(scheduler)
                    .doFinally(signalType -> {
                        if (signalType != SignalType.ON_COMPLETE && signalType != SignalType.ON_ERROR) {
                            metrics.onActionExecutionDisposed(clock.wallTime() - startTimestamp);
                        }
                        terminated = true;
                        worker.schedule(ReactorSerializedInvoker.this::drain);
                    })
                    .subscribe(
                            next -> {
                                metrics.onActionCompleted(clock.wallTime() - startTimestamp);
                                try {
                                    sink.success(next);
                                } catch (Exception e) {
                                    logger.warn("Bad subscriber", e);
                                }
                            },
                            e -> {
                                metrics.onActionFailed(clock.wallTime() - startTimestamp, e);
                                try {
                                    sink.error(e);
                                } catch (Exception e2) {
                                    logger.warn("Bad subscriber", e2);
                                }
                            },
                            () -> {
                                metrics.onActionCompleted(clock.wallTime() - startTimestamp);
                                try {
                                    sink.success();
                                } catch (Exception e) {
                                    logger.warn("Bad subscriber", e);
                                }
                            }
                    );
            disposable.add(actionDisposable);

            return true;
        }

        void terminate() {
            if (!terminated) {
                terminated = true;

                if (disposable != null) {
                    disposable.dispose();
                }

                try {
                    sink.error(newException(new IllegalStateException("Queue shutdown")));
                } catch (Exception e) {
                    logger.warn("Bad subscriber", e);
                }
            }
        }

        <E extends Throwable> E newException(E exception) {
            exception.setStackTrace(stackTrace);
            return exception;
        }
    }
}
