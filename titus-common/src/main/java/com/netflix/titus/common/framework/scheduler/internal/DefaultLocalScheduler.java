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

package com.netflix.titus.common.framework.scheduler.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.LocalSchedulerException;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ExecutionId;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;
import com.netflix.titus.common.framework.scheduler.model.event.LocalSchedulerEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleAddedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleRemovedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleUpdateEvent;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class DefaultLocalScheduler implements LocalScheduler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLocalScheduler.class);

    private static final ThreadGroup SCHEDULER_THREAD_GROUP = new ThreadGroup("LocalScheduler");

    private static final Runnable DO_NOTHING = () -> {
    };

    private final long internalLoopIntervalMs;
    private final Clock clock;
    private final Registry registry;
    private final Scheduler scheduler;
    private final Scheduler.Worker worker;

    private final BlockingQueue<ScheduleHolder> newHolders = new LinkedBlockingQueue<>();
    private final ConcurrentMap<String, ScheduleHolder> activeHoldersById = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Schedule> archivedSchedulesById = new ConcurrentHashMap<>();
    private final DirectProcessor<LocalSchedulerEvent> eventProcessor = DirectProcessor.create();
    private final SchedulerMetrics metrics;
    private final Disposable transactionLoggerDisposable;

    public DefaultLocalScheduler(Duration internalLoopInterval, Scheduler scheduler, Clock clock, boolean localSchedulerLogDisabled, Registry registry) {
        this.internalLoopIntervalMs = internalLoopInterval.toMillis();
        this.scheduler = scheduler;
        this.clock = clock;
        this.registry = registry;
        this.worker = scheduler.createWorker();
        this.metrics = new SchedulerMetrics(this, clock, registry);
        this.transactionLoggerDisposable = localSchedulerLogDisabled ? Disposables.disposed() : LocalSchedulerTransactionLogger.logEvents(this);

        scheduleNextIteration();
    }

    public void shutdown() {
        worker.dispose();
        metrics.shutdown();
        ReactorExt.safeDispose(transactionLoggerDisposable);
    }

    @Override
    public List<Schedule> getActiveSchedules() {
        Map<String, Schedule> all = new HashMap<>();
        activeHoldersById.forEach((id, h) -> all.put(id, h.getSchedule()));
        newHolders.forEach(h -> all.put(h.getSchedule().getId(), h.getSchedule()));
        return new ArrayList<>(all.values());
    }

    @Override
    public List<Schedule> getArchivedSchedules() {
        return new ArrayList<>(archivedSchedulesById.values());
    }

    @Override
    public Optional<Schedule> findSchedule(String scheduleId) {
        ScheduleHolder holder = activeHoldersById.get(scheduleId);
        if (holder == null) {
            holder = newHolders.stream().filter(h -> h.getSchedule().getId().equals(scheduleId)).findFirst().orElse(null);
        }
        return Optional.ofNullable(holder).map(ScheduleHolder::getSchedule);
    }

    @Override
    public Flux<LocalSchedulerEvent> events() {
        return eventProcessor;
    }

    @Override
    public ScheduleReference scheduleMono(ScheduleDescriptor scheduleDescriptor, Function<ExecutionContext, Mono<Void>> actionProducer, Scheduler scheduler) {
        return scheduleInternal(scheduleDescriptor, actionProducer, scheduler, DO_NOTHING);
    }

    @Override
    public ScheduleReference schedule(ScheduleDescriptor scheduleDescriptor, Consumer<ExecutionContext> action, boolean isolated) {
        Scheduler actionScheduler;
        Runnable cleanup;
        if (isolated) {
            Executor executor = Executors.newFixedThreadPool(1, r -> new Thread(SCHEDULER_THREAD_GROUP, r, scheduleDescriptor.getName()));
            actionScheduler = Schedulers.fromExecutor(executor);
            cleanup = ((ExecutorService) executor)::shutdown;
        } else {
            actionScheduler = this.scheduler;
            cleanup = DO_NOTHING;
        }

        return scheduleInternal(scheduleDescriptor, executionContext -> Mono.defer(() -> {
            try {
                action.accept(executionContext);
                return Mono.empty();
            } catch (Exception e) {
                return Mono.error(e);
            }
        }), actionScheduler, cleanup);
    }

    private ScheduleReference scheduleInternal(ScheduleDescriptor descriptor, Function<ExecutionContext, Mono<Void>> actionProducer, Scheduler scheduler, Runnable cleanup) {
        String scheduleId = UUID.randomUUID().toString();

        ScheduleHolder scheduleHolder = new ScheduleHolder(scheduleId, descriptor, actionProducer, scheduler, cleanup);
        newHolders.add(scheduleHolder);

        return scheduleHolder.getReference();
    }

    @Override
    public Mono<Void> cancel(String scheduleId) {
        return ReactorExt.onWorker(() -> {
            ScheduleHolder holder = activeHoldersById.get(scheduleId);
            if (holder == null) {
                throw LocalSchedulerException.scheduleNotFound(scheduleId);
            }
            holder.cancel();
        }, worker);
    }

    private void scheduleNextIteration() {
        worker.schedule(this::doRun, internalLoopIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void doRun() {
        Stopwatch timer = Stopwatch.createStarted();

        List<ScheduleHolder> holders = new ArrayList<>();
        newHolders.drainTo(holders);
        holders.forEach(h -> {
            activeHoldersById.put(h.getSchedule().getId(), h);
            eventProcessor.onNext(new ScheduleAddedEvent(h.getSchedule()));
        });

        try {
            activeHoldersById.values().forEach(ScheduleHolder::handleExecution);
        } catch (Exception e) {
            logger.warn("Unexpected error in the internal scheduler loop", e);
        } finally {
            scheduleNextIteration();
            metrics.recordEvaluationTime(timer.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private class ScheduleHolder {

        private final Runnable cleanup;
        private final ScheduleReference reference;

        private volatile ScheduledActionExecutor executor;
        private volatile boolean closed;

        private ScheduleHolder(String scheduleId,
                               ScheduleDescriptor descriptor,
                               Function<ExecutionContext, Mono<Void>> actionProducer,
                               Scheduler scheduler,
                               Runnable cleanup) {
            ScheduledAction firstAction = ScheduledAction.newBuilder()
                    .withId(scheduleId)
                    .withStatus(SchedulingStatus.newBuilder()
                            .withState(SchedulingState.Waiting)
                            .withExpectedStartTime(clock.wallTime() + descriptor.getInterval().toMillis())
                            .withTimestamp(clock.wallTime())
                            .build()
                    )
                    .withIteration(ExecutionId.initial())
                    .build();
            Schedule schedule = Schedule.newBuilder()
                    .withId(scheduleId)
                    .withDescriptor(descriptor)
                    .withCurrentAction(firstAction)
                    .withCompletedActions(Collections.emptyList())
                    .build();
            this.executor = new ScheduledActionExecutor(schedule, new ScheduleMetrics(schedule, clock, registry), actionProducer, scheduler, clock);

            this.cleanup = cleanup;
            this.reference = new ScheduleReference() {

                @Override
                public Schedule getSchedule() {
                    return executor.getSchedule();
                }

                @Override
                public boolean isClosed() {
                    return closed && executor.getAction().getStatus().getState().isFinal();
                }

                @Override
                public void close() {
                    cancel();
                }
            };
        }

        private Schedule getSchedule() {
            return executor.getSchedule();
        }

        private ScheduleReference getReference() {
            return reference;
        }

        private void cancel() {
            if (closed) {
                return;
            }
            closed = true;
            if (executor.cancel()) {
                eventProcessor.onNext(new ScheduleUpdateEvent(executor.getSchedule()));
            }
        }

        private void handleExecution() {
            if (!executor.handleExecution()) {
                return;
            }
            Schedule currentSchedule = executor.getSchedule();
            eventProcessor.onNext(new ScheduleUpdateEvent(currentSchedule));

            SchedulingState currentState = executor.getAction().getStatus().getState();
            if (currentState.isFinal()) {
                if (closed) {
                    doCleanup();
                } else {
                    this.executor = executor.nextScheduledActionExecutor(currentState == SchedulingState.Failed);
                    eventProcessor.onNext(new ScheduleUpdateEvent(executor.getSchedule()));
                }
            }
        }

        private void doCleanup() {
            Schedule schedule = executor.getSchedule();
            try {
                cleanup.run();
            } catch (Exception e) {
                logger.warn("Cleanup action failed for schedule: name={}", schedule.getDescriptor().getName(), e);
            } finally {
                activeHoldersById.remove(schedule.getId());
                archivedSchedulesById.put(schedule.getId(), schedule);
                eventProcessor.onNext(new ScheduleRemovedEvent(schedule));
            }
        }
    }
}
