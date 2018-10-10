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

import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.LocalSchedulerException;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;
import com.netflix.titus.common.framework.scheduler.model.event.LocalSchedulerEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleAddedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleRemovedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleUpdateEvent;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final int SCHEDULE_HISTORY_LIMIT = 20;

    private final long internalLoopIntervalMs;
    private final Clock clock;
    private final Scheduler scheduler;
    private final Scheduler.Worker worker;

    private final BlockingQueue<ScheduleHolder> newHolders = new LinkedBlockingQueue<>();
    private final ConcurrentMap<String, ScheduleHolder> activeHoldersById = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Schedule> archivedSchedulesById = new ConcurrentHashMap<>();
    private final DirectProcessor<LocalSchedulerEvent> eventProcessor = DirectProcessor.create();

    public DefaultLocalScheduler(Duration internalLoopInterval, Scheduler scheduler, Clock clock) {
        this.internalLoopIntervalMs = internalLoopInterval.toMillis();
        this.scheduler = scheduler;
        this.clock = clock;
        this.worker = scheduler.createWorker();
        scheduleNextIteration();
    }

    public void shutdown() {
        worker.dispose();
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
    public ScheduleReference scheduleMono(ScheduleDescriptor scheduleDescriptor, Function<Long, Mono<Void>> actionProducer, Scheduler scheduler) {
        return scheduleInternal(scheduleDescriptor, actionProducer, scheduler, DO_NOTHING);
    }

    @Override
    public ScheduleReference schedule(ScheduleDescriptor scheduleDescriptor, Consumer<Long> action, boolean isolated) {
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

        return scheduleInternal(scheduleDescriptor, tick -> Mono.defer(() -> {
            try {
                action.accept(tick);
                return Mono.empty();
            } catch (Exception e) {
                return Mono.error(e);
            }
        }), actionScheduler, cleanup);
    }

    private ScheduleReference scheduleInternal(ScheduleDescriptor descriptor, Function<Long, Mono<Void>> actionProducer, Scheduler scheduler, Runnable cleanup) {
        String scheduleId = UUID.randomUUID().toString();

        ScheduledActionExecutor executor = new ScheduledActionExecutor(scheduleId, descriptor, actionProducer, scheduler, clock);

        Schedule schedule = Schedule.newBuilder()
                .withId(scheduleId)
                .withDescriptor(descriptor)
                .withCurrentAction(Optional.of(executor.getAction()))
                .withCompletedActions(Collections.emptyList())
                .build();

        ScheduleHolder scheduleHolder = new ScheduleHolder(schedule, actionProducer, scheduler, executor, cleanup);
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
        }
    }

    private class ScheduleHolder {

        private final Function<Long, Mono<Void>> actionProducer;
        private final Scheduler scheduler;
        private final Runnable cleanup;
        private final ScheduleReference reference;

        private volatile Schedule schedule;
        private volatile ScheduledActionExecutor executor;
        private volatile boolean closed;

        private ScheduleHolder(Schedule schedule,
                               Function<Long, Mono<Void>> actionProducer,
                               Scheduler scheduler,
                               ScheduledActionExecutor executor, Runnable cleanup) {
            this.schedule = schedule;
            this.actionProducer = actionProducer;
            this.scheduler = scheduler;
            this.executor = executor;
            this.cleanup = cleanup;
            this.reference = new ScheduleReference() {

                @Override
                public Schedule getSchedule() {
                    return ScheduleHolder.this.schedule;
                }

                @Override
                public boolean isClosed() {
                    return closed;
                }

                @Override
                public void close() {
                    cancel();
                }
            };
        }

        private Schedule getSchedule() {
            return schedule;
        }

        private ScheduleReference getReference() {
            return reference;
        }

        private void cancel() {
            if (closed) {
                return;
            }
            closed = true;

            // TODO Make it reactive
            executor.cancel();

            try {
                cleanup.run();
            } catch (Exception e) {
                logger.warn("Cleanup action failed for schedule: name={}", schedule.getDescriptor().getName(), e);
            } finally {
                activeHoldersById.remove(schedule.getId());
                Schedule updatedSchedule = schedule.toBuilder()
                        .withCurrentAction(Optional.empty())
                        .withCompletedActions(newCompletedActions(executor.getAction()))
                        .build();
                archivedSchedulesById.put(schedule.getId(), updatedSchedule);
                eventProcessor.onNext(new ScheduleRemovedEvent(updatedSchedule));
            }
        }

        private void handleExecution() {
            if (!executor.handleExecution()) {
                return;
            }

            if (SchedulingState.isFinal(executor.getAction().getStatus().getState())) {
                ScheduledAction lastAction = executor.getAction();
                this.executor = new ScheduledActionExecutor(schedule.getId(), schedule.getDescriptor(), actionProducer, scheduler, clock);
                this.schedule = schedule.toBuilder()
                        .withCurrentAction(Optional.of(executor.getAction()))
                        .withCompletedActions(newCompletedActions(lastAction))
                        .build();
            } else {
                this.schedule = schedule.toBuilder().withCurrentAction(Optional.of(executor.getAction())).build();
            }

            eventProcessor.onNext(new ScheduleUpdateEvent(schedule));
        }

        private List<ScheduledAction> newCompletedActions(ScheduledAction action) {
            List<ScheduledAction> result = CollectionsExt.copyAndAdd(schedule.getCompletedActions(), action);
            if (result.size() > SCHEDULE_HISTORY_LIMIT) {
                result = result.subList(result.size() - SCHEDULE_HISTORY_LIMIT, result.size());
            }
            return result;
        }
    }

    public static void main(String[] args) {
        Scheduler scheduler = Schedulers.newSingle("test");
        Mono.<Void>defer(() -> {
            try {
                return Mono.empty();
            } catch (Exception e) {
                return Mono.error(e);
            }
        }).subscribeOn(scheduler)
                .timeout(Duration.ofSeconds(1), scheduler)
                .doOnError(e -> e.printStackTrace())
                .doOnSuccess(value -> System.out.println("Completed"))
                .subscribe();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
