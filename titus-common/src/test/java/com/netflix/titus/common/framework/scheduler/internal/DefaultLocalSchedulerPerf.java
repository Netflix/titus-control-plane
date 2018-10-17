package com.netflix.titus.common.framework.scheduler.internal;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleRemovedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleUpdateEvent;
import com.netflix.titus.common.util.time.Clocks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DefaultLocalSchedulerPerf {

    private static final Duration LOOP_INTERVAL = Duration.ofMillis(10);

    private static final int EXECUTIONS = 10;

    private static final long ACTIVE_SCHEDULE_COUNT = 10;

    private final LocalScheduler localScheduler = new DefaultLocalScheduler(LOOP_INTERVAL, Schedulers.parallel(), Clocks.system(), new DefaultRegistry());

    private final Set<ScheduleReference> activeReferences = new HashSet<>();

    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong expectedFailures = new AtomicLong();
    private final AtomicLong removedEvents = new AtomicLong();
    private final AtomicLong tooFastSchedules = new AtomicLong();
    private final AtomicLong delayedSchedules = new AtomicLong();
    private final AtomicLong eventFailures = new AtomicLong();

    private void doRun() throws InterruptedException {
        long lastUpdateTimestamp = 0;
        while (true) {
            if (lastUpdateTimestamp + 1000 < System.currentTimeMillis()) {
                printReport();
                lastUpdateTimestamp = System.currentTimeMillis();
            }

            while (activeReferences.size() < ACTIVE_SCHEDULE_COUNT) {
                activeReferences.add(newMonoAction(false));
                activeReferences.add(newMonoAction(true));
                activeReferences.add(newAction(false));
                activeReferences.add(newAction(true));
            }
            activeReferences.removeIf(ScheduleReference::isClosed);
            Thread.sleep(10);
        }
    }

    private void printReport() {
        System.out.println(String.format("active=%8s, finished=%8s, successes=%8s, expectedFailures=%8s, failureEvents=%8s, removedEvents=%8s, tooFastSchedules=%8s, delayedSchedules=%8s",
                activeReferences.size(), localScheduler.getArchivedSchedules().size(), successes.get(), expectedFailures.get(),
                eventFailures.get(), removedEvents.get(), tooFastSchedules.get(), delayedSchedules.get()
        ));
    }

    private ScheduleReference newMonoAction(boolean failSometimes) {
        int intervalMs = 50;
        ScheduleReference reference = localScheduler.scheduleMono(
                ScheduleDescriptor.newBuilder()
                        .withName("monoAction")
                        .withDescription("Test...")
                        .withInterval(Duration.ofMillis(intervalMs))
                        .withTimeout(Duration.ofSeconds(1))
                        .build(),
                context -> {
                    checkSchedulingLatency(intervalMs, context);

                    if (context.getIteration().getTotal() > EXECUTIONS) {
                        localScheduler.cancel(context.getId()).subscribe();
                        return Mono.empty();
                    }

                    if (failSometimes && context.getIteration().getTotal() % 2 == 0) {
                        expectedFailures.incrementAndGet();
                        return Mono.error(new RuntimeException("Simulated error"));
                    }

                    return Mono.delay(Duration.ofMillis(10)).ignoreElement()
                            .cast(Void.class)
                            .doOnError(Throwable::printStackTrace)
                            .doOnSuccess(next -> successes.incrementAndGet());
                },
                Schedulers.parallel()
        );

        observeEvents(reference);
        return reference;
    }

    private ScheduleReference newAction(boolean failSometimes) {
        int intervalMs = 50;
        ScheduleReference reference = localScheduler.schedule(
                ScheduleDescriptor.newBuilder()
                        .withName("runnableAction")
                        .withDescription("Test...")
                        .withInterval(Duration.ofMillis(intervalMs))
                        .withTimeout(Duration.ofSeconds(1))
                        .build(),
                context -> {
                    checkSchedulingLatency(intervalMs, context);

                    if (context.getIteration().getTotal() > EXECUTIONS) {
                        localScheduler.cancel(context.getId()).subscribe();
                        return;
                    }

                    if (failSometimes && context.getIteration().getTotal() % 2 == 0) {
                        expectedFailures.incrementAndGet();
                        throw new RuntimeException("Simulated error");
                    }

                    try {
                        Thread.sleep(10);
                        successes.incrementAndGet();
                    } catch (InterruptedException ignore) {
                    }
                },
                true
        );

        observeEvents(reference);
        return reference;
    }

    private void checkSchedulingLatency(int intervalMs, ExecutionContext context) {
        if (context.getPreviousAction().isPresent()) {
            ScheduledAction previous = context.getPreviousAction().get();
            long actualDelayMs = System.currentTimeMillis() - previous.getStatus().getTimestamp();
            if (actualDelayMs < intervalMs) {
                tooFastSchedules.incrementAndGet();
            }
            if (actualDelayMs > 2 * intervalMs) {
                delayedSchedules.incrementAndGet();
            }
        }
    }

    private void observeEvents(ScheduleReference reference) {
        localScheduler.events()
                .filter(e -> e.getSchedule().getId().equals(reference.getSchedule().getId()))
                .subscribe(event -> {
                    if (event instanceof ScheduleUpdateEvent) {
                        ScheduleUpdateEvent updateEvent = (ScheduleUpdateEvent) event;
                        ScheduledAction action = updateEvent.getSchedule().getCurrentAction();
                        if (action.getStatus().getState() == SchedulingStatus.SchedulingState.Failed) {
                            eventFailures.incrementAndGet();
                        }
                    } else if (event instanceof ScheduleRemovedEvent) {
                        removedEvents.incrementAndGet();
                    }
                });
    }

    public static void main(String[] args) throws InterruptedException {
        new DefaultLocalSchedulerPerf().doRun();
    }
}
