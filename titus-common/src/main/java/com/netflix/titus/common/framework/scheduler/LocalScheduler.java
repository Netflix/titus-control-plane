package com.netflix.titus.common.framework.scheduler;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.event.LocalSchedulerEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * Simple scheduler for running tasks periodically within a JVM process.
 *
 * TODO Missing features:
 * - metrics
 * - logging & transaction log
 * - schedule state dump (similar to Fenzo scheduler)
 * - watchdog
 * - REST API
 */
@Experimental(deadline = "12/30/2018")
public interface LocalScheduler {

    /**
     * Returns all schedules that are currently active.
     */
    List<Schedule> getActiveSchedules();

    /**
     * Returns all archived schedules.
     */
    List<Schedule> getArchivedSchedules();

    /**
     * Returns schedule with the given id.
     *
     * @throws LocalSchedulerException if a schedule with the given id does not exist.
     */
    default Schedule getSchedule(String scheduleId) {
        return findSchedule(scheduleId).orElseThrow(() -> LocalSchedulerException.scheduleNotFound(scheduleId));
    }

    /**
     * Returns schedule with the given id if it exists or {@link Optional#empty()}.
     */
    Optional<Schedule> findSchedule(String scheduleId);

    /**
     * Emits event for each schedule state change.
     */
    Flux<LocalSchedulerEvent> events();

    /**
     * Schedule {@link Mono} action.
     */
    ScheduleReference scheduleMono(ScheduleDescriptor scheduleDescriptor,
                                   Function<Long, Mono<Void>> actionProducer,
                                   Scheduler scheduler);

    /**
     * This method enhances the periodic action execution with reactive stream based triggers. For each item
     * emitted by the trigger stream, a new action is scheduled (serialized).
     */
    default <TRIGGER> ScheduleReference scheduleMono(ScheduleDescriptor scheduleDescriptor,
                                                     TRIGGER timeTrigger,
                                                     Function<TRIGGER, Mono<Void>> actionProducer,
                                                     Flux<TRIGGER> trigger,
                                                     Scheduler scheduler) {
        throw new IllegalStateException("not implemented yet");
    }

    /**
     * Schedule an action which is executed synchronously. If the action execution time is long (>1ms), set
     * isolated flag to true. Isolated actions run on their own thread.
     */
    ScheduleReference schedule(ScheduleDescriptor scheduleDescriptor, Consumer<Long> action, boolean isolated);

    /**
     * Cancel a schedule with the given id.
     */
    Mono<Void> cancel(String scheduleId);
}
