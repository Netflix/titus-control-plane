package com.netflix.titus.common.framework.scheduler.internal;

import java.time.Duration;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;
import com.netflix.titus.common.framework.scheduler.model.event.LocalSchedulerEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleAddedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleRemovedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleUpdateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import static com.netflix.titus.common.util.CollectionsExt.last;

class LocalSchedulerTransactionLogger {

    private static final Logger logger = LoggerFactory.getLogger("LocalSchedulerTransactionLogger");

    private static final Duration RETRY_DELAY = Duration.ofSeconds(1);

    static Disposable logEvents(LocalScheduler localScheduler) {
        return localScheduler.events()
                .retryWhen(error -> Flux.just(1).delayElements(RETRY_DELAY))
                .subscribe(
                        event -> logger.info(doFormat(event)),
                        e -> logger.error("Event stream terminated with an error", e),
                        () -> logger.info("Event stream completed")
                );
    }

    @VisibleForTesting
    static String doFormat(LocalSchedulerEvent event) {
        if (event instanceof ScheduleAddedEvent) {
            return logScheduleAddedEvent((ScheduleAddedEvent) event);
        } else if (event instanceof ScheduleRemovedEvent) {
            return logScheduleRemovedEvent((ScheduleRemovedEvent) event);
        }
        if (event instanceof ScheduleUpdateEvent) {
            return logScheduleUpdateEvent((ScheduleUpdateEvent) event);
        }
        return "Unknown event type: " + event.getClass();
    }

    private static String logScheduleAddedEvent(ScheduleAddedEvent event) {
        return doFormat(event.getSchedule(), "ScheduleAdded", "New schedule added");
    }

    private static String logScheduleRemovedEvent(ScheduleRemovedEvent event) {
        return doFormat(event.getSchedule(), "ScheduleRemoved", "Schedule removed");
    }

    private static String logScheduleUpdateEvent(ScheduleUpdateEvent event) {
        SchedulingStatus status = event.getSchedule().getCurrentAction().getStatus();
        SchedulingState schedulingState = status.getState();

        String summary;
        switch (schedulingState) {
            case Waiting:
                summary = "Waiting...";
                break;
            case Running:
                summary = "Running...";
                break;
            case Cancelling:
                summary = "Schedule cancelled by a user";
                break;
            case Succeeded:
                summary = "Scheduled action completed";
                break;
            case Failed:
                summary = "Scheduled action failed: error="
                        + status.getError().map(Throwable::getMessage).orElse("<error_not_available>");
                break;
            default:
                summary = "Unknown scheduling state: schedulingState=" + schedulingState;
        }
        return doFormat(event.getSchedule(), "ExecutionUpdate", summary);
    }

    private static String doFormat(Schedule schedule,
                                   String eventKind,
                                   String summary) {
        ScheduledAction action = schedule.getCurrentAction();
        return String.format(
                "name=%-20s eventKind=%-15s iteration=%-6s state=%-10s %-15s summary=%s",
                schedule.getDescriptor().getName(),
                eventKind,
                action.getIteration().getId() + "." + action.getIteration().getAttempt(),
                action.getStatus().getState(),
                "elapsed=" + toElapsedMs(schedule) + "ms",
                summary
        );
    }

    private static long toElapsedMs(Schedule schedule) {
        SchedulingStatus currentStatus = schedule.getCurrentAction().getStatus();

        if (currentStatus.getState() != SchedulingState.Waiting) {
            List<SchedulingStatus> statusHistory = schedule.getCurrentAction().getStatusHistory();
            return currentStatus.getTimestamp() - last(statusHistory).getTimestamp();
        }

        // Report time between the new action, and the previous one
        if (schedule.getCompletedActions().isEmpty()) {
            return 0;
        }

        ScheduledAction previousAction = last(schedule.getCompletedActions());
        return currentStatus.getTimestamp() - previousAction.getStatus().getTimestamp();
    }
}
