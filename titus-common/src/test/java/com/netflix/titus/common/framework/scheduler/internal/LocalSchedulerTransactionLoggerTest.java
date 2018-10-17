package com.netflix.titus.common.framework.scheduler.internal;

import java.time.Duration;
import java.util.Collections;

import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;
import com.netflix.titus.common.framework.scheduler.model.TransactionId;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleAddedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleRemovedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleUpdateEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sole purpose of this test is visual inspection of the generated log line.
 */
public class LocalSchedulerTransactionLoggerTest {

    private static final Logger logger = LoggerFactory.getLogger(LocalSchedulerTransactionLoggerTest.class);

    private static final Schedule REFERENCE_SCHEDULE = Schedule.newBuilder()
            .withId("reference")
            .withDescriptor(ScheduleDescriptor.newBuilder()
                    .withName("testSchedule")
                    .withDescription("Testing...")
                    .withTimeout(Duration.ofSeconds(1))
                    .withInterval(Duration.ofSeconds(5))
                    .build()
            )
            .withCurrentAction(ScheduledAction.newBuilder()
                    .withId("reference")
                    .withStatus(newSchedulingStatus(SchedulingState.Waiting))
                    .withTransactionId(TransactionId.initial())
                    .build()
            )
            .build();

    @Test
    public void testDoFormatScheduleAdded() {
        verify(LocalSchedulerTransactionLogger.doFormat(new ScheduleAddedEvent(REFERENCE_SCHEDULE)));
    }

    @Test
    public void testDoFormatScheduleRemoved() {
        verify(LocalSchedulerTransactionLogger.doFormat(new ScheduleRemovedEvent(REFERENCE_SCHEDULE)));
    }

    @Test
    public void testDoFormatScheduleRunningUpdate() {
        Schedule schedule = updateStatus(REFERENCE_SCHEDULE, newSchedulingStatus(SchedulingState.Running));
        verify(LocalSchedulerTransactionLogger.doFormat(new ScheduleUpdateEvent(schedule)));
    }

    @Test
    public void testDoFormatScheduleSuccessUpdate() {
        Schedule schedule = updateStatus(REFERENCE_SCHEDULE, newSchedulingStatus(SchedulingState.Succeeded));
        verify(LocalSchedulerTransactionLogger.doFormat(new ScheduleUpdateEvent(schedule)));
    }

    @Test
    public void testDoFormatScheduleFailureUpdate() {
        Schedule schedule = updateStatus(REFERENCE_SCHEDULE, newSchedulingStatus(SchedulingState.Failed));
        verify(LocalSchedulerTransactionLogger.doFormat(new ScheduleUpdateEvent(schedule)));
    }

    @Test
    public void testDoFormatNewAction() {
        Schedule firstCompleted = updateStatus(REFERENCE_SCHEDULE, newSchedulingStatus(SchedulingState.Succeeded));
        Schedule nextAction = firstCompleted.toBuilder()
                .withCurrentAction(ScheduledAction.newBuilder()
                        .withId("reference")
                        .withStatus(newSchedulingStatus(SchedulingState.Waiting))
                        .withTransactionId(TransactionId.initial())
                        .build()
                )
                .withCompletedActions(Collections.singletonList(firstCompleted.getCurrentAction()))
                .build();
        verify(LocalSchedulerTransactionLogger.doFormat(new ScheduleUpdateEvent(nextAction)));
    }

    private void verify(String logEntry) {
        assertThat(logEntry).isNotEmpty();
        logger.info("Transaction log entry: {}", logEntry);
    }

    private static SchedulingStatus newSchedulingStatus(SchedulingState schedulingState) {
        return SchedulingStatus.newBuilder()
                .withState(schedulingState)
                .withTimestamp(System.currentTimeMillis())
                .withExpectedStartTime(schedulingState == SchedulingState.Waiting ? System.currentTimeMillis() + 1_000 : 0)
                .withError(schedulingState == SchedulingState.Failed ? new RuntimeException("Simulated error") : null)
                .build();
    }

    private Schedule updateStatus(Schedule schedule, SchedulingStatus newStatus) {
        return schedule.toBuilder()
                .withCurrentAction(schedule.getCurrentAction().toBuilder()
                        .withStatus(newStatus)
                        .withStatusHistory(Collections.singletonList(schedule.getCurrentAction().getStatus()))
                        .build()
                )
                .build();
    }
}