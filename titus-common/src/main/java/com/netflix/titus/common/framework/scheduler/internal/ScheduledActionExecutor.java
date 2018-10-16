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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.LocalSchedulerException;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * {@link ScheduledActionExecutor} handles lifecycle of a single action, which transitions trough
 * waiting -> running -> cancelling (optionally) -> succeeded | failed states.
 */
class ScheduledActionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledActionExecutor.class);

    private static final int SCHEDULE_HISTORY_LIMIT = 20;

    private final ScheduleDescriptor descriptor;
    private final Function<ExecutionContext, Mono<Void>> actionProducer;
    private final Scheduler scheduler;
    private final Clock clock;

    private volatile Schedule schedule;
    private final ScheduleMetrics scheduleMetrics;
    private volatile long index;
    private volatile ScheduledAction action;
    private volatile Retryer retryer;
    private volatile Disposable actionDisposable;
    private volatile Throwable error;

    ScheduledActionExecutor(Schedule schedule,
                            ScheduleMetrics scheduleMetrics,
                            long index,
                            Function<ExecutionContext, Mono<Void>> actionProducer,
                            Scheduler scheduler,
                            Clock clock) {
        this.schedule = schedule;
        this.descriptor = schedule.getDescriptor();
        this.scheduleMetrics = scheduleMetrics;
        this.index = index;
        this.actionProducer = actionProducer;
        this.scheduler = scheduler;
        this.clock = clock;

        this.action = schedule.getCurrentAction();

        scheduleMetrics.onNewScheduledActionExecutor(this.getSchedule());
    }

    Schedule getSchedule() {
        return schedule;
    }

    ScheduledAction getAction() {
        return action;
    }

    long getIndex() {
        return index;
    }

    /**
     * Invocations serialized with cancel.
     */
    boolean handleExecution() {
        SchedulingState state = action.getStatus().getState();
        switch (state) {
            case Waiting:
                boolean changed = handleWaitingState();
                if (changed) {
                    scheduleMetrics.onSchedulingStateUpdate(this.getSchedule());
                }
                return changed;
            case Running:
                boolean runningChanged = handleRunningState();
                if (runningChanged) {
                    scheduleMetrics.onSchedulingStateUpdate(this.getSchedule());
                }
                return runningChanged;
            case Cancelling:
                boolean cancellingChanged = handleCancellingState();
                if (cancellingChanged) {
                    scheduleMetrics.onScheduleRemoved(this.getSchedule());
                }
                return cancellingChanged;
            case Succeeded:
            case Failed:
                Preconditions.checkState(false, "Invocation of the terminated action executor: state={}", state);
        }
        Preconditions.checkState(false, "Unrecognized state: {}", state);
        return false;
    }

    ScheduledActionExecutor nextScheduledActionExecutor() {
        Schedule newSchedule = schedule.toBuilder()
                .withCurrentAction(newScheduledAction())
                .withCompletedActions(appendToCompletedActions())
                .build();

        return new ScheduledActionExecutor(
                newSchedule,
                scheduleMetrics,
                index,
                actionProducer,
                scheduler,
                clock
        );
    }

    /**
     * Invocations serialized with handleExecution.
     */
    boolean cancel() {
        if (action.getStatus().getState().isFinal()) {
            return false;
        }

        this.action = changeState(builder -> builder.withState(SchedulingState.Cancelling));
        this.schedule = schedule.toBuilder().withCurrentAction(action).build();

        if (actionDisposable != null) {
            actionDisposable.dispose();
        }

        return true;
    }

    private ScheduledAction newScheduledAction() {
        long now = clock.wallTime();

        long delayMs = retryer == null
                ? descriptor.getInterval().toMillis()
                : retryer.getDelayMs().orElse(descriptor.getInterval().toMillis());

        return ScheduledAction.newBuilder()
                .withId(schedule.getId())
                .withStatus(SchedulingStatus.newBuilder()
                        .withState(SchedulingState.Waiting)
                        .withExpectedStartTime(now + delayMs)
                        .withTimestamp(now)
                        .build()
                )
                .build();
    }

    private List<ScheduledAction> appendToCompletedActions() {
        List<ScheduledAction> completedActions = CollectionsExt.copyAndAdd(schedule.getCompletedActions(), action);
        if (completedActions.size() > SCHEDULE_HISTORY_LIMIT) {
            completedActions = completedActions.subList(completedActions.size() - SCHEDULE_HISTORY_LIMIT, completedActions.size());
        }
        return completedActions;
    }

    private boolean handleWaitingState() {
        long now = clock.wallTime();
        if (action.getStatus().getExpectedStartTime() > now) {
            return false;
        }
        SchedulingStatus oldStatus = action.getStatus();
        this.action = action.toBuilder()
                .withStatus(SchedulingStatus.newBuilder()
                        .withState(SchedulingState.Running)
                        .withTimestamp(now)
                        .build()
                )
                .withStatusHistory(CollectionsExt.copyAndAdd(action.getStatusHistory(), oldStatus))
                .build();
        this.schedule = schedule.toBuilder().withCurrentAction(action).build();

        try {
            this.retryer = descriptor.getRetryerSupplier().get();
            this.error = null;
            this.actionDisposable = actionProducer
                    .apply(
                            ExecutionContext.newBuilder()
                                    .withId(schedule.getId())
                                    .withCycle(index++)
                                    .build()
                    )
                    .timeout(descriptor.getTimeout())
                    .doOnError(e -> {
                        if (logger.isDebugEnabled()) {
                            logger.warn("Action execution error: name={}", descriptor.getName(), e);
                        }
                        ExceptionExt.silent(() -> descriptor.getOnErrorHandler().accept(action, e));
                        this.error = e;
                    })
                    .doOnSuccess(nothing -> ExceptionExt.silent(() -> descriptor.getOnSuccessHandler().accept(action)))
                    .subscribeOn(scheduler)
                    .subscribe();
        } catch (Exception e) {
            logger.warn("Could not produce action: name={}", descriptor.getName(), e);
            this.action = newFailedStatus();
        }

        return true;
    }

    private boolean handleRunningState() {
        if (!actionDisposable.isDisposed()) {
            return false;
        }

        this.action = error == null
                ? changeState(builder -> builder.withState(SchedulingState.Succeeded))
                : newFailedStatus();
        this.schedule = schedule.toBuilder().withCurrentAction(action).build();

        return true;
    }

    private boolean handleCancellingState() {
        if (actionDisposable != null && !actionDisposable.isDisposed()) {
            return false;
        }

        this.error = LocalSchedulerException.cancelled();
        this.action = newFailedStatus();
        this.schedule = schedule.toBuilder().withCurrentAction(action).build();

        return true;
    }

    private ScheduledAction changeState(Consumer<SchedulingStatus.Builder> updater) {
        SchedulingStatus.Builder statusBuilder = SchedulingStatus.newBuilder()
                .withTimestamp(clock.wallTime());
        updater.accept(statusBuilder);

        SchedulingStatus oldStatus = action.getStatus();
        List<SchedulingStatus> statusHistory = CollectionsExt.copyAndAdd(action.getStatusHistory(), oldStatus);
        return action.toBuilder()
                .withStatus(statusBuilder.build())
                .withStatusHistory(statusHistory)
                .build();
    }

    private ScheduledAction newFailedStatus() {
        return changeState(builder -> builder.withState(SchedulingState.Failed).withError(error));
    }
}
