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

class ScheduledActionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledActionExecutor.class);

    private final String id;
    private final ScheduleDescriptor descriptor;
    private final Function<Long, Mono<Void>> actionProducer;
    private final Scheduler scheduler;
    private final Clock clock;

    private volatile long index = 0;
    private volatile ScheduledAction action;
    private volatile Retryer retryer;
    private volatile Disposable actionDisposable;
    private volatile Throwable error;

    ScheduledActionExecutor(String id,
                            ScheduleDescriptor descriptor,
                            Function<Long, Mono<Void>> actionProducer,
                            Scheduler scheduler,
                            Clock clock) {
        this.id = id;
        this.descriptor = descriptor;
        this.actionProducer = actionProducer;
        this.scheduler = scheduler;
        this.clock = clock;

        this.action = newScheduledAction();
    }

    public ScheduledAction getAction() {
        return action;
    }

    boolean handleExecution() {
        SchedulingState state = action.getStatus().getState();
        switch (state) {
            case Waiting:
                return handleWaitingState();
            case Running:
                return handleRunningState();
            case Succeeded:
            case Failed:
                Preconditions.checkState(false, "Invocation of the terminated action executor: state={}", state);
        }
        Preconditions.checkState(false, "Unrecognized state: {}", state);
        return false;
    }

    public void cancel() {
        if (actionDisposable != null) {
            actionDisposable.dispose();
        }
    }

    private ScheduledAction newScheduledAction() {
        long now = clock.wallTime();

        long delayMs = retryer == null
                ? descriptor.getInterval().toMillis()
                : retryer.getDelayMs().orElse(descriptor.getInterval().toMillis());

        return ScheduledAction.newBuilder()
                .withId(id)
                .withStatus(SchedulingStatus.newBuilder()
                        .withState(SchedulingState.Waiting)
                        .withExpectedStartTime(now + delayMs)
                        .withTimestamp(now)
                        .build()
                )
                .build();
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

        try {
            this.retryer = descriptor.getRetryerSupplier().get();
            this.error = null;
            this.actionDisposable = actionProducer.apply(index++)
                    .timeout(descriptor.getTimeout())
                    .doOnError(e -> {
                        logger.warn("Action execution error: name={}", descriptor.getName(), e);
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
