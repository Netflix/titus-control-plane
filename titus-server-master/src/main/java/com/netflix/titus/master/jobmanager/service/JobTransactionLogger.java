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

package com.netflix.titus.master.jobmanager.service;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder.Model;
import com.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Log all events in the following format:
 * <br/>
 * 'jobId=..., transactionId=..., status=ok,    type=beforeChange,           action=..., trigger=User , target=job , entityId=..., waited=..., elapsed=..., summary=...'
 * <br/>
 * 'jobId=..., transactionId=..., status=error, type=modelUpdate/reference,  action=..., trigger=Mesos, target=task, entityId=..., waited=..., elapsed=..., summary=...'
 */
class JobTransactionLogger {

    private static final Logger logger = LoggerFactory.getLogger("JobTransactionLogger");

    private static final long BUFFER_SIZE = 5000;

    static Subscription logEvents(ReconciliationFramework<JobManagerReconcilerEvent> reconciliationFramework) {
        return eventStreamWithBackpressure(reconciliationFramework)
                .observeOn(Schedulers.io())
                .retryWhen(errors -> errors.flatMap(
                        e -> {
                            logger.warn("Transactions may be missing in the log. The event stream has terminated with an error and must be re-subscribed: {}", ExceptionExt.toMessage(e));
                            return eventStreamWithBackpressure(reconciliationFramework);
                        }))
                .subscribe(
                        event -> logger.info(doFormat(event)),
                        e -> logger.error("Event stream terminated with an error", e),
                        () -> logger.info("Event stream completed")
                );
    }

    static String doFormat(JobManagerReconcilerEvent event) {
        if (event instanceof JobChangeReconcilerEvent.JobBeforeChangeReconcilerEvent) {
            return logJobBeforeChangeReconcilerEvent((JobChangeReconcilerEvent.JobBeforeChangeReconcilerEvent) event);
        }
        if (event instanceof JobChangeReconcilerEvent.JobAfterChangeReconcilerEvent) {
            return logJobAfterChangeReconcilerEvent((JobChangeReconcilerEvent.JobAfterChangeReconcilerEvent) event);
        }
        if (event instanceof JobChangeReconcilerEvent.JobChangeErrorReconcilerEvent) {
            return logJobChangeErrorReconcilerEvent((JobChangeReconcilerEvent.JobChangeErrorReconcilerEvent) event);
        }
        if (event instanceof JobModelReconcilerEvent.JobNewModelReconcilerEvent) {
            return logJobNewModelReconcilerEvent((JobModelReconcilerEvent.JobNewModelReconcilerEvent) event);
        }
        if (event instanceof JobModelReconcilerEvent.JobModelUpdateReconcilerEvent) {
            return logJobModelUpdateReconcilerEvent((JobModelReconcilerEvent.JobModelUpdateReconcilerEvent) event);
        }
        if (event instanceof JobModelReconcilerEvent.JobModelUpdateErrorReconcilerEvent) {
            return logJobModelUpdateErrorReconcilerEvent((JobModelReconcilerEvent.JobModelUpdateErrorReconcilerEvent) event);
        }
        return "Unknown event type: " + event.getClass();
    }

    private static Observable<JobManagerReconcilerEvent> eventStreamWithBackpressure(ReconciliationFramework<JobManagerReconcilerEvent> reconciliationFramework) {
        return ObservableExt.onBackpressureDropAndNotify(
                reconciliationFramework.events(),
                BUFFER_SIZE,
                droppedCount -> logger.warn("Dropping events due to buffer overflow in job transaction log: droppedCount={}", droppedCount),
                1, TimeUnit.SECONDS
        );
    }

    private static String logJobBeforeChangeReconcilerEvent(JobChangeReconcilerEvent.JobBeforeChangeReconcilerEvent event) {
        TitusChangeAction changeAction = event.getChangeAction();
        String jobId = event.getJob().getId();
        String entityId = changeAction.getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "beforeChange",
                event.getChangeAction().getName(),
                changeAction.getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                0,
                0,
                changeAction.getSummary(),
                event.getCallMetadata().getCallerId(),
                event.getCallMetadata().getCallReason()
        );
    }

    private static String logJobAfterChangeReconcilerEvent(JobChangeReconcilerEvent.JobAfterChangeReconcilerEvent event) {
        TitusChangeAction changeAction = event.getChangeAction();
        String jobId = event.getJob().getId();
        String entityId = changeAction.getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "afterChange",
                event.getChangeAction().getName(),
                changeAction.getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                event.getWaitTimeMs(),
                event.getExecutionTimeMs(),
                changeAction.getSummary(),
                event.getCallMetadata().getCallerId(),
                event.getCallMetadata().getCallReason()
        );
    }

    private static String logJobChangeErrorReconcilerEvent(JobChangeReconcilerEvent.JobChangeErrorReconcilerEvent event) {
        TitusChangeAction changeAction = event.getChangeAction();
        String jobId = event.getJob().getId();
        String entityId = changeAction.getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "error",
                "afterChange",
                event.getChangeAction().getName(),
                changeAction.getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                event.getWaitTimeMs(),
                event.getExecutionTimeMs(),
                event.getError().getMessage() + '(' + changeAction.getSummary() + ')',
                event.getCallMetadata().getCallerId(),
                event.getCallMetadata().getCallReason()
        );
    }

    private static String logJobNewModelReconcilerEvent(JobModelReconcilerEvent.JobNewModelReconcilerEvent event) {
        String jobId = event.getJob().getId();
        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "modelUpdate/" + Model.Reference.name(),
                "initial",
                V3JobOperations.Trigger.API,
                "job",
                jobId,
                0,
                0,
                "New job created",
                event.getCallMetadata().getCallerId(),
                event.getCallMetadata().getCallReason()
        );
    }

    private static String logJobModelUpdateReconcilerEvent(JobModelReconcilerEvent.JobModelUpdateReconcilerEvent event) {
        String jobId = event.getJob().getId();
        String entityId = event.getChangedEntityHolder().getId();

        ModelActionHolder actionHolder = event.getModelActionHolder();
        TitusModelAction action = (TitusModelAction) actionHolder.getAction();
        String summary = event.getChangedEntityHolder().getEntity() instanceof Task
                ? action.getSummary() + "; " + taskChangeSummary(event)
                : action.getSummary();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "modelUpdate/" + actionHolder.getModel().name(),
                ((TitusModelAction) actionHolder.getAction()).getName(),
                event.getChangeAction().getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                0,
                0,
                summary,
                event.getCallMetadata().getCallerId(),
                event.getCallMetadata().getCallReason()
        );
    }

    private static String taskChangeSummary(JobModelReconcilerEvent.JobModelUpdateReconcilerEvent event) {
        Task currentTask = event.getChangedEntityHolder().getEntity();
        return String.format("Task{state=%s}", currentTask.getStatus().getState());
    }

    private static String logJobModelUpdateErrorReconcilerEvent(JobModelReconcilerEvent.JobModelUpdateErrorReconcilerEvent event) {
        String jobId = event.getJob().getId();
        String entityId = event.getPreviousEntityHolder().getId();

        ModelActionHolder actionHolder = event.getModelActionHolder();
        TitusModelAction action = (TitusModelAction) actionHolder.getAction();
        return doFormat(
                jobId,
                event.getTransactionId(),
                "error",
                "modelUpdate/" + event.getModelActionHolder().getModel().name(),
                ((TitusModelAction) actionHolder.getAction()).getName(),
                event.getChangeAction().getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                0,
                0,
                action.getSummary(),
                event.getCallMetadata().getCallerId(),
                event.getCallMetadata().getCallReason()
        );
    }

    private static String doFormat(String jobId,
                                   String transactionId,
                                   String status,
                                   String type,
                                   String action,
                                   V3JobOperations.Trigger trigger,
                                   String targetName,
                                   String entityId,
                                   long waitTimeMs,
                                   long executionTime,
                                   String summary,
                                   String callerID,
                                   String callReason) {
        return String.format(
                "jobId=%s entity=%s transactionId=%-5s target=%-4s status=%-5s type=%-22s action=%-45s trigger=%-10s %-16s %-15s callerId=%-15s callReason=%-15s summary=%s",
                jobId,
                entityId,
                transactionId,
                targetName,
                status,
                type,
                action,
                trigger,
                "caller id=" + callerID,
                "call reason=" + callReason,
                "waited=" + waitTimeMs + "ms",
                "elapsed=" + executionTime + "ms",
                summary
                );
    }

    private static String toTargetName(String jobId, String entityId) {
        return jobId.equals(entityId) ? "job" : "task";
    }
}
