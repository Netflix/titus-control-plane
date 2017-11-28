package io.netflix.titus.master.jobmanager.service;

import io.netflix.titus.common.framework.reconciler.ModelActionHolder.Model;
import io.netflix.titus.common.framework.reconciler.ReconciliationFramework;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobAfterChangeReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobBeforeChangeReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobChangeErrorReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateErrorReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobNewModelReconcilerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

/**
 * Log all events in the following format:
 * <br/>
 * 'jobId=..., transactionId=..., status=ok,    type=beforeChange, action=..., trigger=User , target=job , entityId=..., model=         , elapsedMs=..., summary=...'
 * <br/>
 * 'jobId=..., transactionId=..., status=error, type=modelUpdate,  action=..., trigger=Mesos, target=task, entityId=..., model=Reference, elapsedMs=..., summary=...'
 */
class JobTransactionLogger {

    private static final Logger logger = LoggerFactory.getLogger(JobTransactionLogger.class.getSimpleName());

    static Subscription logEvents(ReconciliationFramework<JobChange, JobManagerReconcilerEvent> reconciliationFramework) {
        return reconciliationFramework.events().subscribe(
                event -> logger.info(doFormat(event)),
                e -> logger.error("Event stream terminated with an error", e),
                () -> logger.info("Event stream completed")
        );
    }

    static String doFormat(JobManagerReconcilerEvent event) {
        if (event instanceof JobBeforeChangeReconcilerEvent) {
            return logJobBeforeChangeReconcilerEvent((JobBeforeChangeReconcilerEvent) event);
        }
        if (event instanceof JobAfterChangeReconcilerEvent) {
            return logJobAfterChangeReconcilerEvent((JobAfterChangeReconcilerEvent) event);
        }
        if (event instanceof JobChangeErrorReconcilerEvent) {
            return logJobChangeErrorReconcilerEvent((JobChangeErrorReconcilerEvent) event);
        }
        if (event instanceof JobNewModelReconcilerEvent) {
            return logJobNewModelReconcilerEvent((JobNewModelReconcilerEvent) event);
        }
        if (event instanceof JobModelUpdateReconcilerEvent) {
            return logJobModelUpdateReconcilerEvent((JobModelUpdateReconcilerEvent) event);
        }
        if (event instanceof JobModelUpdateErrorReconcilerEvent) {
            return logJobModelUpdateErrorReconcilerEvent((JobModelUpdateErrorReconcilerEvent) event);
        }
        return "Unknown event type: " + event.getClass();
    }

    private static String logJobBeforeChangeReconcilerEvent(JobBeforeChangeReconcilerEvent event) {
        JobChange jobChange = event.getChangeAction().getChange();
        String jobId = event.getJob().getId();
        String entityId = jobChange.getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "beforeChange",
                event.getChangeAction().getClass().getSimpleName(),
                jobChange.getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                "",
                0,
                jobChange.getSummary()
        );
    }

    private static String logJobAfterChangeReconcilerEvent(JobAfterChangeReconcilerEvent event) {
        JobChange jobChange = event.getChangeAction().getChange();
        String jobId = event.getJob().getId();
        String entityId = jobChange.getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "afterChange",
                event.getChangeAction().getClass().getSimpleName(),
                jobChange.getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                "",
                event.getExecutionTimeMs(),
                jobChange.getSummary()
        );
    }

    private static String logJobChangeErrorReconcilerEvent(JobChangeErrorReconcilerEvent event) {
        JobChange jobChange = event.getChangeAction().getChange();
        String jobId = event.getJob().getId();
        String entityId = jobChange.getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "error",
                "afterChange",
                event.getChangeAction().getClass().getSimpleName(),
                jobChange.getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                "",
                event.getExecutionTimeMs(),
                event.getError().getMessage() + '(' + jobChange.getSummary() + ')'
        );
    }

    private static String logJobNewModelReconcilerEvent(JobNewModelReconcilerEvent event) {
        String jobId = event.getJob().getId();
        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "modelUpdate",
                "initial",
                JobChange.Trigger.API,
                "job",
                jobId,
                Model.Reference.name(),
                0,
                "New job created"
        );
    }

    private static String logJobModelUpdateReconcilerEvent(JobModelUpdateReconcilerEvent event) {
        String jobId = event.getJob().getId();
        String entityId = event.getChangedEntityHolder().getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "modelUpdate",
                event.getModelActionHolder().getAction().getClass().getSimpleName(),
                event.getChangeAction().getChange().getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                event.getModelActionHolder().getModel().name(),
                0,
                "New job created"
        );
    }

    private static String logJobModelUpdateErrorReconcilerEvent(JobModelUpdateErrorReconcilerEvent event) {
        String jobId = event.getJob().getId();
        String entityId = event.getPreviousEntityHolder().getId();

        return doFormat(
                jobId,
                event.getTransactionId(),
                "ok",
                "modelUpdate",
                event.getModelActionHolder().getAction().getClass().getSimpleName(),
                event.getChangeAction().getChange().getTrigger(),
                toTargetName(jobId, entityId),
                entityId,
                event.getModelActionHolder().getModel().name(),
                0,
                "New job created"
        );
    }

    private static String doFormat(String jobId,
                                   long transactionId,
                                   String status,
                                   String type,
                                   String action,
                                   JobChange.Trigger trigger,
                                   String targetName,
                                   String entityId,
                                   String model,
                                   long executionTime,
                                   String summary) {
        return String.format(
                "jobId=%s transactionId=%-8d status=%-5s type=%-13s action=%-20s trigger=%-10s target=%-4s entity=%s model=%-9s %-16s summary=%s",
                jobId,
                transactionId,
                status,
                type,
                action,
                trigger,
                targetName,
                entityId,
                model,
                "elapsed=" + executionTime + "ms",
                summary
        );
    }

    private static String toTargetName(String jobId, String entityId) {
        return jobId.equals(entityId) ? "job" : "task";
    }
}
