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

package com.netflix.titus.supplementary.relocation.workflow.step;

import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus.TaskRelocationState;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelocationTransactionLogger {

    private static final Logger logger = LoggerFactory.getLogger("RelocationTransactionLogger");

    private final ReadOnlyJobOperations jobOperations;

    public RelocationTransactionLogger(ReadOnlyJobOperations jobOperations) {
        this.jobOperations = jobOperations;
    }

    void logRelocationReadFromStore(String stepName, TaskRelocationPlan plan) {
        doLog(findJob(plan.getTaskId()), plan.getTaskId(), stepName, "readFromStore", "success", "Relocation plan loaded from store: plan=" + RelocationUtil.doFormat(plan));
    }

    void logRelocationPlanUpdatedInStore(String stepName, TaskRelocationPlan plan) {
        String taskId = plan.getTaskId();
        doLog(findJob(taskId), taskId, stepName, "updateInStore", "success", "Relocation plan added to store: plan=" + RelocationUtil.doFormat(plan));
    }

    void logRelocationPlanUpdateInStoreError(String stepName, TaskRelocationPlan plan, Throwable error) {
        String taskId = plan.getTaskId();
        doLog(findJob(taskId),
                taskId,
                stepName,
                "updateInStore",
                "failure",
                String.format(
                        "Failed to add a relocation plan to store: plan=%s, error=%s",
                        plan,
                        ExceptionExt.toMessageChain(error)
                )
        );
    }

    void logRelocationPlanRemovedFromStore(String stepName, String taskId) {
        doLog(findJob(taskId), taskId, stepName, "removeFromStore", "success", "Obsolete relocation plan removed from store");
    }

    void logRelocationPlanRemoveFromStoreError(String stepName, String taskId, Throwable error) {
        doLog(findJob(taskId),
                taskId,
                stepName,
                "removeFromStore",
                "failure",
                "Failed to remove the obsolete relocation plan from store: error=" + ExceptionExt.toMessageChain(error)
        );
    }

    void logTaskRelocationDeschedulingResult(String stepName, DeschedulingResult deschedulingResult) {
        String taskId = deschedulingResult.getTask().getId();
        doLog(findJob(taskId),
                taskId,
                stepName,
                "descheduling",
                "success",
                "Scheduled for being evicted now from agent: agentId=" + deschedulingResult.getAgentInstance().getId()
        );
    }

    void logTaskRelocationStatus(String stepName, String action, TaskRelocationStatus status) {
        doLog(
                findJob(status.getTaskId()),
                status.getTaskId(),
                stepName,
                action,
                status.getState() == TaskRelocationState.Success ? "success" : "failure",
                String.format(
                        "Details: statusCode=%s, statusMessage=%s, plan=%s",
                        status.getStatusCode(),
                        status.getStatusMessage(),
                        RelocationUtil.doFormat(status.getTaskRelocationPlan())
                )
        );
    }

    void logTaskRelocationStatusStoreFailure(String stepName, TaskRelocationStatus status, Throwable error) {
        doLog(
                findJob(status.getTaskId()),
                status.getTaskId(),
                stepName,
                "storeUpdate",
                "failure",
                String.format(
                        "Details: statusCode=%s, statusMessage=%s, plan=%s, storeError=%s",
                        status.getStatusCode(),
                        status.getStatusMessage(),
                        RelocationUtil.doFormat(status.getTaskRelocationPlan()),
                        ExceptionExt.toMessageChain(error)
                )
        );

    }

    private String findJob(String taskId) {
        return jobOperations.findTaskById(taskId).map(t -> t.getLeft().getId()).orElse("<job_not_found>");
    }

    private static void doLog(String jobId,
                              String taskId,
                              String step,
                              String action,
                              String status,
                              String summary) {
        String message = String.format(
                "jobId=%s taskId=%s step=%-35s action=%-15s status=%-5s summary=%s",
                jobId,
                taskId,
                step,
                action,
                status,
                summary
        );
        logger.info(message);
    }
}
