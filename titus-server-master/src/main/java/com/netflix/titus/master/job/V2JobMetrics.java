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

package com.netflix.titus.master.job;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.master.jobmanager.service.TaskStateReport;
import com.netflix.titus.api.model.v2.JobCompletedReason;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.spectator.SpectatorExt.FsmMetrics;
import com.netflix.titus.master.jobmanager.service.TaskStateReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_UNKNOWN;
import static com.netflix.titus.master.MetricConstants.METRIC_SCHEDULING_JOB;

/**
 * Job related metrics.
 */
public class V2JobMetrics {

    private static final Logger logger = LoggerFactory.getLogger(V2JobMetrics.class);

    private final Id taskRootId;

    private final boolean serviceJob;
    private final Registry registry;

    private final ConcurrentMap<V2WorkerMetadata, TaskMetricHolder> taskMetrics = new ConcurrentHashMap<>();
    private final String capacityGroup;

    public V2JobMetrics(String jobId, boolean serviceJob, String applicationName, String capacityGroup, Registry registry) {
        this.capacityGroup = capacityGroup;
        this.serviceJob = serviceJob;
        this.registry = registry;
        this.taskRootId = buildTaskRootId(jobId, applicationName);
    }

    public void updateTaskMetrics(V2WorkerMetadata task) {
        // Do not create counters if task is already terminated
        if (V2JobState.isTerminalState(task.getState()) && !taskMetrics.containsKey(task)) {
            return;
        }

        String taskId = WorkerNaming.getTaskId(task);
        TaskMetricHolder taskMetricH = taskMetrics.computeIfAbsent(task, myTask -> new TaskMetricHolder(task));

        logger.debug("State transition change for task {}: {}", taskId, task.getState());

        TaskStateReport taskStateReport = toTaskStateReport(task.getState());
        taskMetricH.transition(taskStateReport, toTaskStateReportReason(task.getState(), task.getReason()));

        clearFinishedTasks();
    }

    /**
     * Look at all tasks, in case something leaked.testContainerLifecycleMetrics
     */
    private void clearFinishedTasks() {
        taskMetrics.forEach((nextTask, nextMetrics) -> {
            V2JobState nextTaskState = nextTask.getState();
            if (V2JobState.isTerminalState(nextTaskState)) {
                String nextTaskId = WorkerNaming.getTaskId(nextTask);
                logger.debug("Removing task {}: {}", nextTaskId, nextTaskState);
                nextMetrics.transition(toTaskStateReport(nextTaskState), toTaskStateReportReason(nextTaskState, nextTask.getReason()));
                taskMetrics.remove(nextTask);
            }
        });
    }

    public void finish() {
        taskMetrics.forEach((task, metrics) -> {
            if (V2JobState.isTerminalState(task.getState())) {
                metrics.transition(toTaskStateReport(task.getState()), toTaskStateReportReason(task.getState(), task.getReason()));
            } else {
                logger.warn("Job {} finished with task in non-final state {}", task.getJobId(), task.getWorkerInstanceId());
            }
        });
        taskMetrics.clear();
    }

    private TaskStateReport toTaskStateReport(V2JobState state) {
        switch (state) {
            case Accepted:
                return TaskStateReport.Accepted;
            case Launched:
                return TaskStateReport.Launched;
            case StartInitiated:
                return TaskStateReport.StartInitiated;
            case Started:
                return TaskStateReport.Started;
            case Failed:
                return TaskStateReport.Failed;
            case Completed:
                return TaskStateReport.Finished;
            case Noop:
                break;
        }
        return TaskStateReport.Failed;
    }

    private String toTaskStateReportReason(V2JobState state, JobCompletedReason reason) {
        if (state != V2JobState.Failed || reason == null) {
            return "";
        }
        switch (reason) {
            case Normal:
                return REASON_NORMAL;
            case Error:
                return REASON_FAILED;
            case Lost:
                return REASON_TASK_LOST;
            case TombStone:
            case Killed:
                return REASON_TASK_KILLED;
            case Failed:
                return REASON_FAILED;
            default:
                return REASON_UNKNOWN;
        }
    }

    private Id buildTaskRootId(String jobId, String applicationName) {
        Id id = registry.createId(METRIC_SCHEDULING_JOB, "t.application", applicationName);
        if (serviceJob) {
            id = id.withTag("t.jobId", jobId);
        }
        id = id.withTag("t.engine", "V2");
        return id;
    }

    private Id stateIdOf(V2WorkerMetadata task) {
        return taskRootId
                .withTag("t.taskIndex", Integer.toString(task.getWorkerIndex()))
                .withTag("t.capacityGroup", capacityGroup)
                .withTag("t.taskInstanceId", task.getWorkerInstanceId())
                .withTag("t.taskId", WorkerNaming.getWorkerName(task.getJobId(), task.getWorkerIndex(), task.getWorkerNumber()));
    }

    private class TaskMetricHolder {
        private final FsmMetrics<TaskStateReport> stateMetrics;

        private TaskMetricHolder(V2WorkerMetadata task) {
            this.stateMetrics = SpectatorExt.fsmMetrics(stateIdOf(task), TaskStateReport::isTerminalState, toTaskStateReport(task.getState()), registry);
        }

        private void transition(TaskStateReport state, String reason) {
            stateMetrics.transition(state, reason);
        }
    }
}
