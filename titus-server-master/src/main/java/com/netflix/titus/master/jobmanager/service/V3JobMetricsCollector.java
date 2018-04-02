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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateReconcilerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.MetricConstants.METRIC_SCHEDULING_JOB;

class V3JobMetricsCollector {

    private static final Logger logger = LoggerFactory.getLogger(V3JobMetricsCollector.class);

    private final Registry registry;

    private final ConcurrentMap<String, JobMetrics> jobMetricsMap = new ConcurrentHashMap<>();

    V3JobMetricsCollector(Registry registry) {
        this.registry = registry;
    }

    void updateTaskMetrics(Job<?> job, Task task) {
        jobMetricsMap.computeIfAbsent(task.getJobId(), jobId -> new JobMetrics(job)).updateTaskMetrics(task);
    }

    void updateTaskMetrics(JobModelReconcilerEvent.JobModelUpdateReconcilerEvent jobUpdateEvent) {
        if (isTaskReferenceEvent(jobUpdateEvent)) {
            Task task = jobUpdateEvent.getChangedEntityHolder().getEntity();
            updateTaskMetrics(jobUpdateEvent.getJob(), task);
        }
    }

    void removeJob(String jobId) {
        JobMetrics toRemove = jobMetricsMap.remove(jobId);
        if (toRemove != null) {
            toRemove.finish();
        }
    }

    private boolean isTaskReferenceEvent(JobModelReconcilerEvent.JobModelUpdateReconcilerEvent jobUpdateEvent) {
        return jobUpdateEvent.getModelActionHolder().getModel() == ModelActionHolder.Model.Reference
                && jobUpdateEvent.getChangedEntityHolder().getEntity() instanceof Task;
    }

    private class JobMetrics {

        private final Id taskRootId;

        private final ConcurrentMap<String, TaskMetricHolder> taskMetrics = new ConcurrentHashMap<>();
        private final String capacityGroup;

        private JobMetrics(Job<?> job) {
            this.capacityGroup = job.getJobDescriptor().getCapacityGroup();
            this.taskRootId = buildTaskRootId(job.getId(), job.getJobDescriptor().getApplicationName());
        }

        private void updateTaskMetrics(Task task) {
            TaskStatus status = task.getStatus();
            TaskState taskState = status.getState();
            // Do not create counters if task is already terminated
            if (taskState == TaskState.Finished && !taskMetrics.containsKey(task.getId())) {
                return;
            }

            TaskMetricHolder taskMetricH = taskMetrics.computeIfAbsent(task.getId(), myTask -> new TaskMetricHolder(task));
            logger.debug("State transition change for task {}: {}", task.getId(), taskState);
            taskMetricH.transition(TaskStateReport.of(status), status.getReasonCode());
        }

        private void finish() {
            taskMetrics.forEach((key, value) -> value.transition(TaskStateReport.Finished, ""));
            taskMetrics.clear();
        }

        private Id buildTaskRootId(String jobId, String applicationName) {
            Id id = registry.createId(METRIC_SCHEDULING_JOB, "t.application", applicationName);
            id = id.withTag("t.jobId", jobId);
            id = id.withTag("t.engine", "V3");
            return id;
        }

        private Id stateIdOf(Task task) {
            return taskRootId
                    .withTag("t.capacityGroup", capacityGroup)
                    .withTag("t.taskOriginalId", task.getOriginalId())
                    .withTag("t.taskId", task.getId());
        }

        private class TaskMetricHolder {
            private final SpectatorExt.FsmMetrics<TaskStateReport> stateMetrics;

            private TaskMetricHolder(Task task) {
                this.stateMetrics = SpectatorExt.fsmMetrics(stateIdOf(task), TaskStateReport::isTerminalState, TaskStateReport.of(task.getStatus()), registry);
            }

            private void transition(TaskStateReport state, String reason) {
                stateMetrics.transition(state, reason);
            }
        }
    }
}
