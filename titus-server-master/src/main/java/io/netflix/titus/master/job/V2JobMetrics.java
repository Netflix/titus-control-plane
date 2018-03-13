/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.job;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.spectator.SpectatorExt;
import io.netflix.titus.common.util.spectator.SpectatorExt.FsmMetrics;
import io.netflix.titus.master.jobmanager.service.TaskStateReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.master.MetricConstants.METRIC_SCHEDULING_JOB;

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
        taskMetricH.transition(taskStateReport);

        clearFinishedTasks();
    }

    /**
     * Look at all tasks, in case something leaked.testContainerLifecycleMetrics
     */
    private void clearFinishedTasks() {
        taskMetrics.forEach((nextTask, nextMetrics) -> {
            if (V2JobState.isTerminalState(nextTask.getState())) {
                String nextTaskId = WorkerNaming.getTaskId(nextTask);
                logger.debug("Removing task {}: {}", nextTaskId, nextTask.getState());
                nextMetrics.transition(toTaskStateReport(nextTask.getState()));
                taskMetrics.remove(nextTask);
            }
        });
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

    public void finish() {
        taskMetrics.forEach((task, metrics) -> {
            if (V2JobState.isTerminalState(task.getState())) {
                metrics.transition(toTaskStateReport(task.getState()));
            } else {
                logger.warn("Job {} finished with task in non-final state {}", task.getJobId(), task.getWorkerInstanceId());
            }
        });
        taskMetrics.clear();
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
        Id id = taskRootId
                .withTag("t.taskIndex", Integer.toString(task.getWorkerIndex()))
                .withTag("t.capacityGroup", capacityGroup);
        if (serviceJob) {
            id = id.withTag("t.taskInstanceId", task.getWorkerInstanceId());
            id = id.withTag("t.taskId", WorkerNaming.getWorkerName(task.getJobId(), task.getWorkerIndex(), task.getWorkerNumber()));
        }
        return id;
    }

    private class TaskMetricHolder {
        private final FsmMetrics<TaskStateReport> stateMetrics;

        private TaskMetricHolder(V2WorkerMetadata task) {
            this.stateMetrics = SpectatorExt.fsmMetrics(TaskStateReport.setOfAll(), stateIdOf(task), TaskStateReport::isTerminalState, registry);
        }

        private void transition(TaskStateReport state) {
            stateMetrics.transition(state);
        }
    }
}
