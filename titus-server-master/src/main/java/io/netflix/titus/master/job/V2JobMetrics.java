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
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.spectator.SpectatorExt;
import io.netflix.titus.common.util.spectator.SpectatorExt.FsmMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.api.model.v2.V2JobState.toV3TaskState;
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

        TaskMetricHolder taskMetricH = taskMetrics.computeIfAbsent(task, myTask -> new TaskMetricHolder(task));
        logger.debug("State transition change for task {} ({}): {}", task.getWorkerInstanceId(), task, task.getState());

        TaskState v3TaskState = toV3TaskState(task.getState());
        taskMetricH.transition(v3TaskState);

        // Look at all tasks, in case something leaked
        taskMetrics.entrySet().forEach(e -> {
            V2WorkerMetadata t = e.getKey();
            if (V2JobState.isTerminalState(t.getState())) {
                logger.debug("Removing task {} ({}): {}", t.getWorkerInstanceId(), t, t.getState());
                e.getValue().transition(v3TaskState);
                taskMetrics.remove(t);
            }
        });
    }

    public void finish() {
        taskMetrics.entrySet().forEach(e -> {
            V2WorkerMetadata t = e.getKey();
            if (V2JobState.isTerminalState(t.getState())) {
                e.getValue().transition(toV3TaskState(t.getState()));
            } else {
                logger.warn("Job {} finished with task in non-final state {}", t.getJobId(), t.getWorkerInstanceId());
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
        private final FsmMetrics<TaskState> stateMetrics;

        private TaskMetricHolder(V2WorkerMetadata task) {
            this.stateMetrics = SpectatorExt.fsmMetrics(TaskState.setOfAll(), stateIdOf(task), TaskState::isTerminalState, registry);
        }

        private void transition(TaskState state) {
            stateMetrics.transition(state);
        }
    }
}
