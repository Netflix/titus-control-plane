/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.ContainerState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.master.jobmanager.service.ComputeProvider;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import reactor.core.publisher.Mono;

class StubbedComputeProvider implements ComputeProvider {

    private final Map<String, ComputeProviderTask> providerTasks = new HashMap<>();

    private boolean enabled;

    private RuntimeException simulatedError;

    public StubbedComputeProvider() {
        enabled = true;
    }

    @Override
    public Mono<Void> launchTask(Job<?> job, Task task) {
        return Mono.defer(() -> {
            if (simulatedError != null) {
                RuntimeException error = simulatedError;
                simulatedError = null;
                return Mono.error(error);
            }
            providerTasks.put(task.getId(), new ComputeProviderTask(job, task));
            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> terminateTask(Task task) {
        return Mono.fromRunnable(() -> {
            ComputeProviderTask providerTask = providerTasks.get(task.getId());
            if (providerTask != null) {
                providerTask.terminate();
            }
        });
    }

    @Override
    public boolean isReadyForScheduling() {
        return enabled;
    }

    @Override
    public String resolveReasonCode(Throwable error) {
        return TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
    }

    public void enableScheduling(boolean enabled) {
        this.enabled = enabled;
    }

    public void scheduleTask(String taskId) {
        ComputeProviderTask providerTask = mustHaveTask(taskId);
        providerTask.schedule();
    }

    @Override
    public List<ContainerState> getPodStatus(String taskId) {
        return Collections.emptyList();
    }
    public void failNextTaskLaunch(RuntimeException simulatedError) {
        this.simulatedError = simulatedError;
    }

    public boolean hasComputeProviderTask(String taskId) {
        return providerTasks.containsKey(taskId);
    }

    public boolean isTaskFinished(String taskId) {
        ComputeProviderTask providerTask = mustHaveTask(taskId);
        return providerTask.isFinished();
    }

    public TitusExecutorDetails buildExecutorDetails(String taskId) {
        ComputeProviderTask providerTask = mustHaveTask(taskId);
        return providerTask.getExecutorDetails();
    }

    public void finishTask(String taskId) {
        mustHaveTask(taskId);
        providerTasks.remove(taskId);
    }

    public Map<String, String> getScheduledTaskContext(String taskId) {
        ComputeProviderTask providerTask = mustHaveTask(taskId);
        return providerTask.getScheduledTaskContext();
    }

    private ComputeProviderTask mustHaveTask(String taskId) {
        return Preconditions.checkNotNull(providerTasks.get(taskId), "Task not found: %s", taskId);
    }
}
