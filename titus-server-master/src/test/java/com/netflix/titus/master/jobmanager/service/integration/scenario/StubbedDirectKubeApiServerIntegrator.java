/*
 * Copyright 2020 Netflix, Inc.
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.titus.api.jobmanager.model.job.ContainerState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.master.kubernetes.client.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class StubbedDirectKubeApiServerIntegrator implements DirectKubeApiServerIntegrator {

    private final ConcurrentMap<String, V1Pod> podHoldersByTaskId = new ConcurrentHashMap<>();

    private volatile boolean enabled = true;

    private volatile RuntimeException nextLaunchError;

    @Override
    public Map<String, V1Pod> getPods() {
        return new HashMap<>(podHoldersByTaskId);
    }

    @Override
    public boolean isReadyForScheduling() {
        return enabled;
    }

    @Override
    public Mono<V1Pod> launchTask(Job job, Task task) {
        return Mono.fromCallable(() -> {
            if (nextLaunchError != null) {
                try {
                    throw nextLaunchError;
                } finally {
                    nextLaunchError = null;
                }
            }
            V1Pod v1Pod = new V1Pod()
                    .metadata(new V1ObjectMeta()
                            .name(task.getId())
                    );
            podHoldersByTaskId.put(task.getId(), v1Pod);
            return v1Pod;
        });
    }

    @Override
    public Mono<Void> terminateTask(Task task) {
        return Mono.fromRunnable(() -> {
            if (podHoldersByTaskId.remove(task.getId()) == null) {
                throw new IllegalArgumentException("Task not found: " + task.getId());
            }
        });
    }

    @Override
    public List<ContainerState> getPodStatus(String taskId) {
        return Collections.emptyList();
    }

    @Override
    public Flux<PodEvent> events() {
        throw new IllegalStateException("not implemented"); // not used
    }

    @Override
    public String resolveReasonCode(Throwable cause) {
        return TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
    }

    public void enableKubeIntegration(boolean enabled) {
        this.enabled = enabled;
    }

    public void failNextTaskLaunch(RuntimeException nextLaunchError) {
        this.nextLaunchError = nextLaunchError;
    }
}