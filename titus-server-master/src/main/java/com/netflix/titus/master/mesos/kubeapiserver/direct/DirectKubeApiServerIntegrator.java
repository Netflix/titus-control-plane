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

package com.netflix.titus.master.mesos.kubeapiserver.direct;

import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import io.kubernetes.client.openapi.models.V1Pod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface DirectKubeApiServerIntegrator {

    String COMPONENT = "kubernetesIntegrator";

    Map<String, V1Pod> getPods();

    Mono<V1Pod> launchTask(Job job, Task task);

    Mono<Void> terminateTask(Task task);

    Flux<PodEvent> events();

    /**
     * Given a KubeAPI pod create error, resolve it to a reason code defined in {@link com.netflix.titus.api.jobmanager.model.job.TaskStatus}.
     */
    String resolveReasonCode(Throwable cause);

    /**
     * Returns true, if the Kubernetes integration subsystem is ready for scheduling.
     */
    default boolean isReadyForScheduling() {
        return true;
    }
}