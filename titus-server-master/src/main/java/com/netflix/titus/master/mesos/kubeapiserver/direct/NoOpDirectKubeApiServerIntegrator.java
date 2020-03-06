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

import java.util.Collections;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import io.kubernetes.client.openapi.models.V1Pod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class NoOpDirectKubeApiServerIntegrator implements DirectKubeApiServerIntegrator {

    @Override
    public Map<String, V1Pod> getPods() {
        return Collections.emptyMap();
    }

    @Override
    public Mono<V1Pod> launchTask(Job job, Task task) {
        return Mono.error(new IllegalStateException("Kube scheduler disabled"));
    }

    @Override
    public Mono<Void> terminateTask(Task task) {
        return Mono.error(new IllegalStateException("Kube scheduler disabled"));
    }

    @Override
    public Flux<PodEvent> events() {
        return Flux.never();
    }
}
