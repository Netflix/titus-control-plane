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

package com.netflix.titus.master.kubernetes.client;

import java.util.List;
import java.util.Map;

import com.netflix.titus.master.jobmanager.service.ComputeProvider;
import com.netflix.titus.api.jobmanager.model.job.ContainerState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import io.kubernetes.client.openapi.models.V1Pod;
import reactor.core.publisher.Flux;

public interface DirectKubeApiServerIntegrator extends ComputeProvider {

    String COMPONENT = "kubernetesIntegrator";

    Map<String, V1Pod> getPods();

    List<ContainerState> getPodStatus(String taskId);

    Flux<PodEvent> events();
}