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

package com.netflix.titus.testkit.embedded.kube;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;

import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_CPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_EPHERMERAL_STORAGE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_GPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_MEMORY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_NETWORK;

class NodeAndPodCatalog {

    static V1Pod newPod() {
        ContainerResources containerResources = ContainerResources.newBuilder()
                .withCpu(4)
                .withMemoryMB(8192)
                .withDiskMB(10000)
                .withNetworkMbps(256)
                .build();
        return new V1Pod()
                .metadata(new V1ObjectMeta()
                        .name(UUID.randomUUID().toString())
                )
                .spec(new V1PodSpec()
                        .containers(Collections.singletonList(new V1Container()
                                .name("container1")
                                .resources(buildV1ResourceRequirements(containerResources))
                        ))
                );
    }

    static V1ResourceRequirements buildV1ResourceRequirements(ContainerResources containerResources) {
        Map<String, Quantity> requests = new HashMap<>();
        Map<String, Quantity> limits = new HashMap<>();

        requests.put(RESOURCE_CPU, new Quantity(String.valueOf(containerResources.getCpu())));
        limits.put(RESOURCE_CPU, new Quantity(String.valueOf(containerResources.getCpu())));

        requests.put(RESOURCE_GPU, new Quantity(String.valueOf(containerResources.getGpu())));
        limits.put(RESOURCE_GPU, new Quantity(String.valueOf(containerResources.getGpu())));

        Quantity memory = new Quantity(containerResources.getMemoryMB() + "Mi");
        Quantity disk = new Quantity(containerResources.getDiskMB() + "Mi");
        Quantity network = new Quantity(containerResources.getNetworkMbps() + "M");

        requests.put(RESOURCE_MEMORY, memory);
        limits.put(RESOURCE_MEMORY, memory);

        requests.put(RESOURCE_EPHERMERAL_STORAGE, disk);
        limits.put(RESOURCE_EPHERMERAL_STORAGE, disk);

        requests.put(RESOURCE_NETWORK, network);
        limits.put(RESOURCE_NETWORK, network);

        return new V1ResourceRequirements().requests(requests).limits(limits);
    }
}
