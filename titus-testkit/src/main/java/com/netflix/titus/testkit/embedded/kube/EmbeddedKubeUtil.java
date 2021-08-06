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

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.testkit.model.PrimitiveValueGenerators;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Pod;

import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_CPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_EPHERMERAL_STORAGE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_GPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_MEMORY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_NETWORK;

public class EmbeddedKubeUtil {

    private static final int ONE_MB = 1024 * 1024;
    private static final int ONE_MBPS = 1_000_000;

    private static final MutableDataGenerator<String> IP_ADDRESS_GENERATOR = new MutableDataGenerator<>(PrimitiveValueGenerators.ipv4CIDRs("10.0.0.0/24"));

    public synchronized static String nextIpAddress() {
        return IP_ADDRESS_GENERATOR.getValue();
    }

    public static ResourceDimension fromPodToResourceDimension(V1Pod pod) {
        Map<String, Quantity> resources = pod.getSpec().getContainers().get(0).getResources().getRequests();
        return ResourceDimension.newBuilder()
                .withCpus(resources.get(RESOURCE_CPU).getNumber().doubleValue())
                .withGpu(resources.get(RESOURCE_GPU).getNumber().longValue())
                .withMemoryMB(resources.get(RESOURCE_MEMORY).getNumber().longValue() / ONE_MB)
                .withDiskMB(resources.get(RESOURCE_EPHERMERAL_STORAGE).getNumber().longValue() / ONE_MB)
                .withNetworkMbs(resources.get(RESOURCE_NETWORK).getNumber().longValue() / ONE_MBPS)
                .build();
    }

    public static Map<String, Quantity> fromResourceDimensionsToKubeQuantityMap(ResourceDimension resources) {
        Map<String, Quantity> quantityMap = new HashMap<>();

        quantityMap.put(RESOURCE_CPU, new Quantity(String.valueOf(resources.getCpu())));
        quantityMap.put(RESOURCE_GPU, new Quantity(String.valueOf(resources.getGpu())));

        Quantity memory = new Quantity(resources.getMemoryMB() + "Mi");
        Quantity disk = new Quantity(resources.getDiskMB() + "Mi");
        Quantity network = new Quantity(resources.getNetworkMbs() + "M");

        quantityMap.put(RESOURCE_MEMORY, memory);
        quantityMap.put(RESOURCE_EPHERMERAL_STORAGE, disk);
        quantityMap.put(RESOURCE_NETWORK, network);

        return quantityMap;
    }

    public static TaskState getPodState(V1Pod pod) {
        if (pod.getSpec().getNodeName() == null) {
            return TaskState.Accepted;
        }
        if ("SCHEDULED".equals(pod.getStatus().getReason())) {
            return TaskState.Launched;
        }
        if ("TASK_STARTING".equals(pod.getStatus().getReason())) {
            return TaskState.StartInitiated;
        }
        if ("TASK_RUNNING".equals(pod.getStatus().getReason())) {
            return TaskState.Started;
        }
        if (pod.getMetadata().getDeletionTimestamp() != null) {
            return TaskState.KillInitiated;
        }
        return TaskState.Finished;
    }

    public static <T> T copy(T value) {
        Gson gson = new Gson();
        return (T) gson.fromJson(gson.toJson(value), value.getClass());
    }
}
