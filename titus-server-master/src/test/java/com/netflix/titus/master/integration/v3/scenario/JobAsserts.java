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

package com.netflix.titus.master.integration.v3.scenario;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeUtil;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1VolumeMount;

public class JobAsserts {
    public static Predicate<Job> jobInState(JobState expectedState) {
        return job -> job.getStatus().getState() == expectedState;
    }

    public static Predicate<V1Pod> podWithResources(ContainerResources containerResources, int diskMbMin) {
        return pod -> {
            ResourceDimension podResources = EmbeddedKubeUtil.fromPodToResourceDimension(pod);
            if (podResources.getCpu() != containerResources.getCpu()) {
                return false;
            }
            if (podResources.getMemoryMB() != containerResources.getMemoryMB()) {
                return false;
            }
            int diskMB = Math.max(containerResources.getDiskMB(), diskMbMin);
            if (podResources.getDiskMB() != diskMB) {
                return false;
            }
            if (podResources.getNetworkMbs() != containerResources.getNetworkMbps()) {
                return false;
            }
            return true;
        };
    }

    /**
     * FIXME Incomplete checks as some parsing is required.
     */
    public static Predicate<V1Pod> podWithEfsMounts(List<EfsMount> expectedEfsMounts) {
        return pod -> {
            List<V1VolumeMount> efsVolumes = pod.getSpec().getContainers().get(0).getVolumeMounts().stream()
                    .filter(v -> v.getName().startsWith("efs"))
                    .collect(Collectors.toList());
            if (efsVolumes.size() != expectedEfsMounts.size()) {
                return false;
            }
            for (int i = 0; i < efsVolumes.size(); i++) {
                V1VolumeMount efsVolume = efsVolumes.get(i);
                EfsMount expectedEfsMount = expectedEfsMounts.get(i);
                if (!efsVolume.getMountPath().equals(expectedEfsMount.getMountPoint())) {
                    return false;
                }
            }
            return true;
        };
    }
}
