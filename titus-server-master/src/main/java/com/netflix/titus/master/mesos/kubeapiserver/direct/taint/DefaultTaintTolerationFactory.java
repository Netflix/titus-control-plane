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

package com.netflix.titus.master.mesos.kubeapiserver.direct.taint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import io.kubernetes.client.openapi.models.V1Toleration;

@Singleton
public class DefaultTaintTolerationFactory implements TaintTolerationFactory {

    private final DirectKubeConfiguration configuration;

    @Inject
    public DefaultTaintTolerationFactory(DirectKubeConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<V1Toleration> buildV1Toleration(Job job, Task task) {
        List<V1Toleration> tolerations = new ArrayList<>();

        // Default taints.
        tolerations.add(Tolerations.TOLERATION_VIRTUAL_KUBLET);
        tolerations.add(Tolerations.TOLERATION_KUBE_SCHEDULER);

        resolveAvailabilityZoneToleration(job).ifPresent(tolerations::add);
        resolveGpuInstanceTypeToleration(job).ifPresent(tolerations::add);
        resolveKubeBackendToleration(job).ifPresent(tolerations::add);

        return tolerations;
    }

    private Optional<V1Toleration> resolveAvailabilityZoneToleration(Job job) {
        return KubeUtil.findFarzoneId(configuration, job).map(Tolerations.TOLERATION_FARZONE_FACTORY);
    }

    private Optional<V1Toleration> resolveGpuInstanceTypeToleration(Job job) {
        return job.getJobDescriptor().getContainer().getContainerResources().getGpu() <= 0
                ? Optional.empty()
                : Optional.of(Tolerations.TOLERATION_GPU_INSTANCE);
    }

    private Optional<V1Toleration> resolveKubeBackendToleration(Job job) {
        Map<String, String> constraints = job.getJobDescriptor().getContainer().getHardConstraints();
        String backend = constraints.get(JobConstraints.KUBE_BACKEND);
        if (backend == null) {
            return Optional.empty();
        }
        return Optional.of(new V1Toleration()
                .key(KubeConstants.TAINT_KUBE_BACKEND)
                .operator("Equal")
                .value(backend)
                .effect("NoSchedule")
        );
    }
}
