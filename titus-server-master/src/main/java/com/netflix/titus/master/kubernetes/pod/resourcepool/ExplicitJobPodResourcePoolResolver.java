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

package com.netflix.titus.master.kubernetes.pod.resourcepool;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.StringExt;

/**
 * Pick a resource pool directly by setting a job parameter.
 */
public class ExplicitJobPodResourcePoolResolver implements PodResourcePoolResolver {
    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job, Task task) {
        List<String> resourcePoolNames = StringExt.splitByComma(
                job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_PARAMETER_RESOURCE_POOLS)
        );
        if (resourcePoolNames.isEmpty()) {
            return Collections.emptyList();
        }

        String rule = "Job requested placement in resource pools: " + resourcePoolNames;
        return resourcePoolNames.stream().map(name ->
                ResourcePoolAssignment.newBuilder().withResourcePoolName(name).withRule(rule).build()
        ).collect(Collectors.toList());
    }
}
