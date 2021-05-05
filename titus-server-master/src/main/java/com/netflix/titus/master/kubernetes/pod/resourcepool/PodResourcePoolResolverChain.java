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

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;

public class PodResourcePoolResolverChain implements PodResourcePoolResolver {

    private static final String POD_RESOLVER_METRIC_NAME = "titus.pod.resourcepool.resolver";

    private static final String RESOLVER_NAME = "resolver";

    private static final String RESOURCE_POOL_ASSIGNMENT = "resourcepool";

    private final List<PodResourcePoolResolver> delegates;

    private final Registry registry;

    private final Id resolverId;

    public PodResourcePoolResolverChain(List<PodResourcePoolResolver> delegates, TitusRuntime titusRuntime) {
        this.delegates = delegates;
        this.registry = titusRuntime.getRegistry();
        this.resolverId = this.registry.createId(POD_RESOLVER_METRIC_NAME);
    }

    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job, Task task) {
        for (PodResourcePoolResolver delegate : delegates) {
            List<ResourcePoolAssignment> result = delegate.resolve(job, task);
            if (!result.isEmpty()) {
                this.registry
                        .counter(this.resolverId
                                .withTag(RESOLVER_NAME, delegate.getClass().getSimpleName())
                                .withTag(RESOURCE_POOL_ASSIGNMENT, result.stream().map(ResourcePoolAssignment::getResourcePoolName).collect(Collectors.joining(",")))
                        ).increment();
                return result;
            }
        }
        return Collections.emptyList();
    }
}
