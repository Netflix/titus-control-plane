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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

public class PodResourcePoolResolverChain implements PodResourcePoolResolver {

    private final List<PodResourcePoolResolver> delegates;

    public PodResourcePoolResolverChain(List<PodResourcePoolResolver> delegates) {
        this.delegates = delegates;
    }

    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job, Task task) {
        for (PodResourcePoolResolver delegate : delegates) {
            List<ResourcePoolAssignment> result = delegate.resolve(job, task);
            if (!result.isEmpty()) {
                return result;
            }
        }
        return Collections.emptyList();
    }
}
