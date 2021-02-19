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
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GpuPodResourcePoolResolver implements PodResourcePoolResolver {

    private static final Logger logger = LoggerFactory.getLogger(GpuPodResourcePoolResolver.class);

    private final KubePodConfiguration configuration;

    private final Lock lock = new ReentrantLock();
    private volatile String lastParsedValue;
    private volatile List<ResourcePoolAssignment> gpuResourcePoolAssignments = Collections.emptyList();

    public GpuPodResourcePoolResolver(KubePodConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job) {
        return job.getJobDescriptor().getContainer().getContainerResources().getGpu() <= 0
                ? Collections.emptyList()
                : getCurrent();
    }

    private List<ResourcePoolAssignment> getCurrent() {
        if (!lock.tryLock()) {
            return gpuResourcePoolAssignments;
        }

        try {
            String currentValue = configuration.getGpuResourcePoolNames();
            if (!Objects.equals(lastParsedValue, currentValue)) {
                this.lastParsedValue = currentValue;
                this.gpuResourcePoolAssignments = StringExt.splitByComma(currentValue).stream()
                        .map(name -> ResourcePoolAssignment.newBuilder()
                                .withResourcePoolName(name)
                                .withRule("GPU resources pool assignment")
                                .build()
                        )
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            logger.error("Cannot parse GPU resource pool", e);
        } finally {
            lock.unlock();
        }
        return gpuResourcePoolAssignments;
    }
}
