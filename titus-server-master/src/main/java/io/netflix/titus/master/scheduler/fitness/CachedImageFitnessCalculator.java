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

package io.netflix.titus.master.scheduler.fitness;

import java.util.Optional;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.scheduler.ScheduledRequest;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheImage;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;

import static io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheFunctions.createImage;
import static io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheFunctions.getImage;

/**
 * A fitness calculator that will prefer placing tasks on agents that have previous launched the same image so that the
 * image is already cached.
 */
public class CachedImageFitnessCalculator implements VMTaskFitnessCalculator {

    private static final double IMAGE_NOT_CACHED_SCORE = 0.01;
    private static final double IMAGE_CACHED_SCORE = 1.0;
    private final AgentResourceCache agentResourceCache;

    public CachedImageFitnessCalculator(AgentResourceCache agentResourceCache) {
        this.agentResourceCache = agentResourceCache;
    }

    @Override
    public String getName() {
        return "Image Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Optional<AgentResourceCacheInstance> instanceOpt = agentResourceCache.getActive(targetVM.getHostname());
        if (instanceOpt.isPresent()) {
            AgentResourceCacheInstance instance = instanceOpt.get();
            AgentResourceCacheImage image = getImage(taskRequest);
            if (instance.getImages().contains(image)) {
                return IMAGE_CACHED_SCORE;
            }
        }
        return IMAGE_NOT_CACHED_SCORE;
    }
}
