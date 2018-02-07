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
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheNetworkInterface;

import static io.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.*;

/**
 * A fitness calculator that will prefer placing tasks on agents that have previous launched the containers with
 * the same security groups so that the network interface already exists.
 */
public class SecurityGroupFitnessCalculator implements VMTaskFitnessCalculator {
    private static final double SECURITY_GROUPS_NOT_CACHED_SCORE = 0.01;
    private static final double SECURITY_GROUPS_CACHED_SCORE = 1.0;

    private final AgentResourceCache agentResourceCache;

    public SecurityGroupFitnessCalculator(AgentResourceCache agentResourceCache) {
        this.agentResourceCache = agentResourceCache;
    }

    @Override
    public String getName() {
        return "Security Group Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Optional<AgentResourceCacheInstance> instanceOpt = agentResourceCache.getActive(targetVM.getHostname());
        if (instanceOpt.isPresent()) {
            String joinSecurityGroupIds = getJoinedSecurityGroupIds(taskRequest);
            AgentResourceCacheInstance instance = instanceOpt.get();
            Optional<AgentResourceCacheNetworkInterface> matchingNetworkInterface = instance.getNetworkInterfaces()
                    .values().stream().filter(i -> i.getJoinedSecurityGroupIds().equals(joinSecurityGroupIds)).findFirst();
            if (matchingNetworkInterface.isPresent()) {
                return SECURITY_GROUPS_CACHED_SCORE;
            }
        }
        return SECURITY_GROUPS_NOT_CACHED_SCORE;
    }
}
