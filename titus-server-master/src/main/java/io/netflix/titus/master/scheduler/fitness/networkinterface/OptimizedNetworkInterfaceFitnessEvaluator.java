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

package io.netflix.titus.master.scheduler.fitness.networkinterface;

import java.util.Optional;

import com.google.common.base.Strings;
import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheNetworkInterface;

/**
 * Prefers to reuse provisioned network resources
 */
public class OptimizedNetworkInterfaceFitnessEvaluator implements PreferentialNamedConsumableResourceEvaluator {

    private enum NetworkInterfaceState {
        // Cases for network interface that is actively used by some tasks
        UsedNoIps,
        UsedSomeIps,

        // Cases for free network interface with matching SG
        FreeMatchingNoIps,
        FreeMatchingSomeIps,

        // Cases for free network interface with not-matching SG
        FreeNotMatchingNoIps,
        FreeNotMatchingSomeIps,

        // Cases for free network interface with no SG assigned
        FreeUnassignedNoIps,
        FreeUnassignedSomeIps,

        // Free unknown network interface (no information about resource allocation)
        FreeUnknown
    }

    private static final double PARTITION_SIZE = 1.0 / (NetworkInterfaceState.values().length);

    private final AgentResourceCache cache;

    public OptimizedNetworkInterfaceFitnessEvaluator(AgentResourceCache cache) {
        this.cache = cache;
    }

    @Override
    public double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit) {
        AgentResourceCacheNetworkInterface networkInterface = null;
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = this.cache.get(hostname);
        if (cacheInstanceOpt.isPresent()) {
            networkInterface = cacheInstanceOpt.get().getNetworkInterface(index);
        }

        if (networkInterface == null) {
            // We know nothing about this network interface
            return evaluateNetworkInterfaceInState(NetworkInterfaceState.FreeUnknown, subResourcesNeeded, 0, subResourcesLimit);
        }

        // Network interface with no security group associated
        if (Strings.isNullOrEmpty(networkInterface.getJoinedSecurityGroupIds())) {
            if (!networkInterface.hasAvailableIps()) {
                return evaluateNetworkInterfaceInState(NetworkInterfaceState.FreeUnassignedNoIps, subResourcesNeeded, 0, subResourcesLimit);
            }
            return evaluateNetworkInterfaceInState(NetworkInterfaceState.FreeUnassignedSomeIps, subResourcesNeeded, 0, subResourcesLimit);
        }

        // Network interface with same security group assigned
        if (networkInterface.getJoinedSecurityGroupIds().equals(resourceName)) {
            if (!networkInterface.hasAvailableIps()) {
                return evaluateNetworkInterfaceInState(NetworkInterfaceState.FreeMatchingNoIps, subResourcesNeeded, 0, subResourcesLimit);
            }
            return evaluateNetworkInterfaceInState(NetworkInterfaceState.FreeMatchingSomeIps, subResourcesNeeded, 0, subResourcesLimit);
        }

        // Network interface with different security group assigned
        if (!networkInterface.hasAvailableIps()) {
            return evaluateNetworkInterfaceInState(NetworkInterfaceState.FreeNotMatchingNoIps, subResourcesNeeded, 0, subResourcesLimit);
        }
        return evaluateNetworkInterfaceInState(NetworkInterfaceState.FreeNotMatchingSomeIps, subResourcesNeeded, 0, subResourcesLimit);
    }

    @Override
    public double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = this.cache.get(hostname);
        AgentResourceCacheNetworkInterface networkInterface = null;
        if (cacheInstanceOpt.isPresent()) {
            networkInterface = cacheInstanceOpt.get().getNetworkInterface(index);
        }
        NetworkInterfaceState networkInterfaceState = (networkInterface != null && networkInterface.hasAvailableIps())
                ? NetworkInterfaceState.UsedSomeIps
                : NetworkInterfaceState.UsedNoIps;

        return evaluateNetworkInterfaceInState(networkInterfaceState, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
    }

    private double evaluateNetworkInterfaceInState(NetworkInterfaceState networkInterfaceState, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        // 0 is the highest priority.
        int priority;
        switch (networkInterfaceState) {
            case UsedSomeIps:
                priority = 0;
                break;
            case FreeMatchingSomeIps:
                priority = 1;
                break;
            case FreeUnassignedSomeIps:
                priority = 2;
                break;
            case FreeNotMatchingSomeIps:
                priority = 3;
                break;
            case UsedNoIps:
                priority = 4;
                break;
            case FreeMatchingNoIps:
                priority = 5;
                break;
            case FreeUnassignedNoIps:
                priority = 6;
                break;
            case FreeUnknown:
                priority = 7;
                break;
            case FreeNotMatchingNoIps:
                priority = 8;
                break;
            default:
                priority = 8;
        }
        double fitness = Math.min(1.0, (subResourcesUsed + subResourcesNeeded + 1.0) / (subResourcesLimit + 1));
        double from = 1.0 - priority * PARTITION_SIZE;
        return from + fitness * PARTITION_SIZE;
    }
}
