package io.netflix.titus.master.scheduler.fitness;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.netflix.fenzo.DefaultPreferentialNamedConsumableResourceEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheNetworkInterface;

/**
 * Network interface preference/fitness evaluator. Two strategies are applied depending on the dynamic configuration
 * setting ({@link SchedulerConfiguration#isOptimizingNetworkInterfaceAllocationEnabled()}):
 * <ul>
 * <li>optimizing evaluator - tries to reuse provisioned network resources</li>
 * <li>spreading evaluator - prefers unused or the least recently used network resources</li>
 * </ul>
 * Spreading mode is recommended in steady system state, as it keeps all resource allocations hot. Optimizing evaluator
 * performs better for higher traffic spikes, as it minimizes amount of work required to provision resources for a
 * container.
 */
@Singleton
public class NetworkInterfaceFitnessEvaluator implements PreferentialNamedConsumableResourceEvaluator {

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
    private final SchedulerConfiguration configuration;

    @Inject
    public NetworkInterfaceFitnessEvaluator(AgentResourceCache cache,
                                            SchedulerConfiguration configuration) {
        this.cache = cache;
        this.configuration = configuration;
    }

    @Override
    public double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit) {
        if (!configuration.isOptimizingNetworkInterfaceAllocationEnabled()) {
            return DefaultPreferentialNamedConsumableResourceEvaluator.INSTANCE.evaluateIdle(hostname, resourceName, index, subResourcesNeeded, subResourcesLimit);
        }

        AgentResourceCacheNetworkInterface networkInterface = null;
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = this.cache.getActive(hostname);
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
        if (!configuration.isOptimizingNetworkInterfaceAllocationEnabled()) {
            return DefaultPreferentialNamedConsumableResourceEvaluator.INSTANCE.evaluate(hostname, resourceName, index, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
        }

        Optional<AgentResourceCacheInstance> cacheInstanceOpt = this.cache.getActive(hostname);
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
