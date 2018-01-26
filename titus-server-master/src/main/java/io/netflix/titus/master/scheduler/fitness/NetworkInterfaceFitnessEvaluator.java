package io.netflix.titus.master.scheduler.fitness;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

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
    public NetworkInterfaceFitnessEvaluator(AgentResourceCache cache, SchedulerConfiguration configuration) {
        this.cache = cache;
        this.configuration = configuration;
    }

    @Override
    public double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit) {
        AgentResourceCacheNetworkInterface networkInterface = null;
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = this.cache.getActive(hostname);
        if (cacheInstanceOpt.isPresent()) {
            networkInterface = cacheInstanceOpt.get().getNetworkInterface(index);
        }

        if (networkInterface == null) {
            // We know nothing about this network interface
            return evaluateNetworkInterfaceInState(networkInterface, NetworkInterfaceState.FreeUnknown, subResourcesNeeded, 0, subResourcesLimit);
        }

        // Network interface with no security group associated
        if (networkInterface.getJoinedSecurityGroupIds() == null) {
            if (!networkInterface.hasAvailableIps()) {
                return evaluateNetworkInterfaceInState(networkInterface, NetworkInterfaceState.FreeUnassignedNoIps, subResourcesNeeded, 0, subResourcesLimit);
            }
            return evaluateNetworkInterfaceInState(networkInterface, NetworkInterfaceState.FreeUnassignedSomeIps, subResourcesNeeded, 0, subResourcesLimit);
        }

        // Network interface with same security group assigned
        if (networkInterface.getJoinedSecurityGroupIds().equals(resourceName)) {
            if (!networkInterface.hasAvailableIps()) {
                return evaluateNetworkInterfaceInState(networkInterface, NetworkInterfaceState.FreeMatchingNoIps, subResourcesNeeded, 0, subResourcesLimit);
            }
            return evaluateNetworkInterfaceInState(networkInterface, NetworkInterfaceState.FreeMatchingSomeIps, subResourcesNeeded, 0, subResourcesLimit);
        }

        // Network interface with different security group assigned
        if (!networkInterface.hasAvailableIps()) {
            return evaluateNetworkInterfaceInState(networkInterface, NetworkInterfaceState.FreeNotMatchingNoIps, subResourcesNeeded, 0, subResourcesLimit);
        }
        return evaluateNetworkInterfaceInState(networkInterface, NetworkInterfaceState.FreeNotMatchingSomeIps, subResourcesNeeded, 0, subResourcesLimit);
    }

    @Override
    public double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = this.cache.getActive(hostname);
        AgentResourceCacheNetworkInterface networkInterface = null;
        if (cacheInstanceOpt.isPresent()) {
            networkInterface = cacheInstanceOpt.get().getNetworkInterface(index);
        }
        NetworkInterfaceState networkInterfaceState = (networkInterface != null && networkInterface.hasAvailableIps())
                ? NetworkInterfaceState.UsedSomeIps
                : NetworkInterfaceState.UsedNoIps;

        return evaluateNetworkInterfaceInState(networkInterface, networkInterfaceState, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
    }

    private double evaluateNetworkInterfaceInState(AgentResourceCacheNetworkInterface networkInterface,
                                                   NetworkInterfaceState networkInterfaceState,
                                                   double subResourcesNeeded,
                                                   double subResourcesUsed,
                                                   double subResourcesLimit) {
        if (configuration.isOptimizingNetworkInterfaceAllocationEnabled()) {
            return evaluateNetworkInterfaceInStateOptimized(networkInterfaceState, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
        }
        return evaluateNetworkInterfaceInStateSpreading(networkInterface, networkInterfaceState);
    }

    private double evaluateNetworkInterfaceInStateOptimized(NetworkInterfaceState networkInterfaceState, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
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

    private double evaluateNetworkInterfaceInStateSpreading(AgentResourceCacheNetworkInterface networkInterface,
                                                            NetworkInterfaceState networkInterfaceState) {
        switch (networkInterfaceState) {
            case FreeUnknown:
            case FreeUnassignedNoIps:
                return adjust(fitnessLRU(networkInterface), 0.6, 1.0);
            case FreeNotMatchingSomeIps:
            case FreeNotMatchingNoIps:
                return adjust(fitnessLRU(networkInterface), 0.4, 0.6);
            case FreeMatchingNoIps:
                return adjust(fitnessLRU(networkInterface), 0.2, 0.4);
            case UsedSomeIps:
            case UsedNoIps:
            case FreeMatchingSomeIps:
            case FreeUnassignedSomeIps:
            default:
        }
        return adjust(fitnessLRU(networkInterface), 0.0, 0.2);
    }

    private double adjust(double fitnessLRU, double from, double to) {
        return from + (to - from) * fitnessLRU;
    }

    private double fitnessLRU(AgentResourceCacheNetworkInterface networkInterface) {
        if (networkInterface == null) {
            return 1.0;
        }
        long lastUpdateSec = (System.currentTimeMillis() - networkInterface.getTimestamp()) / 1_000;
        if (lastUpdateSec <= 10) {
            return 0.01;
        }
        return Math.max(0.01, 1.0 - 1.0 / Math.log10(lastUpdateSec));
    }
}
