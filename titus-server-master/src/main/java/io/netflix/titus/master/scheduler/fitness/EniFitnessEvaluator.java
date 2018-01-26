package io.netflix.titus.master.scheduler.fitness;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheNetworkInterface;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;

@Singleton
public class EniFitnessEvaluator implements PreferentialNamedConsumableResourceEvaluator {

    private enum EniState {
        // Cases for ENI that is actively used by some tasks
        UsedNoIps,
        UsedSomeIps,

        // Cases for free ENI with matching SG
        FreeMatchingNoIps,
        FreeMatchingSomeIps,

        // Cases for free ENI with not-matching SG
        FreeNotMatchingNoIps,
        FreeNotMatchingSomeIps,

        // Cases for free ENI with no SG assigned
        FreeUnassignedNoIps,
        FreeUnassignedSomeIps,

        // Free unknown ENI (no information about resource allocation)
        FreeUnknown
    }

    private static final double PARTITION_SIZE = 1.0 / (EniState.values().length);

    private final AgentResourceCache cache;

    @Inject
    public EniFitnessEvaluator(AgentResourceCache cache) {
        this.cache = cache;
    }

    @Override
    public double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit) {
        AgentResourceCacheNetworkInterface eniCache = null;
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = cache.getActive(hostname);
        if (cacheInstanceOpt.isPresent()) {
            eniCache = cacheInstanceOpt.get().getNetworkInterface(index);
        }

        if (eniCache == null) {
            // We know nothing about this ENI
            return evaluateEniInState(EniState.FreeUnknown, subResourcesNeeded, 0, subResourcesLimit);
        }

        // ENI with no security group associated
        if (eniCache.getSecurityGroupIds() == null) {
            if (eniCache.getIpAddresses().isEmpty()) {
                return evaluateEniInState(EniState.FreeUnassignedNoIps, subResourcesNeeded, 0, subResourcesLimit);
            }
            return evaluateEniInState(EniState.FreeUnassignedSomeIps, subResourcesNeeded, 0, subResourcesLimit);
        }

        // ENI with same security group assigned
//        if(eniCache.getSecurityGroupValue().equals(resourceName)) {
        if (eniCache.getSecurityGroupIds().isEmpty()) {
            if (eniCache.getIpAddresses().isEmpty()) {
                return evaluateEniInState(EniState.FreeMatchingNoIps, subResourcesNeeded, 0, subResourcesLimit);
            }
            return evaluateEniInState(EniState.FreeMatchingSomeIps, subResourcesNeeded, 0, subResourcesLimit);
        }

        // ENI with different security group assigned
        if (eniCache.getIpAddresses().isEmpty()) {
            return evaluateEniInState(EniState.FreeNotMatchingNoIps, subResourcesNeeded, 0, subResourcesLimit);
        }
        return evaluateEniInState(EniState.FreeNotMatchingSomeIps, subResourcesNeeded, 0, subResourcesLimit);
    }

    @Override
    public double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        AgentResourceCacheNetworkInterface eniCache = null;
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = cache.getActive(hostname);
        if (cacheInstanceOpt.isPresent()) {
            eniCache = cacheInstanceOpt.get().getNetworkInterface(index);
        }
        EniState eniState = eniCache == null || eniCache.hasAvailableIps() ? EniState.UsedSomeIps : EniState.UsedNoIps;

        return evaluateEniInState(eniState, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
    }

    private double evaluateEniInState(EniState eniState, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        // 0 is the highest priority.
        int priority;
        switch (eniState) {
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
