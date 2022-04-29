package com.netflix.titus.master.kubernetes.pod.resourcepool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;

public class MixedSchedulerResourcePoolResolver implements PodResourcePoolResolver {

    private final KubePodConfiguration configuration;

    public MixedSchedulerResourcePoolResolver(KubePodConfiguration configuration) {
        this.configuration = configuration;
    }


    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job, Task task) {
        if (!configuration.isMixedSchedulingEnabled()) {
            return Collections.emptyList();
        }
        String preferredPool = calculatePreferredPool(job);
        ResourcePoolAssignment elasticAssignment = ResourcePoolAssignment.newBuilder()
                .withResourcePoolName(PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC)
                .withPreferred(preferredPool.equals(PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC))
                .withRule("Mixed scheduling pool assignment " + PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC)
                .build();
        ResourcePoolAssignment reservedAssignment = ResourcePoolAssignment.newBuilder()
                .withResourcePoolName(PodResourcePoolResolvers.RESOURCE_POOL_RESERVED)
                .withPreferred(preferredPool.equals(PodResourcePoolResolvers.RESOURCE_POOL_RESERVED))
                .withRule("Mixed scheduling pool assignment " + PodResourcePoolResolvers.RESOURCE_POOL_RESERVED)
                .build();
        return Arrays.asList(elasticAssignment, reservedAssignment);
    }

    /**
     * calculatePreferredPool looks at a job and picks the "best" pool for the job,
     * which is a function of its resources, *not* its capacity group!
     * In a mixed scheduling world, we prefer to put jobs based on where they might fit best
     * based on their ratio, and not due to a policy.
     */
    private String calculatePreferredPool(Job<?> job) {
        double ratio = getRamCPURatio(job);
        if (ratio > 12) {
            return PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC;
        }
        return PodResourcePoolResolvers.RESOURCE_POOL_RESERVED;
    }

    private double getRamCPURatio(Job<?> job) {
        ContainerResources resources = job.getJobDescriptor().getContainer().getContainerResources();
        if (resources.getCpu() == 0) {
            return 100;
        }
        return (resources.getMemoryMB() / 1024.0) / resources.getCpu();
    }
}


