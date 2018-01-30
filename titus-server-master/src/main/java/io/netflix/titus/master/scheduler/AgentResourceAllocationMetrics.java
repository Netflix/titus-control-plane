package io.netflix.titus.master.scheduler;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.primitives.Doubles;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.MetricConstants;

/**
 * Metrics for available agent resources.
 */
final class AgentResourceAllocationMetrics {

    private static final String ROOT_NAME = MetricConstants.METRIC_SCHEDULING_SERVICE + "availableResources";

    private final AgentManagementService agentManagementService;
    private final Registry registry;
    private final ConcurrentMap<String, AgentMetrics> agentMetrics = new ConcurrentHashMap<>();

    AgentResourceAllocationMetrics(AgentManagementService agentManagementService,
                                   TitusRuntime titusRuntime) {
        this.agentManagementService = agentManagementService;
        this.registry = titusRuntime.getRegistry();
    }

    void update(List<VirtualMachineCurrentState> vmStates) {
        vmStates.forEach(vms -> agentMetrics.computeIfAbsent(vms.getHostname(), h -> new AgentMetrics(vms)).update(vms));
        Set<String> toRemove = CollectionsExt.copyAndRemove(
                agentMetrics.keySet(),
                vmStates.stream().map(VirtualMachineCurrentState::getHostname).collect(Collectors.toSet())
        );
        toRemove.forEach(hostname -> agentMetrics.remove(hostname).clear());
    }

    class ResourceMetrics {
        private final Gauge availableGauge;
        private final Gauge totalGauge;
        private final Gauge percentageGauge;

        private final double total;

        ResourceMetrics(Id baseId, double total) {
            this.availableGauge = registry.gauge(baseId.withTag("measure", "available"));
            this.percentageGauge = registry.gauge(baseId.withTag("measure", "percentage"));
            this.totalGauge = registry.gauge(baseId.withTag("measure", "total"));
            this.total = total;
            totalGauge.set(total);
        }

        void update(double available) {
            availableGauge.set(available);
            percentageGauge.set(asPercentage(available, total));
        }

        void clear() {
            availableGauge.set(0);
            totalGauge.set(0);
            percentageGauge.set(0);
        }

        private int asPercentage(double available, double total) {
            return total == 0 ? 0 : (int) (100 * available / total);
        }
    }

    class AgentMetrics {

        private final ResourceMetrics cpuMetrics;
        private final ResourceMetrics gpuMetrics;
        private final ResourceMetrics memoryMetrics;
        private final ResourceMetrics diskMetrics;
        private final ResourceMetrics networkMetrics;
        private final Gauge dominantResourcPercentageGauge;

        AgentMetrics(VirtualMachineCurrentState vms) {
            String instanceType;
            String instanceGroupId;
            ResourceDimension instanceDimension;
            List<Pair<AgentInstanceGroup, List<AgentInstance>>> matchingInstances = agentManagementService.findAgentInstances(pair ->
                    Objects.equals(vms.getHostname(), pair.getRight().getHostname()) || Objects.equals(vms.getHostname(), pair.getRight().getIpAddress())
            );
            if (matchingInstances.size() == 1) {
                AgentInstanceGroup instanceGroup = matchingInstances.get(0).getLeft();
                instanceDimension = instanceGroup.getResourceDimension();
                instanceType = instanceGroup.getInstanceType();
                instanceGroupId = instanceGroup.getId();
            } else {
                instanceDimension = ResourceDimension.empty();
                instanceType = "UNKNOWN";
                instanceGroupId = "UNKNOWN";
            }

            Id baseId = registry.createId(ROOT_NAME,
                    "instanceGroupId", instanceGroupId,
                    "instanceType", instanceType,
                    "hostname", vms.getHostname()
            );

            this.cpuMetrics = new ResourceMetrics(baseId.withTag("resourceType", "cpu"), instanceDimension.getCpu());
            if (instanceDimension.getGpu() > 0) {
                this.gpuMetrics = new ResourceMetrics(baseId.withTag("resourceType", "gpu"), instanceDimension.getGpu());
            } else {
                this.gpuMetrics = null;
            }
            this.memoryMetrics = new ResourceMetrics(baseId.withTag("resourceType", "memoryMB"), instanceDimension.getMemoryMB());
            this.diskMetrics = new ResourceMetrics(baseId.withTag("resourceType", "diskMB"), instanceDimension.getDiskMB());
            this.networkMetrics = new ResourceMetrics(baseId.withTag("resourceType", "networkMbps"), instanceDimension.getNetworkMbs());
            this.dominantResourcPercentageGauge = registry.gauge(baseId
                    .withTag("resourceType", "dominantResource")
                    .withTag("measure", "percentage")
            );
        }

        void update(VirtualMachineCurrentState vms) {
            VirtualMachineLease availableResources = vms.getCurrAvailableResources();
            cpuMetrics.update(availableResources.cpuCores());
            memoryMetrics.update(availableResources.memoryMB());
            diskMetrics.update(availableResources.diskMB());
            networkMetrics.update(availableResources.networkMbps());

            double dominantResourcePercentage = Doubles.min(cpuMetrics.percentageGauge.value(),
                    memoryMetrics.percentageGauge.value(),
                    diskMetrics.percentageGauge.value(),
                    networkMetrics.percentageGauge.value()
            );

            if (gpuMetrics != null) {
                Double gpu = availableResources.getScalarValue("gpu");
                gpuMetrics.update(gpu == null ? 0.0 : gpu);
                dominantResourcePercentage = Math.min(dominantResourcePercentage, gpuMetrics.percentageGauge.value());
            }

            dominantResourcPercentageGauge.set(dominantResourcePercentage);
        }

        void clear() {
            cpuMetrics.clear();
            if (gpuMetrics != null) {
                gpuMetrics.clear();
            }
            memoryMetrics.clear();
            diskMetrics.clear();
            networkMetrics.clear();
            dominantResourcPercentageGauge.set(0);
        }
    }
}
