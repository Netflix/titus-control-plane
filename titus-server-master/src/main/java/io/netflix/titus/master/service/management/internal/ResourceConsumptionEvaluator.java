/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.service.management.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.code.CodePointTracker;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.model.ResourceDimensions;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import io.netflix.titus.master.service.management.CompositeResourceConsumption;
import io.netflix.titus.master.service.management.ResourceConsumption;
import io.netflix.titus.master.service.management.ResourceConsumption.ConsumptionLevel;
import io.netflix.titus.master.service.management.ResourceConsumptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.common.util.CollectionsExt.copyAndRemove;
import static io.netflix.titus.master.service.management.ApplicationSlaManagementService.DEFAULT_APPLICATION;
import static io.netflix.titus.master.service.management.ResourceConsumption.SYSTEM_CONSUMER;

/**
 * Computes current resource consumption.
 */
class ResourceConsumptionEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(ResourceConsumptionEvaluator.class);

    private final V2JobOperations v2JobOperations;
    private final V3JobOperations v3JobOperations;
    private final CapacityManagementConfiguration config;
    private final Set<String> definedCapacityGroups;
    private final Map<String, ApplicationSLA> applicationSlaMap;

    private CompositeResourceConsumption systemConsumption;
    private Set<String> undefinedCapacityGroups;

    ResourceConsumptionEvaluator(ApplicationSlaManagementService applicationSlaManagementService,
                                 V2JobOperations v2JobOperations,
                                 V3JobOperations v3JobOperations,
                                 CapacityManagementConfiguration config) {
        this.v2JobOperations = v2JobOperations;
        this.v3JobOperations = v3JobOperations;
        this.config = config;
        Collection<ApplicationSLA> applicationSLAs = applicationSlaManagementService.getApplicationSLAs();
        this.definedCapacityGroups = applicationSLAs.stream().map(ApplicationSLA::getAppName).collect(Collectors.toSet());
        this.applicationSlaMap = applicationSLAs.stream().collect(Collectors.toMap(ApplicationSLA::getAppName, Function.identity()));

        Pair<Map<String, Map<String, ResourceConsumption>>, Set<String>> allocationsByCapacityGroupPair = computeAllocationsByCapacityGroupAndAppName();
        this.systemConsumption = buildSystemConsumption(allocationsByCapacityGroupPair.getLeft());
        this.undefinedCapacityGroups = allocationsByCapacityGroupPair.getRight();
    }

    Set<String> getDefinedCapacityGroups() {
        return definedCapacityGroups;
    }

    Set<String> getUndefinedCapacityGroups() {
        return undefinedCapacityGroups;
    }

    public CompositeResourceConsumption getSystemConsumption() {
        return systemConsumption;
    }

    private CompositeResourceConsumption buildSystemConsumption(Map<String, Map<String, ResourceConsumption>> capacityGroupConsumptionMap) {
        // Capacity group level
        Map<Tier, List<CompositeResourceConsumption>> tierConsumptions = new HashMap<>();
        capacityGroupConsumptionMap.forEach((capacityGroup, appConsumptions) -> {

            ApplicationSLA sla = applicationSlaMap.get(capacityGroup);
            double buffer = getBuffer(sla.getTier());
            ResourceDimension allowedConsumption = ResourceDimensions.multiply(sla.getResourceDimension(), sla.getInstanceCount() * (1 + buffer));
            ResourceDimension maxConsumption = ResourceConsumptions.addMaxConsumptions(appConsumptions.values());

            List<Map<String, Object>> attrsList = appConsumptions.values().stream().map(ResourceConsumption::getAttributes).collect(Collectors.toList());
            CompositeResourceConsumption capacityGroupConsumption = new CompositeResourceConsumption(
                    capacityGroup,
                    ConsumptionLevel.CapacityGroup,
                    ResourceConsumptions.addCurrentConsumptions(appConsumptions.values()),
                    maxConsumption,
                    allowedConsumption, ResourceConsumptions.mergeAttributes(attrsList),
                    appConsumptions,
                    !ResourceDimensions.isBigger(allowedConsumption, maxConsumption)
            );
            tierConsumptions.computeIfAbsent(sla.getTier(), t -> new ArrayList<>()).add(capacityGroupConsumption);
        });

        // Tier level
        List<CompositeResourceConsumption> aggregatedTierConsumptions = new ArrayList<>();
        tierConsumptions.forEach((tier, consumptions) ->
                aggregatedTierConsumptions.add(ResourceConsumptions.aggregate(tier.name(), ConsumptionLevel.Tier, consumptions))
        );

        // System level
        return ResourceConsumptions.aggregate(SYSTEM_CONSUMER, ConsumptionLevel.System, aggregatedTierConsumptions);
    }

    private Pair<Map<String, Map<String, ResourceConsumption>>, Set<String>> computeAllocationsByCapacityGroupAndAppName() {
        Map<String, Map<String, ResourceConsumption>> consumptionMap = new HashMap<>();
        Set<String> undefinedCapacityGroups = new HashSet<>();

        // V2 engine
        v2JobOperations.getAllJobMgrs().stream()
                .map(V2JobMgrIntf::getJobMetadata)
                .filter(Objects::nonNull)
                .forEach(jobMetadata -> {
                    V2StageMetadata stageMetadata = jobMetadata.getStageMetadata(1);

                    ResourceDimension taskResources = toResourceDimension(stageMetadata);
                    int max = getMaxJobSize(jobMetadata, stageMetadata);

                    Map<String, Object> tasksStates = getWorkerStateMap(stageMetadata);

                    String appName = Parameters.getAppName(jobMetadata.getParameters());

                    ResourceConsumption jobConsumption = new ResourceConsumption(
                            appName == null ? DEFAULT_APPLICATION : appName,
                            ConsumptionLevel.Application,
                            ResourceDimensions.multiply(taskResources, getRunningWorkers(stageMetadata.getAllWorkers()).size()),
                            ResourceDimensions.multiply(taskResources, max),
                            tasksStates
                    );

                    String capacityGroup = resolveCapacityGroup(undefinedCapacityGroups, jobMetadata, appName);
                    updateConsumptionMap(appName, capacityGroup, jobConsumption, consumptionMap);
                });

        // V3 engine
        v3JobOperations.getJobs().forEach(job -> {
            ResourceDimension taskResources = toResourceDimension(job);
            int max = getMaxJobSize(job);

            Map<String, Object> tasksStates = getWorkerStateMap(job);
            String appName = job.getJobDescriptor().getApplicationName();

            ResourceConsumption jobConsumption = new ResourceConsumption(
                    appName == null ? DEFAULT_APPLICATION : appName,
                    ConsumptionLevel.Application,
                    ResourceDimensions.multiply(taskResources, getRunningWorkers(job).size()),
                    ResourceDimensions.multiply(taskResources, max),
                    tasksStates
            );

            String capacityGroup = resolveCapacityGroup(undefinedCapacityGroups, job, appName);
            updateConsumptionMap(appName, capacityGroup, jobConsumption, consumptionMap);
        });

        // Add unused capacity groups
        copyAndRemove(definedCapacityGroups, consumptionMap.keySet()).forEach(capacityGroup ->
                consumptionMap.put(capacityGroup, Collections.emptyMap())
        );

        return Pair.of(consumptionMap, undefinedCapacityGroups);
    }

    private void updateConsumptionMap(String applicationName,
                                      String capacityGroup,
                                      ResourceConsumption jobConsumption,
                                      Map<String, Map<String, ResourceConsumption>> consumptionMap) {

        Map<String, ResourceConsumption> capacityGroupAllocation = consumptionMap.computeIfAbsent(capacityGroup, k -> new HashMap<>());

        String effectiveAppName = applicationName == null ? DEFAULT_APPLICATION : applicationName;
        ResourceConsumption appAllocation = capacityGroupAllocation.get(effectiveAppName);

        if (appAllocation == null) {
            capacityGroupAllocation.put(effectiveAppName, jobConsumption);
        } else {
            capacityGroupAllocation.put(effectiveAppName, ResourceConsumptions.add(appAllocation, jobConsumption));
        }
    }

    private int getMaxJobSize(V2JobMetadata jobMetadata, V2StageMetadata stageMetadata) {
        String jobId = jobMetadata.getJobId();
        int max;
        Parameters.JobType jobType = Parameters.getJobType(jobMetadata.getParameters());
        if (jobType == Parameters.JobType.Service) {

            // This is a guard, as some old jobs might have no policy defined
            if (stageMetadata.getScalingPolicy() == null) {
                CodePointTracker.mark("scaling policy not defined for job " + jobId);
                logger.warn("Scaling policy for job {} not defined", jobId);
                max = stageMetadata.getNumWorkers();
            } else {
                max = stageMetadata.getScalingPolicy().getMax();
            }
        } else {
            max = stageMetadata.getNumWorkers();
        }
        return max;
    }

    private int getMaxJobSize(Job<?> job) {
        return JobFunctions.isServiceJob(job)
                ? ((Job<ServiceJobExt>) job).getJobDescriptor().getExtensions().getCapacity().getMax()
                : ((Job<BatchJobExt>) job).getJobDescriptor().getExtensions().getSize();
    }

    private Map<String, Object> getWorkerStateMap(V2StageMetadata stageMetadata) {
        List<V2WorkerMetadata> allWorkers = new ArrayList<>(stageMetadata.getAllWorkers());
        Map<String, Object> tasksStates = newTaskStateMap();
        allWorkers.forEach(w -> tasksStates.put(w.getState().name(), (int) tasksStates.get(w.getState().name()) + 1));
        return tasksStates;
    }

    private Map<String, Object> getWorkerStateMap(Job job) {
        List<Task> tasks;
        try {
            tasks = v3JobOperations.getTasks(job.getId());
        } catch (Exception e) {
            return Collections.emptyMap();
        }

        Map<String, Object> tasksStates = newTaskStateMap();
        tasks.stream().map(task -> JobFunctions.toV2JobState(task.getStatus().getState())).forEach(taskState ->
                tasksStates.put(taskState.name(), (int) tasksStates.get(taskState.name()) + 1)
        );
        return tasksStates;
    }

    private Map<String, Object> newTaskStateMap() {
        Map<String, Object> tasksStates = new HashMap<>();
        for (V2JobState state : V2JobState.values()) {
            tasksStates.put(state.name(), 0);
        }
        return tasksStates;
    }

    private String resolveCapacityGroup(Set<String> undefinedCapacityGroups, V2JobMetadata jobMetadata, String appName) {
        String capacityGroup;
        capacityGroup = Parameters.getCapacityGroup(jobMetadata.getParameters());
        if (capacityGroup == null) {
            if (appName != null && definedCapacityGroups.contains(appName)) {
                capacityGroup = appName;
            }
        }
        if (capacityGroup == null) {
            capacityGroup = DEFAULT_APPLICATION;
        } else if (!definedCapacityGroups.contains(capacityGroup)) {
            undefinedCapacityGroups.add(capacityGroup);
            capacityGroup = DEFAULT_APPLICATION;
        }
        return capacityGroup;
    }

    private String resolveCapacityGroup(Set<String> undefinedCapacityGroups, Job job, String appName) {
        String capacityGroup = job.getJobDescriptor().getCapacityGroup();
        if (capacityGroup == null) {
            if (appName != null && definedCapacityGroups.contains(appName)) {
                capacityGroup = appName;
            }
        }
        if (capacityGroup == null) {
            capacityGroup = DEFAULT_APPLICATION;
        } else if (!definedCapacityGroups.contains(capacityGroup)) {
            undefinedCapacityGroups.add(capacityGroup);
            capacityGroup = DEFAULT_APPLICATION;
        }
        return capacityGroup;
    }

    static Supplier<DefaultResourceConsumptionService.ConsumptionEvaluationResult> newEvaluator(ApplicationSlaManagementService applicationSlaManagementService,
                                                                                                V2JobOperations v2JobOperations,
                                                                                                V3JobOperations v3JobOperations,
                                                                                                CapacityManagementConfiguration config) {
        return () -> {
            ResourceConsumptionEvaluator evaluator = new ResourceConsumptionEvaluator(applicationSlaManagementService, v2JobOperations, v3JobOperations, config);
            return new DefaultResourceConsumptionService.ConsumptionEvaluationResult(
                    evaluator.getDefinedCapacityGroups(),
                    evaluator.getUndefinedCapacityGroups(),
                    evaluator.getSystemConsumption()
            );
        };
    }

    @VisibleForTesting
    static ResourceDimension toResourceDimension(V2StageMetadata stageMetadata) {
        MachineDefinition machineDefinition = stageMetadata.getMachineDefinition();
        double gpu;
        try {
            gpu = machineDefinition.getScalars().get("gpu");
        } catch (Exception ignore) {
            gpu = 0;
        }
        return new ResourceDimension(
                machineDefinition.getCpuCores(),
                (int) gpu,
                (int) machineDefinition.getMemoryMB(),
                (int) machineDefinition.getDiskMB(),
                (int) machineDefinition.getNetworkMbps()
        );
    }

    private static ResourceDimension toResourceDimension(Job<?> job) {
        ContainerResources containerResources = job.getJobDescriptor().getContainer().getContainerResources();
        return new ResourceDimension(
                containerResources.getCpu(),
                containerResources.getGpu(),
                containerResources.getMemoryMB(),
                containerResources.getDiskMB(),
                containerResources.getNetworkMbps()
        );
    }

    private List<? extends V2WorkerMetadata> getRunningWorkers(Collection<V2WorkerMetadata> allWorkers) {
        return allWorkers.stream().filter(t -> V2JobState.isRunningState(t.getState())).collect(Collectors.toList());
    }

    private List<Task> getRunningWorkers(Job<?> job) {
        return v3JobOperations.getTasks(job.getId()).stream().filter(t -> TaskState.isRunning(t.getStatus().getState())).collect(Collectors.toList());
    }

    private double getBuffer(Tier tier) {
        double buffer = 0.0;
        if (tier == Tier.Critical) {
            buffer = config.getCriticalTierBuffer();
        } else if (tier == Tier.Flex) {
            buffer = config.getFlexTierBuffer();
        }
        return buffer;
    }
}