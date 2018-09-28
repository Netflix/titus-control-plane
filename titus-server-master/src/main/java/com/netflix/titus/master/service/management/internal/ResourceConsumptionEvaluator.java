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

package com.netflix.titus.master.service.management.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.CompositeResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption.ConsumptionLevel;
import com.netflix.titus.master.service.management.ResourceConsumptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.CollectionsExt.copyAndRemove;
import static com.netflix.titus.master.service.management.ApplicationSlaManagementService.DEFAULT_APPLICATION;
import static com.netflix.titus.master.service.management.ResourceConsumption.SYSTEM_CONSUMER;

/**
 * Computes current resource consumption.
 */
class ResourceConsumptionEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(ResourceConsumptionEvaluator.class);

    private final V3JobOperations v3JobOperations;
    private final Set<String> definedCapacityGroups;
    private final Map<String, ApplicationSLA> applicationSlaMap;

    private CompositeResourceConsumption systemConsumption;
    private Set<String> undefinedCapacityGroups;

    ResourceConsumptionEvaluator(ApplicationSlaManagementService applicationSlaManagementService,
                                 V3JobOperations v3JobOperations) {
        this.v3JobOperations = v3JobOperations;
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
            ResourceDimension allowedConsumption = ResourceDimensions.multiply(sla.getResourceDimension(), sla.getInstanceCount());
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

        // V3 engine
        v3JobOperations.getJobsAndTasks().forEach(jobsAndTasks -> {
            Job job = jobsAndTasks.getLeft();
            List<Task> tasks = jobsAndTasks.getRight();

            ResourceDimension taskResources = toResourceDimension(job);
            int max = getMaxJobSize(job);

            Map<String, Object> tasksStates = getWorkerStateMap(tasks);
            String appName = job.getJobDescriptor().getApplicationName();

            ResourceConsumption jobConsumption = new ResourceConsumption(
                    appName == null ? DEFAULT_APPLICATION : appName,
                    ConsumptionLevel.Application,
                    ResourceDimensions.multiply(taskResources, getRunningWorkers(tasks).size()),
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

    private int getMaxJobSize(Job<?> job) {
        return JobFunctions.isServiceJob(job)
                ? ((Job<ServiceJobExt>) job).getJobDescriptor().getExtensions().getCapacity().getMax()
                : ((Job<BatchJobExt>) job).getJobDescriptor().getExtensions().getSize();
    }

    private Map<String, Object> getWorkerStateMap(List<Task> tasks) {
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
                                                                                                V3JobOperations v3JobOperations) {
        return () -> {
            ResourceConsumptionEvaluator evaluator = new ResourceConsumptionEvaluator(applicationSlaManagementService, v3JobOperations);
            return new DefaultResourceConsumptionService.ConsumptionEvaluationResult(
                    evaluator.getDefinedCapacityGroups(),
                    evaluator.getUndefinedCapacityGroups(),
                    evaluator.getSystemConsumption()
            );
        };
    }

    @VisibleForTesting
    static ResourceDimension toResourceDimension(Job<?> job) {
        ContainerResources containerResources = job.getJobDescriptor().getContainer().getContainerResources();
        return new ResourceDimension(
                containerResources.getCpu(),
                containerResources.getGpu(),
                containerResources.getMemoryMB(),
                containerResources.getDiskMB(),
                containerResources.getNetworkMbps()
        );
    }

    private List<Task> getRunningWorkers(List<Task> tasks) {
        return tasks.stream().filter(t -> TaskState.isRunning(t.getStatus().getState())).collect(Collectors.toList());
    }
}