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
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
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
import io.netflix.titus.master.ApiOperations;
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

    private final ApiOperations apiOperations;
    private final CapacityManagementConfiguration config;
    private final Set<String> definedCapacityGroups;
    private final Map<String, ApplicationSLA> applicationSlaMap;

    private CompositeResourceConsumption systemConsumption;
    private Set<String> undefinedCapacityGroups;

    ResourceConsumptionEvaluator(ApplicationSlaManagementService applicationSlaManagementService,
                                 ApiOperations apiOperations,
                                 CapacityManagementConfiguration config) {
        this.apiOperations = apiOperations;
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

        Set<String> allActiveJobs = apiOperations.getAllActiveJobs();
        for (String jobId : allActiveJobs) {
            // FIXME This is potentially blocking call.
            final V2JobMetadata jobMetadata = apiOperations.getJobMetadata(jobId);
            if(jobMetadata == null) {
                continue;
            }
            V2StageMetadata stageMetadata = jobMetadata.getStageMetadata(1);

            ResourceDimension taskResources = toResourceDimension(stageMetadata);

            Parameters.JobType jobType = Parameters.getJobType(jobMetadata.getParameters());
            int max;
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

            List<V2WorkerMetadata> allWorkers = apiOperations.getAllWorkers(jobId);

            Map<String, Object> tasksStates = new HashMap<>();
            for (V2JobState state : V2JobState.values()) {
                tasksStates.put(state.name(), 0);
            }
            allWorkers.forEach(w -> tasksStates.put(w.getState().name(), (int) (tasksStates.get(w.getState().name())) + 1));

            String appName = Parameters.getAppName(jobMetadata.getParameters());

            ResourceConsumption jobConsumption = new ResourceConsumption(
                    appName == null ? DEFAULT_APPLICATION : appName,
                    ConsumptionLevel.Application,
                    ResourceDimensions.multiply(taskResources, apiOperations.getRunningWorkers(jobId).size()),
                    ResourceDimensions.multiply(taskResources, max),
                    tasksStates
            );

            String capacityGroup = Parameters.getCapacityGroup(jobMetadata.getParameters());
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

            Map<String, ResourceConsumption> capacityGroupAllocation = consumptionMap.computeIfAbsent(capacityGroup, k -> new HashMap<>());

            String effectiveAppName = appName == null ? DEFAULT_APPLICATION : appName;
            ResourceConsumption appAllocation = capacityGroupAllocation.get(effectiveAppName);
            if (appAllocation == null) {
                capacityGroupAllocation.put(effectiveAppName, jobConsumption);
            } else {
                capacityGroupAllocation.put(effectiveAppName, ResourceConsumptions.add(appAllocation, jobConsumption));
            }
        }

        // Add unused capacity groups
        copyAndRemove(definedCapacityGroups, consumptionMap.keySet()).forEach(capacityGroup ->
                consumptionMap.put(capacityGroup, Collections.emptyMap())
        );

        return Pair.of(consumptionMap, undefinedCapacityGroups);
    }

    static Supplier<DefaultResourceConsumptionService.ConsumptionEvaluationResult> newEvaluator(ApplicationSlaManagementService applicationSlaManagementService,
                                                                                                ApiOperations apiOperations,
                                                                                                CapacityManagementConfiguration config) {
        return () -> {
            ResourceConsumptionEvaluator evaluator = new ResourceConsumptionEvaluator(applicationSlaManagementService, apiOperations, config);
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