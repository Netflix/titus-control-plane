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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.service.management.CompositeResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption.ConsumptionLevel;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.ResourceConsumptionEvent;
import com.netflix.titus.master.service.management.ResourceConsumptions;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;

/**
 * A helper class to generate test data for capacity groups and resource consumption.
 */
class ConsumptionModelGenerator {

    static final ApplicationSLA CRITICAL_SLA_1 = ApplicationSlaSample.CriticalSmall.build();
    static final ApplicationSLA NOT_USED_SLA = ApplicationSlaSample.CriticalLarge.build();
    static final ApplicationSLA DEFAULT_SLA = ApplicationSlaSample.DefaultFlex.build();

    private final Map<String, ApplicationSLA> capacityGroupMap = new HashMap<>();

    private final Map<String, Map<String, ResourceDimension>> actualConsumptionByGroupAndApp = new HashMap<>();
    private final Map<String, Map<String, ResourceDimension>> maxConsumptionByApp = new HashMap<>();

    public ConsumptionModelGenerator() {
        capacityGroupMap.put(CRITICAL_SLA_1.getAppName(), CRITICAL_SLA_1);
        capacityGroupMap.put(NOT_USED_SLA.getAppName(), NOT_USED_SLA);
        capacityGroupMap.put(DEFAULT_SLA.getAppName(), DEFAULT_SLA);
    }

    void addConsumption(String capacityGroup, String appName, ResourceDimension actualConsumption, ResourceDimension maxConsumption) {
        addConsumption(capacityGroup, appName, actualConsumption, actualConsumptionByGroupAndApp);
        addConsumption(capacityGroup, appName, maxConsumption, maxConsumptionByApp);
    }

    private void addConsumption(String capacityGroup, String appName, ResourceDimension consumption, Map<String, Map<String, ResourceDimension>> output) {
        Map<String, ResourceDimension> appsMap = output.get(capacityGroup);
        if (appsMap == null) {
            output.put(capacityGroup, appsMap = new HashMap<>());
        }
        if (appsMap.get(appName) == null) {
            appsMap.put(appName, consumption);
        } else {
            appsMap.put(
                    appName,
                    ResourceDimensions.add(appsMap.get(appName), consumption)
            );
        }
    }

    Map<String, ApplicationSLA> getCapacityGroupMap() {
        return new HashMap<>(capacityGroupMap);
    }

    Set<String> getDefinedCapacityGroupNames() {
        return capacityGroupMap.keySet();
    }

    void removeCapacityGroup(String capacityGroup) {
        Preconditions.checkState(capacityGroupMap.containsKey(capacityGroup));
        capacityGroupMap.remove(capacityGroup);
    }

    DefaultResourceConsumptionService.ConsumptionEvaluationResult getEvaluation() {
        Map<String, CompositeResourceConsumption> groupConsumptionMap = new HashMap<>();

        Set<String> definedCapacityGroups = new HashSet<>(capacityGroupMap.keySet());

        // Used capacity groups

        Set<String> capacityGroupNames = actualConsumptionByGroupAndApp.keySet();
        for (String capacityGroupName : capacityGroupNames) {
            List<ResourceConsumption> appConsumptions = buildApplicationConsumptions(capacityGroupName);

            CompositeResourceConsumption groupConsumption = ResourceConsumptions.aggregate(
                    capacityGroupName,
                    ConsumptionLevel.CapacityGroup,
                    appConsumptions,
                    capacityGroupLimit(capacityGroupMap.getOrDefault(capacityGroupName, DEFAULT_SLA))
            );

            groupConsumptionMap.put(capacityGroupName, groupConsumption);
        }

        // Unused capacity groups
        CollectionsExt.copyAndRemove(definedCapacityGroups, capacityGroupNames).forEach(capacityGroup -> {
            ApplicationSLA sla = capacityGroupMap.getOrDefault(capacityGroup, DEFAULT_SLA);
            ResourceDimension limit = capacityGroupLimit(sla);
            groupConsumptionMap.put(capacityGroup, new CompositeResourceConsumption(
                    capacityGroup,
                    ConsumptionLevel.CapacityGroup,
                    ResourceDimension.empty(),
                    ResourceDimension.empty(),
                    limit,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    false
            ));
        });

        // Undefined capacity groups
        Set<String> undefinedCapacityGroups = CollectionsExt.copyAndRemove(capacityGroupNames, definedCapacityGroups);

        // Tier consumption
        Map<Tier, List<CompositeResourceConsumption>> tierCapacityGroups = groupConsumptionMap.values().stream()
                .collect(Collectors.groupingBy(rc -> {
                    ApplicationSLA sla = capacityGroupMap.get(rc.getConsumerName());
                    if (sla == null) {
                        sla = capacityGroupMap.get(DEFAULT_SLA.getAppName());
                    }
                    return sla.getTier();
                }));
        Map<String, CompositeResourceConsumption> tierConsumptions = new HashMap<>();
        tierCapacityGroups.forEach((tier, consumptions) ->
                tierConsumptions.put(
                        tier.name(),
                        ResourceConsumptions.aggregate(
                                tier.name(),
                                ConsumptionLevel.Tier,
                                consumptions
                        )
                ));

        // System consumption
        CompositeResourceConsumption systemConsumption = ResourceConsumptions.aggregate(
                ResourceConsumption.SYSTEM_CONSUMER,
                ConsumptionLevel.System,
                tierConsumptions.values()
        );

        return new DefaultResourceConsumptionService.ConsumptionEvaluationResult(
                definedCapacityGroups,
                undefinedCapacityGroups,
                systemConsumption
        );
    }

    private List<ResourceConsumption> buildApplicationConsumptions(String capacityGroupName) {
        Map<String, ResourceDimension> actual = actualConsumptionByGroupAndApp.get(capacityGroupName);
        Map<String, ResourceDimension> max = maxConsumptionByApp.get(capacityGroupName);

        List<ResourceConsumption> appConsumptions = new ArrayList<>();
        for (String appName : actual.keySet()) {
            appConsumptions.add(
                    new ResourceConsumption(
                            appName,
                            ConsumptionLevel.Application,
                            actual.get(appName),
                            max.get(appName),
                            Collections.emptyMap()
                    )
            );
        }

        return appConsumptions;
    }

    ResourceDimension capacityGroupLimit(String capacityGroup) {
        ApplicationSLA sla = capacityGroupMap.get(capacityGroup);
        Preconditions.checkNotNull(sla, "Unknown capacity group " + capacityGroup);
        return ResourceDimensions.multiply(sla.getResourceDimension(), sla.getInstanceCount());
    }

    static ResourceDimension capacityGroupLimit(ApplicationSLA sla) {
        return ResourceDimensions.multiply(sla.getResourceDimension(), sla.getInstanceCount());
    }

    static ResourceDimension singleWorkerConsumptionOf(V2JobMetadata jobMetadata) {
        return ResourceConsumptionEvaluator.toResourceDimension(jobMetadata.getStageMetadata(1));
    }

    static <E extends ResourceConsumptionEvent> Optional<E> findEvent(List<ResourceConsumptionEvent> events, Class<E> eventClass, String capacityGroup) {
        for (ResourceConsumptionEvent event : events) {
            if (eventClass.isAssignableFrom(event.getClass()) && event.getCapacityGroup().equals(capacityGroup)) {
                return Optional.of((E) event);
            }
        }
        return Optional.empty();
    }
}
