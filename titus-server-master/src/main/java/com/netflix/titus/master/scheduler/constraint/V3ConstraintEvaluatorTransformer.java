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

package com.netflix.titus.master.scheduler.constraint;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.AsSoftConstraint;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps V3 API constraint definitions into Fenzo model.
 * Supported Constraints:
 * <ul>
 * <li>exclusiveHost</li>
 * <li>uniqueHost</li>
 * <li>zoneBalance</li>
 * </ul>
 * <p>
 * Experimental Constraints:
 * <ul>
 * <li>activeHost</li>
 * <li>availabilityZone</li>
 * <li>machineId</li>
 * <li>machineType</li>
 * </ul>
 */
@Singleton
public class V3ConstraintEvaluatorTransformer implements ConstraintEvaluatorTransformer<Pair<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(V3ConstraintEvaluatorTransformer.class);

    private static final int EXPECTED_NUM_ZONES = 3;

    private static final ExclusiveHostConstraint EXCLUSIVE_HOST_CONSTRAINT = new ExclusiveHostConstraint();
    private static final String EXCLUSIVE_HOST = "exclusivehost";
    private static final String UNIQUE_HOST = "uniquehost";
    private static final String ZONE_BALANCE = "zonebalance";

    // Experimental Constraints
    private static final String ACTIVE_HOST = "activehost";
    private static final String AVAILABILITY_ZONE = "availabilityzone";
    private static final String MACHINE_ID = "machineid";
    private static final String MACHINE_TYPE = "machinetype";

    private final MasterConfiguration config;
    private final SchedulerConfiguration schedulerConfiguration;
    private final TaskCache taskCache;
    private final AgentManagementService agentManagementService;

    @Inject
    public V3ConstraintEvaluatorTransformer(MasterConfiguration config,
                                            SchedulerConfiguration schedulerConfiguration,
                                            TaskCache taskCache,
                                            AgentManagementService agentManagementService) {
        this.config = config;
        this.schedulerConfiguration = schedulerConfiguration;
        this.taskCache = taskCache;
        this.agentManagementService = agentManagementService;
    }

    @Override
    public Optional<ConstraintEvaluator> hardConstraint(Pair<String, String> hardConstraint, Supplier<Set<String>> activeTasksGetter) {
        String name = hardConstraint.getLeft();
        String value = hardConstraint.getRight();
        switch (name.toLowerCase()) {
            case EXCLUSIVE_HOST:
                return "true".equals(value) ? Optional.of(EXCLUSIVE_HOST_CONSTRAINT) : Optional.empty();
            case UNIQUE_HOST:
                return "true".equals(value) ? Optional.of(new V3UniqueHostConstraint()) : Optional.empty();
            case ZONE_BALANCE:
                return "true".equals(value)
                        ? Optional.of(new V3ZoneBalancedHardConstraintEvaluator(taskCache, EXPECTED_NUM_ZONES, config.getHostZoneAttributeName()))
                        : Optional.empty();
            case ACTIVE_HOST:
                return "true".equals(value)
                        ? Optional.of(new ActiveHostConstraint(schedulerConfiguration, agentManagementService))
                        : Optional.empty();
            case AVAILABILITY_ZONE:
                return StringExt.isNotEmpty(value)
                        ? Optional.of(new AvailabilityZoneConstraint(schedulerConfiguration, agentManagementService, value))
                        : Optional.empty();
            case MACHINE_ID:
                return StringExt.isNotEmpty(value)
                        ? Optional.of(new MachineIdConstraint(schedulerConfiguration, agentManagementService, value))
                        : Optional.empty();
            case MACHINE_TYPE:
                return StringExt.isNotEmpty(value)
                        ? Optional.of(new MachineTypeConstraint(schedulerConfiguration, agentManagementService, value))
                        : Optional.empty();
        }
        logger.error("Unknown or not supported job hard constraint: {}", name);
        return Optional.empty();
    }

    @Override
    public Optional<VMTaskFitnessCalculator> softConstraint(Pair<String, String> softConstraints, Supplier<Set<String>> activeTasksGetter) {
        String name = softConstraints.getLeft();
        String value = softConstraints.getRight();
        switch (name.toLowerCase()) {
            case EXCLUSIVE_HOST:
                return "true".equals(value) ? Optional.of(AsSoftConstraint.get(EXCLUSIVE_HOST_CONSTRAINT)) : Optional.empty();
            case UNIQUE_HOST:
                return "true".equals(value) ? Optional.of(AsSoftConstraint.get(new V3UniqueHostConstraint())) : Optional.empty();
            case ZONE_BALANCE:
                return "true".equals(value)
                        ? Optional.of(new V3ZoneBalancedFitnessCalculator(taskCache, EXPECTED_NUM_ZONES, config.getHostZoneAttributeName()))
                        : Optional.empty();
            case ACTIVE_HOST:
                return "true".equals(value)
                        ? Optional.of(AsSoftConstraint.get(new ActiveHostConstraint(schedulerConfiguration, agentManagementService)))
                        : Optional.empty();
            case AVAILABILITY_ZONE:
                return StringExt.isNotEmpty(value)
                        ? Optional.of(AsSoftConstraint.get(new AvailabilityZoneConstraint(schedulerConfiguration, agentManagementService, value)))
                        : Optional.empty();
            case MACHINE_ID:
                return StringExt.isNotEmpty(value)
                        ? Optional.of(AsSoftConstraint.get(new MachineIdConstraint(schedulerConfiguration, agentManagementService, value)))
                        : Optional.empty();
            case MACHINE_TYPE:
                return StringExt.isNotEmpty(value)
                        ? Optional.of(AsSoftConstraint.get(new MachineTypeConstraint(schedulerConfiguration, agentManagementService, value)))
                        : Optional.empty();
        }
        logger.error("Unknown or not supported job hard constraint: {}", name);
        return Optional.empty();
    }
}
