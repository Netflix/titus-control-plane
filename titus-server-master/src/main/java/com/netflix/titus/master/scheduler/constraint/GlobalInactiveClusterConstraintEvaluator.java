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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.agent.service.AgentManagementFunctions.observeActiveInstanceGroupIds;

/**
 *
 */
public class GlobalInactiveClusterConstraintEvaluator implements GlobalConstraintEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(GlobalInactiveClusterConstraintEvaluator.class);

    private final String serverGroupAttrName;

    private volatile Set<String> activeInstanceGroups = Collections.emptySet();

    public GlobalInactiveClusterConstraintEvaluator(MasterConfiguration config,
                                                    AgentManagementService agentManagementService,
                                                    TitusRuntime titusRuntime) {
        this.serverGroupAttrName = config.getActiveSlaveAttributeName();
        titusRuntime.persistentStream(observeActiveInstanceGroupIds(agentManagementService)).subscribe(this::updateActiveInstanceGroups);
    }

    @Override
    public String getName() {
        return "Global Inactive Cluster Constraint Evaluator";
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        String serverGroupName = LeaseAttributes.getOrDefault(targetVM.getCurrAvailableResources(), serverGroupAttrName, null);

        if (activeInstanceGroups.isEmpty()) {
            logger.debug("Active cluster set empty. Assuming cluster {} is active", serverGroupName);
            return new Result(true, null);
        }

        boolean active = serverGroupName == null || activeInstanceGroups.contains(serverGroupName);
        logger.debug("Cluster {} activation status={}", serverGroupName, active);

        return active ? new Result(true, null) : new Result(false, "Inactive cluster " + serverGroupName);
    }

    private void updateActiveInstanceGroups(List<String> activeInstanceGroups) {
        logger.info("Updating GlobalInactiveClusterConstraintEvaluator active instance group list to: {}", activeInstanceGroups);
        this.activeInstanceGroups = activeInstanceGroups == null ? Collections.emptySet() : new HashSet<>(activeInstanceGroups);
    }
}
