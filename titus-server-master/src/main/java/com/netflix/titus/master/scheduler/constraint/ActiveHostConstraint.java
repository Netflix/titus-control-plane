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

import com.google.common.base.Strings;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;

import static com.netflix.titus.master.scheduler.SchedulerUtils.getAttributeValueOrEmptyString;

/**
 * Experimental constraint such that users can prefer to only land on hosts that are part of an active instance group.
 */
@Experimental(deadline = "4/1/2019")
public class ActiveHostConstraint implements ConstraintEvaluator {

    public static final String NAME = "ActiveHostConstraint";

    private static final Result VALID = new Result(true, null);
    private static final Result INVALID = new Result(false, "The host is not active");
    private final SchedulerConfiguration configuration;
    private final AgentManagementService agentManagementService;

    public ActiveHostConstraint(SchedulerConfiguration configuration, AgentManagementService agentManagementService) {
        this.configuration = configuration;
        this.agentManagementService = agentManagementService;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        String instanceGroupAttributeName = configuration.getInstanceGroupAttributeName();
        String instanceGroupId = getAttributeValueOrEmptyString(targetVM, instanceGroupAttributeName);
        if (Strings.isNullOrEmpty(instanceGroupId)) {
            return INVALID;
        }

        Optional<AgentInstanceGroup> instanceGroupOpt = agentManagementService.findInstanceGroup(instanceGroupId);
        if (!instanceGroupOpt.isPresent()) {
            return INVALID;
        }

        AgentInstanceGroup instanceGroup = instanceGroupOpt.get();
        InstanceGroupLifecycleState state = instanceGroup.getLifecycleStatus().getState();

        if (state != InstanceGroupLifecycleState.Active) {
            return INVALID;
        }

        return VALID;
    }
}
