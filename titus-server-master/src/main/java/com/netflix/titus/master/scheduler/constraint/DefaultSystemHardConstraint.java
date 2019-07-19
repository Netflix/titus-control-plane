/*
 * Copyright 2019 Netflix, Inc.
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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.scheduler.systemselector.SystemSelectorConstraintEvaluator;

import static java.util.Arrays.asList;

@Singleton
public class DefaultSystemHardConstraint implements SystemHardConstraint {

    public static final String NAME = "DefaultSystemHardConstraint";

    private final AgentManagementConstraint agentManagementConstraint;
    private final AgentLaunchGuardConstraint agentLaunchGuardConstraint;
    private final SystemSelectorConstraintEvaluator systemSelectorConstraintEvaluator;
    private final IpAllocationConstraint ipAllocationConstraint;

    private CompositeSystemConstraint delegate;

    @Inject
    public DefaultSystemHardConstraint(AgentManagementConstraint agentManagementConstraint,
                                       AgentLaunchGuardConstraint agentLaunchGuardConstraint,
                                       SystemSelectorConstraintEvaluator systemSelectorConstraintEvaluator,
                                       IpAllocationConstraint ipAllocationConstraint) {
        this.agentManagementConstraint = agentManagementConstraint;
        this.agentLaunchGuardConstraint = agentLaunchGuardConstraint;
        this.systemSelectorConstraintEvaluator = systemSelectorConstraintEvaluator;
        this.ipAllocationConstraint = ipAllocationConstraint;
    }

    @Activator
    public void enterActiveMode() {
        this.delegate = new CompositeSystemConstraint(asList(
                agentManagementConstraint,
                agentLaunchGuardConstraint,
                systemSelectorConstraintEvaluator,
                ipAllocationConstraint
        ));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Preconditions.checkNotNull(delegate, "System activation not finished yet");
        return delegate.evaluate(taskRequest, targetVM, taskTrackerState);
    }

    @Override
    public void prepare() {
        delegate.prepare();
    }
}