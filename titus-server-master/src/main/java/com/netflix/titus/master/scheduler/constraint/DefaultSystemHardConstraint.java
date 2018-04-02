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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.scheduler.systemselector.SystemSelectorConstraintEvaluator;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.systemselector.SystemSelectorConstraintEvaluator;

import static java.util.Arrays.asList;

@Singleton
public class DefaultSystemHardConstraint implements SystemHardConstraint {
    private final MasterConfiguration config;
    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor agentStatusMonitor;
    private final TitusRuntime titusRuntime;
    private final GlobalTaskLaunchingConstraintEvaluator globalTaskLaunchingConstraintEvaluator;
    private final SystemSelectorConstraintEvaluator systemSelectorConstraintEvaluator;

    private CompositeGlobalConstraintEvaluator delegate;

    @Inject
    public DefaultSystemHardConstraint(MasterConfiguration config,
                                       SchedulerConfiguration schedulerConfiguration,
                                       AgentManagementService agentManagementService,
                                       AgentStatusMonitor agentStatusMonitor,
                                       TitusRuntime titusRuntime,
                                       GlobalTaskLaunchingConstraintEvaluator globalTaskLaunchingConstraintEvaluator,
                                       SystemSelectorConstraintEvaluator systemSelectorConstraintEvaluator) {
        this.config = config;
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;
        this.titusRuntime = titusRuntime;
        this.globalTaskLaunchingConstraintEvaluator = globalTaskLaunchingConstraintEvaluator;
        this.systemSelectorConstraintEvaluator = systemSelectorConstraintEvaluator;
    }

    @Activator
    public void enterActiveMode() {
        this.delegate = new CompositeGlobalConstraintEvaluator(asList(
                new GlobalInactiveClusterConstraintEvaluator(config, agentManagementService, titusRuntime),
                new GlobalAgentClusterConstraint(schedulerConfiguration, agentManagementService, agentStatusMonitor),
                new GlobalTaskResubmitConstraintEvaluator(),
                globalTaskLaunchingConstraintEvaluator,
                systemSelectorConstraintEvaluator
        ));

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