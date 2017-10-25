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

package io.netflix.titus.master.scheduler;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.scheduler.constraint.CompositeGlobalConstraintEvaluator;
import io.netflix.titus.master.scheduler.constraint.GlobalAgentClusterConstraint;
import io.netflix.titus.master.scheduler.constraint.GlobalConstraintEvaluator;
import io.netflix.titus.master.scheduler.constraint.GlobalInactiveClusterConstraintEvaluator;
import io.netflix.titus.master.scheduler.constraint.GlobalTaskLaunchingConstraintEvaluator;
import io.netflix.titus.master.scheduler.constraint.GlobalTaskResubmitConstraintEvaluator;

import static java.util.Arrays.asList;

@Singleton
public class TitusGlobalConstraintEvaluator implements GlobalConstraintEvaluator {

    private final MasterConfiguration config;
    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor agentStatusMonitor;
    private final TitusRuntime titusRuntime;
    private final GlobalTaskLaunchingConstraintEvaluator globalTaskLaunchingConstraintEvaluator;

    private CompositeGlobalConstraintEvaluator delegate;

    @Inject
    public TitusGlobalConstraintEvaluator(MasterConfiguration config,
                                          SchedulerConfiguration schedulerConfiguration,
                                          AgentManagementService agentManagementService,
                                          AgentStatusMonitor agentStatusMonitor,
                                          TitusRuntime titusRuntime,
                                          GlobalTaskLaunchingConstraintEvaluator globalTaskLaunchingConstraintEvaluator) {
        this.config = config;
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;
        this.titusRuntime = titusRuntime;
        this.globalTaskLaunchingConstraintEvaluator = globalTaskLaunchingConstraintEvaluator;
    }

    @Activator
    public void enterActiveMode() {
        this.delegate = new CompositeGlobalConstraintEvaluator(asList(
                new GlobalInactiveClusterConstraintEvaluator(config, agentManagementService, titusRuntime),
                new GlobalAgentClusterConstraint(config, schedulerConfiguration, agentManagementService, agentStatusMonitor),
                new GlobalTaskResubmitConstraintEvaluator(),
                globalTaskLaunchingConstraintEvaluator
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