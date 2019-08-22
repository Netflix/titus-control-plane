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

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.master.scheduler.resourcecache.OpportunisticCpuCache;
import com.netflix.titus.master.scheduler.resourcecache.TaskCache;

@Experimental(deadline = "10/2019")
@Singleton
public class OpportunisticCpuConstraint implements SystemConstraint {
    public static final String NAME = OpportunisticCpuConstraint.class.getSimpleName();

    private static final Result VALID = new Result(true, null);
    private static final Result NO_RUNTIME_PREDICTION = new Result(false, "Task requested opportunistic CPUs without a runtime prediction");
    private static final Result NO_MACHINE_ID = new Result(false, "No machine id attribute filled by Fenzo");
    private static final Result NO_OPPORTUNISTIC_CPUS = new Result(false, "The machine does not have opportunistic CPUs available");
    private static final Result AVAILABILITY_NOT_LONG_ENOUGH = new Result(false, "CPU availability on the machine will not last for long enough");
    private static final Result NOT_ENOUGH_OPPORTUNISTIC_CPUS = new Result(false, "The machine does not have enough opportunistic CPUs available");

    private final SchedulerConfiguration configuration;
    private final TaskCache taskCache;
    private final OpportunisticCpuCache opportunisticCpuCache;
    private final TitusRuntime titusRuntime;

    @Inject
    public OpportunisticCpuConstraint(SchedulerConfiguration configuration, TaskCache taskCache, OpportunisticCpuCache opportunisticCpuCache, TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.taskCache = taskCache;
        this.opportunisticCpuCache = opportunisticCpuCache;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Preconditions.checkArgument(taskRequest instanceof V3QueueableTask, "opportunistic CPUs can only be used with V3QueueableTask");
        V3QueueableTask request = (V3QueueableTask) taskRequest;

        if (!request.isCpuOpportunistic()) {
            return VALID;
        }

        Optional<Duration> runtimePrediction = request.getRuntimePrediction();
        if (!runtimePrediction.isPresent()) {
            return NO_RUNTIME_PREDICTION;
        }

        String agentId = SchedulerUtils.getAttributeValueOrEmptyString(targetVM, configuration.getInstanceAttributeName());
        if (StringExt.isEmpty(agentId)) {
            return NO_MACHINE_ID;
        }

        Optional<OpportunisticCpuAvailability> availabilityOpt = opportunisticCpuCache.findAvailableOpportunisticCpus(agentId);
        if (!availabilityOpt.isPresent()) {
            return NO_OPPORTUNISTIC_CPUS;
        }

        Duration taskRuntime = runtimePrediction.get();
        Instant now = Instant.ofEpochMilli(titusRuntime.getClock().wallTime());
        if (now.plus(taskRuntime).isAfter(availabilityOpt.get().getExpiresAt())) {
            return AVAILABILITY_NOT_LONG_ENOUGH;
        }

        if (availabilityOpt.get().getCount() - taskCache.getOpportunisticCpusAllocated(agentId) < request.getOpportunisticCpus()) {
            return NOT_ENOUGH_OPPORTUNISTIC_CPUS;
        }

        return VALID;
    }
}
