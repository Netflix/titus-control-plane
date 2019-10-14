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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.master.scheduler.resourcecache.OpportunisticCpuCache;
import com.netflix.titus.master.scheduler.resourcecache.TaskCache;

@Experimental(deadline = "10/2019")
@Singleton
public class OpportunisticCpuConstraint implements SystemConstraint {
    public static final String NAME = OpportunisticCpuConstraint.class.getSimpleName();

    private static final Result VALID = new Result(true, null);

    private enum Failure {
        NO_RUNTIME_PREDICTION("Task requested opportunistic CPUs without a runtime prediction"),
        NO_MACHINE_ID("No machine id attribute filled by Fenzo"),
        NO_OPPORTUNISTIC_CPUS("The machine does not have opportunistic CPUs available"),
        AVAILABILITY_NOT_LONG_ENOUGH("CPU availability on the machine will not last for long enough"),
        NOT_ENOUGH_OPPORTUNISTIC_CPUS("The machine does not have enough opportunistic CPUs available");

        private Result result;

        Failure(String reason) {
            this.result = new Result(false, reason);
        }

        public Result toResult() {
            return result;
        }
    }

    private static final Set<String> FAILURE_REASONS = Stream.of(Failure.values())
            .map(f -> f.toResult().getFailureReason())
            .collect(Collectors.toSet());

    public static boolean isOpportunisticCpuConstraintReason(String reason) {
        return reason != null && FAILURE_REASONS.contains(reason);
    }

    private final SchedulerConfiguration configuration;
    private final FeatureActivationConfiguration featureConfiguration;
    private final TaskCache taskCache;
    private final OpportunisticCpuCache opportunisticCpuCache;
    private final TitusRuntime titusRuntime;

    @Inject
    public OpportunisticCpuConstraint(SchedulerConfiguration configuration,
                                      FeatureActivationConfiguration featureConfiguration,
                                      TaskCache taskCache,
                                      OpportunisticCpuCache opportunisticCpuCache,
                                      TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.featureConfiguration = featureConfiguration;
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
            return Failure.NO_RUNTIME_PREDICTION.toResult();
        }

        String machineId = targetVM.getHostname();
        if (StringExt.isEmpty(machineId)) {
            return Failure.NO_MACHINE_ID.toResult();
        }

        Optional<OpportunisticCpuAvailability> availabilityOpt = opportunisticCpuCache.findAvailableOpportunisticCpus(machineId);
        if (!availabilityOpt.isPresent()) {
            return Failure.NO_OPPORTUNISTIC_CPUS.toResult();
        }

        Duration taskRuntime = runtimePrediction.get();
        Instant now = Instant.ofEpochMilli(titusRuntime.getClock().wallTime());
        if (now.plus(taskRuntime).isAfter(availabilityOpt.get().getExpiresAt())) {
            return Failure.AVAILABILITY_NOT_LONG_ENOUGH.toResult();
        }

        if (availabilityOpt.get().getCount() - taskCache.getOpportunisticCpusAllocated(machineId) < request.getOpportunisticCpus()) {
            return Failure.NOT_ENOUGH_OPPORTUNISTIC_CPUS.toResult();
        }

        return VALID;
    }
}
