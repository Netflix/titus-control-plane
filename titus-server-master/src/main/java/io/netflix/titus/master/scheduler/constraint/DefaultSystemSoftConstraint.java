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

package io.netflix.titus.master.scheduler.constraint;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.plugins.WeightedAverageFitnessCalculator;
import com.netflix.fenzo.plugins.WeightedAverageFitnessCalculator.WeightedFitnessCalculator;
import io.netflix.titus.master.scheduler.fitness.AgentManagementFitnessCalculator;
import io.netflix.titus.master.scheduler.systemselector.SystemSelectorFitnessCalculator;

import static java.util.Arrays.asList;

@Singleton
public class DefaultSystemSoftConstraint implements SystemSoftConstraint {
    private final WeightedAverageFitnessCalculator delegate;

    @Inject
    public DefaultSystemSoftConstraint(AgentManagementFitnessCalculator agentManagementFitnessCalculator,
                                       SystemSelectorFitnessCalculator systemSelectorFitnessCalculator) {
        List<WeightedFitnessCalculator> calculators = asList(
                new WeightedFitnessCalculator(agentManagementFitnessCalculator, 0.6),
                new WeightedFitnessCalculator(systemSelectorFitnessCalculator, 0.4)
        );
        delegate = new WeightedAverageFitnessCalculator(calculators);
    }

    @Override
    public String getName() {
        return "System Soft Constraint";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        return delegate.calculateFitness(taskRequest, targetVM, taskTrackerState);
    }
}