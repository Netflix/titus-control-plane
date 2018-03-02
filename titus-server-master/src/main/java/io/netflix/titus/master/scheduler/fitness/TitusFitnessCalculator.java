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

package io.netflix.titus.master.scheduler.fitness;

import java.util.ArrayList;
import java.util.List;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.netflix.fenzo.plugins.WeightedAverageFitnessCalculator;
import com.netflix.fenzo.plugins.WeightedAverageFitnessCalculator.WeightedFitnessCalculator;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;

import static io.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.isCriticalTier;
import static io.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.isServiceJob;

public class TitusFitnessCalculator implements VMTaskFitnessCalculator {

    private final SchedulerConfiguration configuration;
    private final VMTaskFitnessCalculator criticalServiceJobSpreader;
    private final VMTaskFitnessCalculator criticalServiceJobBinPacker;
    private final VMTaskFitnessCalculator defaultFitnessCalculator;

    public static final com.netflix.fenzo.functions.Func1<Double, Boolean> fitnessGoodEnoughFunction =
            f -> f > 0.9;

    public TitusFitnessCalculator(SchedulerConfiguration configuration, AgentResourceCache agentResourceCache) {
        this.configuration = configuration;
        this.criticalServiceJobSpreader = criticalServiceJobSpreader();
        this.criticalServiceJobBinPacker = criticalServiceJobBinPacker(agentResourceCache);
        this.defaultFitnessCalculator = defaultFitnessCalculator(agentResourceCache);
    }

    @Override
    public String getName() {
        return "Titus Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        if (isCriticalTier(taskRequest) && isServiceJob(taskRequest)) {
            if (configuration.isCriticalServiceJobSpreadingEnabled()) {
                return criticalServiceJobSpreader.calculateFitness(taskRequest, targetVM, taskTrackerState);
            } else {
                return criticalServiceJobBinPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
            }
        }
        return defaultFitnessCalculator.calculateFitness(taskRequest, targetVM, taskTrackerState);
    }

    private VMTaskFitnessCalculator criticalServiceJobSpreader() {
        List<WeightedFitnessCalculator> calculators = new ArrayList<>();
        calculators.add(new WeightedFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker, 0.1));
        calculators.add(new WeightedFitnessCalculator(new JobTypeFitnessCalculator(), 0.1));
        calculators.add(new WeightedFitnessCalculator(new ImageSpreadingFitnessCalculator(), 0.2));
        calculators.add(new WeightedFitnessCalculator(new SecurityGroupSpreadingFitnessCalculator(), 0.6));
        return new WeightedAverageFitnessCalculator(calculators);
    }

    private VMTaskFitnessCalculator criticalServiceJobBinPacker(AgentResourceCache agentResourceCache) {
        List<WeightedFitnessCalculator> calculators = new ArrayList<>();
        calculators.add(new WeightedFitnessCalculator(new JobTypeFitnessCalculator(), 0.1));
        calculators.add(new WeightedFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker, 0.2));
        calculators.add(new WeightedFitnessCalculator(new CachedImageFitnessCalculator(agentResourceCache), 0.3));
        calculators.add(new WeightedFitnessCalculator(new CachedSecurityGroupFitnessCalculator(agentResourceCache), 0.4));
        return new WeightedAverageFitnessCalculator(calculators);
    }

    private VMTaskFitnessCalculator defaultFitnessCalculator(AgentResourceCache agentResourceCache) {
        List<WeightedFitnessCalculator> calculators = new ArrayList<>();
        calculators.add(new WeightedFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker, 0.2));
        calculators.add(new WeightedFitnessCalculator(new JobTypeFitnessCalculator(), 0.2));
        calculators.add(new WeightedFitnessCalculator(new CachedImageFitnessCalculator(agentResourceCache), 0.3));
        calculators.add(new WeightedFitnessCalculator(new CachedSecurityGroupFitnessCalculator(agentResourceCache), 0.3));
        return new WeightedAverageFitnessCalculator(calculators);
    }
}
