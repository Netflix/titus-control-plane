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

public class AgentFitnessCalculator implements VMTaskFitnessCalculator {
    private static final double BIN_PACKER_WEIGHT = 0.1;
    private static final double JOB_TYPE_WEIGHT = 0.2;
    private static final double RECENT_TASK_LAUNCH_WEIGHT = 0.7;

    private final WeightedAverageFitnessCalculator weightedAverageFitnessCalculator;

    public static final com.netflix.fenzo.functions.Func1<Double, Boolean> fitnessGoodEnoughFunc =
            f -> f > 1.0;

    public AgentFitnessCalculator() {
        List<WeightedFitnessCalculator> calculators = new ArrayList<>();
        calculators.add(new WeightedFitnessCalculator(BinPackingFitnessCalculators.cpuMemBinPacker, BIN_PACKER_WEIGHT));
        calculators.add(new WeightedFitnessCalculator(new JobTypeFitnessCalculator(), JOB_TYPE_WEIGHT));
        calculators.add(new WeightedFitnessCalculator(new TaskLaunchingFitnessCalculator(), RECENT_TASK_LAUNCH_WEIGHT));
        this.weightedAverageFitnessCalculator = new WeightedAverageFitnessCalculator(calculators);
    }

    @Override
    public String getName() {
        return "Titus Agent Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        return weightedAverageFitnessCalculator.calculateFitness(taskRequest, targetVM, taskTrackerState);
    }
}
