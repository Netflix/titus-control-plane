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

import java.util.Map;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerUtils;

public class V3ZoneBalancedFitnessCalculator implements VMTaskFitnessCalculator {

    public static final String NAME = "V3ZoneBalancedFitnessCalculator";

    private static final double NOT_MATCHING = 0.01;
    private static final double MATCHING = 1.0;

    private final TaskCache taskCache;
    private final int expectedValues;
    private final String zoneAttributeName;

    public V3ZoneBalancedFitnessCalculator(TaskCache taskCache, int expectedValues, String zoneAttributeName) {
        this.taskCache = taskCache;
        this.expectedValues = expectedValues;
        this.zoneAttributeName = zoneAttributeName;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        // Ignore the constraint for non-V3 tasks.
        if (!(taskRequest instanceof V3QueueableTask)) {
            return MATCHING;
        }

        String targetZoneId = SchedulerUtils.getAttributeValueOrEmptyString(targetVM, zoneAttributeName);
        if (targetZoneId.isEmpty()) {
            return NOT_MATCHING;
        }

        V3QueueableTask v3FenzoTask = (V3QueueableTask) taskRequest;
        Map<String, Integer> tasksByZoneId = SchedulerUtils.groupCurrentlyAssignedTasksByZoneId(v3FenzoTask.getJob().getId(), taskTrackerState.getAllCurrentlyAssignedTasks().values(), zoneAttributeName);
        Map<String, Integer> runningTasksByZoneId = taskCache.getTasksByZoneIdCounters(v3FenzoTask.getJob().getId());
        for (Map.Entry<String, Integer> entry : runningTasksByZoneId.entrySet()) {
            tasksByZoneId.put(entry.getKey(), tasksByZoneId.getOrDefault(entry.getKey(), 0) + entry.getValue());
        }

        int taskZoneCounter = tasksByZoneId.getOrDefault(targetZoneId, 0);
        if (taskZoneCounter == 0 || tasksByZoneId.isEmpty()) {
            return MATCHING;
        }

        double sum = 0.0;
        for (int value : tasksByZoneId.values()) {
            sum += value;
        }
        double avg = Math.ceil((sum + 1) / Math.max(expectedValues, tasksByZoneId.size()));
        if (taskZoneCounter < avg) {
            return (avg - (double) taskZoneCounter) / avg;
        }
        return NOT_MATCHING;
    }
}
