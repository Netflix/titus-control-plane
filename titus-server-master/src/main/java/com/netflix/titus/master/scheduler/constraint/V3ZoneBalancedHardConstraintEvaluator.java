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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerUtils;
import com.netflix.titus.master.scheduler.resourcecache.TaskCache;

public class V3ZoneBalancedHardConstraintEvaluator implements ConstraintEvaluator {

    public static final String NAME = "V3ZoneBalancedHardConstraintEvaluator";

    private static final Result VALID = new Result(true, null);
    private static final Result INVALID = new Result(false, "Zone balancing constraints not met");
    private static final Result NO_ZONE_ID = new Result(false, "Host without zone data");

    private final int expectedValues;
    private final String zoneAttributeName;
    private final TaskCache taskCache;

    protected V3ZoneBalancedHardConstraintEvaluator(TaskCache taskCache, int expectedValues, String zoneAttributeName) {
        this.taskCache = taskCache;
        this.expectedValues = expectedValues;
        this.zoneAttributeName = zoneAttributeName;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        // Ignore the constraint for non-V3 tasks.
        if (!(taskRequest instanceof V3QueueableTask)) {
            return VALID;
        }

        String targetZoneId = SchedulerUtils.getAttributeValueOrEmptyString(targetVM, zoneAttributeName);
        if (targetZoneId.isEmpty()) {
            return NO_ZONE_ID;
        }

        V3QueueableTask v3FenzoTask = (V3QueueableTask) taskRequest;
        return evaluate(targetZoneId, v3FenzoTask.getJob().getId(), taskTrackerState);
    }

    protected Result evaluate(String targetZoneId, String jobId, TaskTrackerState taskTrackerState) {
        Map<String, Integer> tasksByZoneId = SchedulerUtils.groupCurrentlyAssignedTasksByZoneId(jobId, taskTrackerState.getAllCurrentlyAssignedTasks().values(), zoneAttributeName);
        Map<String, Integer> runningTasksByZoneId = taskCache.getTasksByZoneIdCounters(jobId);
        for (Map.Entry<String, Integer> entry : runningTasksByZoneId.entrySet()) {
            tasksByZoneId.put(entry.getKey(), tasksByZoneId.getOrDefault(entry.getKey(), 0) + entry.getValue());
        }

        int taskZoneCounter = tasksByZoneId.getOrDefault(targetZoneId, 0);
        if (taskZoneCounter == 0) {
            return VALID;
        }

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i : tasksByZoneId.values()) {
            min = Math.min(min, i);
            max = Math.max(max, i);
        }
        min = expectedValues > tasksByZoneId.size() ? 0 : min;
        if (min == max || taskZoneCounter < max) {
            return VALID;
        }
        return INVALID;
    }
}
