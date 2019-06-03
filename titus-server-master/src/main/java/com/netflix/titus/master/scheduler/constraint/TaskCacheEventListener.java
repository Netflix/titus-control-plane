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

import java.util.Map;

import com.netflix.fenzo.SchedulingEventListener;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID;

public class TaskCacheEventListener implements SchedulingEventListener {

    private final TaskCache taskCache;

    public TaskCacheEventListener(TaskCache taskCache) {
        this.taskCache = taskCache;
    }

    @Override
    public void onScheduleStart() {
    }

    @Override
    public void onAssignment(TaskAssignmentResult taskAssignmentResult) {
        Map<String, String> taskContext = ((V3QueueableTask)taskAssignmentResult.getRequest()).getTask().getTaskContext();
        if (taskContext.containsKey(TASK_ATTRIBUTES_IP_ALLOCATION_ID)) {
            taskCache.addTaskIpAllocation(taskAssignmentResult.getTaskId(), taskContext.get(TASK_ATTRIBUTES_IP_ALLOCATION_ID));
        }
    }

    @Override
    public void onScheduleFinish() {
    }
}
