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
import java.util.Optional;

import com.netflix.fenzo.SchedulingEventListener;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.resourcecache.OpportunisticCpuAllocation;
import com.netflix.titus.master.scheduler.resourcecache.OpportunisticCpuCache;
import com.netflix.titus.master.scheduler.resourcecache.TaskCache;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID;

public class TaskCacheEventListener implements SchedulingEventListener {

    public static final String UNKNOWN_ALLOCATION_ID = "UNKNOWN_ALLOCATION_ID";

    private final TaskCache taskCache;
    private final OpportunisticCpuCache opportunisticCpuCache;
    private final TitusRuntime titusRuntime;

    public TaskCacheEventListener(TaskCache taskCache, OpportunisticCpuCache opportunisticCpuCache, TitusRuntime titusRuntime) {
        this.taskCache = taskCache;
        this.opportunisticCpuCache = opportunisticCpuCache;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public void onScheduleStart() {
        taskCache.prepare();
        opportunisticCpuCache.prepare();
    }

    @Override
    public void onAssignment(TaskAssignmentResult taskAssignmentResult) {
        V3QueueableTask request = (V3QueueableTask) taskAssignmentResult.getRequest();
        Map<String, String> taskContext = request.getTask().getTaskContext();
        if (taskContext.containsKey(TASK_ATTRIBUTES_IP_ALLOCATION_ID)) {
            taskCache.addTaskIpAllocation(taskAssignmentResult.getTaskId(), taskContext.get(TASK_ATTRIBUTES_IP_ALLOCATION_ID));
        }

        int opportunisticCpus = request.getOpportunisticCpus();
        if (opportunisticCpus > 0) {
            String agentId = taskAssignmentResult.getVMId();
            if (agentId == null) {
                String hostname = taskAssignmentResult.getHostname();
                codeInvariants().inconsistent("No machine ID for hostname %s", hostname);
                agentId = hostname;
            }
            Optional<String> allocationId = opportunisticCpuCache.findOpportunisticCpuAllocationId(agentId);
            if (!allocationId.isPresent()) {
                codeInvariants().inconsistent("Task assigned to opportunistic CPUs on machine %s that can not be found", agentId);
            }
            taskCache.addOpportunisticCpuAllocation(new OpportunisticCpuAllocation(
                    taskAssignmentResult.getTaskId(),
                    agentId,
                    allocationId.orElse(UNKNOWN_ALLOCATION_ID),
                    opportunisticCpus
            ));
        }
    }

    @Override
    public void onScheduleFinish() {
    }

    private CodeInvariants codeInvariants() {
        return titusRuntime.getCodeInvariants();
    }

}
