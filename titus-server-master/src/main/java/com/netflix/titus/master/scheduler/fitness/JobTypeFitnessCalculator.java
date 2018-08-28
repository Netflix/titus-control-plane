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

package com.netflix.titus.master.scheduler.fitness;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;

/**
 * A fitness calculator that will prefer task placement on agents with the same job type.
 */
public class JobTypeFitnessCalculator implements VMTaskFitnessCalculator {

    public static final String NAME = "JobTypeFitnessCalculator";

    private static final double EMPTY_HOST_SCORE = 0.7;
    private static final double ZERO_SAME_JOB_TASKS_SCORE = 0.01;

    private enum JobType {Batch, Service}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        int totalTasks = 0;
        int totalSameJobTasks = 0;

        for (TaskRequest request : targetVM.getRunningTasks()) {
            totalTasks++;
            if (isSameJobType(taskRequest, request)) {
                totalSameJobTasks++;
            }
        }
        for (TaskAssignmentResult result : targetVM.getTasksCurrentlyAssigned()) {
            totalTasks++;
            if (isSameJobType(taskRequest, result.getRequest())) {
                totalSameJobTasks++;
            }
        }
        if (totalTasks == 0) {
            return EMPTY_HOST_SCORE;
        } else if (totalSameJobTasks == 0) {
            return ZERO_SAME_JOB_TASKS_SCORE;
        }
        return (double) totalSameJobTasks / (double) totalTasks;
    }

    private boolean isSameJobType(TaskRequest first, TaskRequest second) {
        return getJobType(first) == getJobType(second);
    }

    private JobType getJobType(TaskRequest taskRequest) {
        JobDescriptor.JobDescriptorExt jobDescriptorExt = ((V3QueueableTask) taskRequest).getJob().getJobDescriptor().getExtensions();
        if (jobDescriptorExt instanceof BatchJobExt) {
            return JobType.Batch;
        } else if (jobDescriptorExt instanceof ServiceJobExt) {
            return JobType.Service;
        }
        return JobType.Batch;
    }
}
