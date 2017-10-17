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

package io.netflix.titus.master.jobmanager.service;

import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;

/**
 * Collection of common functions.
 */
public final class JobManagerUtil {

    private JobManagerUtil() {
    }

    public static Pair<Tier, String> getTierAssignment(Job<?> job, ApplicationSlaManagementService capacityGroupService) {
        String capacityGroup = job.getJobDescriptor().getCapacityGroup();

        ApplicationSLA applicationSLA = capacityGroupService.getApplicationSLA(capacityGroup);
        if (applicationSLA == null) {
            capacityGroup = ApplicationSlaManagementService.DEFAULT_APPLICATION;
            applicationSLA = capacityGroupService.getApplicationSLA(capacityGroup);
        }

        return Pair.of(applicationSLA.getTier(), capacityGroup);
    }

    public static Function<Task, Task> newTaskStateUpdater(TaskStatus taskStatus) {
        return oldTask -> {
            // De-duplicate task status updates. For example 'Launched' state is reported from two places, so we got
            // 'Launched' state update twice.
            if (oldTask.getStatus().getState() == taskStatus.getState()) {
                return oldTask;
            }

            return JobFunctions.updateTaskStatus(oldTask, taskStatus);
        };
    }
}
