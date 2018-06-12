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

package com.netflix.titus.master.integration.v3.scenario;

import java.util.List;
import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;

/**
 */
public class JobAsserts {
    public static Predicate<Job> jobInState(JobState expectedState) {
        return job -> job.getStatus().getState() == expectedState;
    }

    public static Predicate<Task> taskInState(TaskState expectedState) {
        return task -> task.getStatus().getState() == expectedState;
    }

    public static Predicate<TaskExecutorHolder> containerWithResources(ContainerResources containerResources, int diskMbMin) {
        return taskExecutorHolder -> {
            if (taskExecutorHolder.getTaskCPUs() != containerResources.getCpu()) {
                return false;
            }
            if (taskExecutorHolder.getTaskMem() != containerResources.getMemoryMB()) {
                return false;
            }
            int diskMB = Math.max(containerResources.getDiskMB(), diskMbMin);
            if (taskExecutorHolder.getTaskDisk() != diskMB) {
                return false;
            }
            if (taskExecutorHolder.getTaskNetworkMbs() != containerResources.getNetworkMbps()) {
                return false;
            }
            return true;
        };
    }

    public static Predicate<TaskExecutorHolder> containerWithEfsMounts(List<EfsMount> expectedEfsMounts) {
        return taskExecutorHolder -> {
            List<EfsMount> actualEfsMounts = taskExecutorHolder.getEfsMounts();
            return expectedEfsMounts.equals(actualEfsMounts);
        };
    }
}
