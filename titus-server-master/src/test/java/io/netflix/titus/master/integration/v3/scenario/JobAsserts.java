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

package io.netflix.titus.master.integration.v3.scenario;

import java.util.List;
import java.util.function.Predicate;

import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;

/**
 */
public class JobAsserts {
    public static Predicate<Job> jobInState(JobState expectedState) {
        return job -> job.getStatus().getState() == expectedState;
    }

    public static Predicate<Task> taskInState(TaskState expectedState) {
        return task -> task.getStatus().getState() == expectedState;
    }

    public static Predicate<TaskExecutorHolder> containerWithResources(ContainerResources containerResources) {
        return taskExecutorHolder -> {
            if (taskExecutorHolder.getTaskCPUs() != containerResources.getCpu()) {
                return false;
            }
            if (taskExecutorHolder.getTaskMem() != containerResources.getMemoryMB()) {
                return false;
            }
            if (taskExecutorHolder.getTaskDisk() != containerResources.getDiskMB()) {
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
