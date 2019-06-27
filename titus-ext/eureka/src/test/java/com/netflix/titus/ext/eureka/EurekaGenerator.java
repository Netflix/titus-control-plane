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

package com.netflix.titus.ext.eureka;

import com.google.common.base.Preconditions;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;

/**
 * Generator of Eureka data.
 */
public class EurekaGenerator {

    public static InstanceInfo newInstanceInfo(String instanceId, String vipAddress, String ipAddress, InstanceInfo.InstanceStatus status) {
        return InstanceInfo.Builder.newBuilder()
                .setInstanceId(instanceId)
                .setAppName("testApp")
                .setVIPAddress(vipAddress)
                .setIPAddr(ipAddress)
                .setStatus(status)
                .build();
    }

    public static InstanceInfo newTaskInstanceInfo(Job<?> job, Task task) {
        return newTaskInstanceInfo(job, task, InstanceStatus.UP);
    }

    public static InstanceInfo newTaskInstanceInfo(Job<?> job, Task task, InstanceStatus instanceStatus) {
        Preconditions.checkArgument(job.getId().equals(task.getJobId()), "Task belongs to another job");
        Preconditions.checkArgument(task.getStatus().getState() != TaskState.Accepted, "Task not started");

        return InstanceInfo.Builder.newBuilder()
                .setInstanceId(task.getId())
                .setAppName(getOrDefault(job.getJobDescriptor().getApplicationName(), "NO_NAME"))
                .setStatus(instanceStatus)
                .build();
    }
}
