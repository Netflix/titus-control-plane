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
