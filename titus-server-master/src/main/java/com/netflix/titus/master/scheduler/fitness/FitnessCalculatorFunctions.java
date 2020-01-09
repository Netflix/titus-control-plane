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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.base.Strings;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import org.apache.mesos.Protos;

import static org.apache.commons.collections.CollectionUtils.subtract;

public class FitnessCalculatorFunctions {

    public static boolean isBatchJob(TaskRequest taskRequest) {
        Job job = getJob(taskRequest);
        return JobFunctions.isBatchJob(job);
    }

    public static boolean isServiceJob(TaskRequest taskRequest) {
        Job job = getJob(taskRequest);
        return JobFunctions.isServiceJob(job);
    }

    public static boolean isFlexTier(TaskRequest taskRequest) {
        return ((QueuableTask) taskRequest).getQAttributes().getTierNumber() == 1;
    }

    public static boolean isCriticalTier(TaskRequest taskRequest) {
        return ((QueuableTask) taskRequest).getQAttributes().getTierNumber() == 0;
    }

    public static boolean isTaskLaunching(TaskRequest request) {
        Task task = ((V3QueueableTask) request).getTask();
        TaskState state = task.getStatus().getState();
        return state == TaskState.Accepted || state == TaskState.Launched || state == TaskState.StartInitiated;
    }

    public static List<String> getSecurityGroups(TaskRequest taskRequest) {
        Job<?> job = getJob(taskRequest);
        Container container = job.getJobDescriptor().getContainer();
        return container.getSecurityProfile().getSecurityGroups();
    }

    public static boolean areSecurityGroupsEqual(Collection<String> first, Collection<String> second) {
        return subtract(first, second).size() == 0;
    }

    public static Job<?> getJob(TaskRequest taskRequest) {
        V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
        return v3QueueableTask.getJob();
    }

    public static List<TaskRequest> getAllTasksOnAgent(VirtualMachineCurrentState targetVm) {
        List<TaskRequest> tasksOnAgent = new ArrayList<>(targetVm.getRunningTasks());
        for (TaskAssignmentResult taskAssignmentResult : targetVm.getTasksCurrentlyAssigned()) {
            tasksOnAgent.add(taskAssignmentResult.getRequest());
        }
        return tasksOnAgent;
    }

    public static long countMatchingTasks(List<TaskRequest> tasksOnAgent, Predicate<TaskRequest> predicate) {
        int count = 0;
        for (TaskRequest taskRequest : tasksOnAgent) {
            if (predicate.test(taskRequest)) {
                count++;
            }
        }
        return count;
    }

    public static String getAgentAttributeValue(VirtualMachineCurrentState targetVM, String attributeName) {
        Protos.Attribute attribute = targetVM.getCurrAvailableResources().getAttributeMap().get(attributeName);
        return Strings.nullToEmpty(attribute.getText().getValue());
    }
}
