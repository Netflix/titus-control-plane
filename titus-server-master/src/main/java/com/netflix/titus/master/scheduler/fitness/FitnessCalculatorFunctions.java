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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Strings;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheFunctions;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.ScheduledRequest;
import org.apache.mesos.Protos;

import static com.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheFunctions.EMPTY_JOINED_SECURITY_GROUP_IDS;
import static com.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheFunctions.SECURITY_GROUP_ID_DELIMITER;

public class FitnessCalculatorFunctions {

    public static boolean isBatchJob(TaskRequest taskRequest) {
        if (taskRequest instanceof ScheduledRequest) {
            ScheduledRequest scheduledRequest = (ScheduledRequest) taskRequest;
            V2JobMetadata job = scheduledRequest.getJob();
            return Parameters.getJobType(job.getParameters()) == Parameters.JobType.Batch;

        } else if (taskRequest instanceof V3QueueableTask) {
            V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
            Job job = v3QueueableTask.getJob();
            return JobFunctions.isBatchJob(job);
        }
        return false;
    }

    public static boolean isServiceJob(TaskRequest taskRequest) {
        if (taskRequest instanceof ScheduledRequest) {
            ScheduledRequest scheduledRequest = (ScheduledRequest) taskRequest;
            V2JobMetadata job = scheduledRequest.getJob();
            return Parameters.getJobType(job.getParameters()) == Parameters.JobType.Service;

        } else if (taskRequest instanceof V3QueueableTask) {
            V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
            Job job = v3QueueableTask.getJob();
            return JobFunctions.isServiceJob(job);
        }
        return false;
    }

    public static boolean isFlexTier(TaskRequest taskRequest) {
        return taskRequest instanceof QueuableTask
                && ((QueuableTask) taskRequest).getQAttributes().getTierNumber() == 1;
    }

    public static boolean isCriticalTier(TaskRequest taskRequest) {
        return taskRequest instanceof QueuableTask
                && ((QueuableTask) taskRequest).getQAttributes().getTierNumber() == 0;
    }

    public static boolean isTaskLaunching(TaskRequest request) {
        if (request instanceof ScheduledRequest) {
            V2WorkerMetadata task = ((ScheduledRequest) request).getTask();
            V2JobState state = task.getState();
            return state == V2JobState.Accepted || state == V2JobState.Launched || state == V2JobState.StartInitiated;
        } else if (request instanceof V3QueueableTask) {
            Task task = ((V3QueueableTask) request).getTask();
            TaskState state = task.getStatus().getState();
            return state == TaskState.Accepted || state == TaskState.Launched || state == TaskState.StartInitiated;
        }
        return false;
    }

    public static String getJoinedSecurityGroupIds(TaskRequest taskRequest) {
        if (taskRequest instanceof ScheduledRequest) {
            ScheduledRequest scheduledRequest = (ScheduledRequest) taskRequest;
            V2JobMetadata job = scheduledRequest.getJob();
            List<Parameter> parameters = job.getParameters();
            Set<String> securityGroupIds = new HashSet<>(Parameters.getSecurityGroups(parameters));
            return StringExt.concatenate(securityGroupIds, AgentResourceCacheFunctions.SECURITY_GROUP_ID_DELIMITER);
        } else if (taskRequest instanceof V3QueueableTask) {
            V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
            Job job = v3QueueableTask.getJob();
            Container container = job.getJobDescriptor().getContainer();
            Set<String> securityGroupIds = new HashSet<>(container.getSecurityProfile().getSecurityGroups());
            return StringExt.concatenate(securityGroupIds, AgentResourceCacheFunctions.SECURITY_GROUP_ID_DELIMITER);
        }
        return AgentResourceCacheFunctions.EMPTY_JOINED_SECURITY_GROUP_IDS;
    }

    public static List<TaskRequest> getAllTasksOnAgent(VirtualMachineCurrentState targetVm) {
        List<TaskRequest> tasksOnAgent = new ArrayList<>(targetVm.getRunningTasks());
        targetVm.getTasksCurrentlyAssigned().forEach(t -> tasksOnAgent.add(t.getRequest()));
        return tasksOnAgent;
    }

    public static long countMatchingTasks(List<TaskRequest> tasksOnAgent, Predicate<TaskRequest> predicate) {
        return tasksOnAgent.stream().filter(predicate).count();
    }

    public static String getAgentAttributeValue(VirtualMachineCurrentState targetVM, String attributeName) {
        Protos.Attribute attribute = targetVM.getCurrAvailableResources().getAttributeMap().get(attributeName);
        return Strings.nullToEmpty(attribute.getText().getValue());
    }
}
