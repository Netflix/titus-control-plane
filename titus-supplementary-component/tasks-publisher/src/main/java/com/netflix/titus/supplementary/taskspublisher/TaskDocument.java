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

package com.netflix.titus.supplementary.taskspublisher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.ExecutableStatus;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.StringExt;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_ASG;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_ITYPE;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_REGION;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TIER;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_SCALED_DOWN;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;

public class TaskDocument {
    private String id;
    private String instanceId;
    private String jobId;
    private String host;
    private String hostInstanceId;
    private String zone;
    private String asg;
    private String instanceType;
    private String region;
    private String message;
    private String submittedAt;
    private String launchedAt;
    private String startingAt;
    private String startedAt;
    private String finishedAt;
    private String state;
    private ComputedFields computedFields;
    private Map<String, String> titusContext;

    /* Job Spec Fields */
    private String name;
    private String applicationName;
    private String appName;
    private String user;
    private TitusJobType type;
    private Map<String, String> labels;
    private String version;
    private String digest;
    private String entryPoint;
    private Boolean inService;
    private int instances;
    private int instancesMin;
    private int instancesMax;
    private int instancesDesired;
    private double cpu;
    private double memory;
    private double networkMbps;
    private double disk;
    private int gpu;
    private Map<String, String> env;
    private int retries;
    private boolean restartOnSuccess;
    private Long runtimeLimitSecs;
    private boolean allocateIpAddress;
    private String iamProfile;
    private List<String> securityGroups;
    private List<String> softConstraints;
    private List<String> hardConstraints;
    private String jobGroupStack;
    private String jobGroupDetail;
    private String jobGroupSequence;
    private String capacityGroup;
    private String tier;

    // Network configuration
    private String containerIp;
    private String networkInterfaceId;
    private String networkInterfaceIndex;

    public String getName() {
        return name;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getAppName() {
        return appName;
    }

    public String getUser() {
        return user;
    }

    public TitusJobType getType() {
        return type;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public String getVersion() {
        return version;
    }

    public String getDigest() {
        return digest;
    }

    public String getEntryPoint() {
        return entryPoint;
    }

    public Boolean getInService() {
        return inService;
    }

    public int getInstances() {
        return instances;
    }

    public int getInstancesMin() {
        return instancesMin;
    }

    public int getInstancesMax() {
        return instancesMax;
    }

    public int getInstancesDesired() {
        return instancesDesired;
    }

    public double getCpu() {
        return cpu;
    }

    public double getMemory() {
        return memory;
    }

    public double getNetworkMbps() {
        return networkMbps;
    }

    public double getDisk() {
        return disk;
    }

    public int getGpu() {
        return gpu;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public int getRetries() {
        return retries;
    }

    public boolean isRestartOnSuccess() {
        return restartOnSuccess;
    }

    public Long getRuntimeLimitSecs() {
        return runtimeLimitSecs;
    }

    public boolean isAllocateIpAddress() {
        return allocateIpAddress;
    }

    public String getIamProfile() {
        return iamProfile;
    }

    public List<String> getSecurityGroups() {
        return securityGroups;
    }

    public List<String> getSoftConstraints() {
        return softConstraints;
    }

    public List<String> getHardConstraints() {
        return hardConstraints;
    }

    public String getJobGroupStack() {
        return jobGroupStack;
    }

    public String getJobGroupDetail() {
        return jobGroupDetail;
    }

    public String getJobGroupSequence() {
        return jobGroupSequence;
    }

    public String getCapacityGroup() {
        return capacityGroup;
    }

    public String getSubmittedAt() {
        return submittedAt;
    }

    public String getLaunchedAt() {
        return launchedAt;
    }

    public String getStartingAt() {
        return startingAt;
    }

    public String getStartedAt() {
        return startedAt;
    }

    public String getFinishedAt() {
        return finishedAt;
    }

    public String getMessage() {
        return message;
    }

    public String getId() {
        return id;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getState() {
        return state;
    }

    public String getHost() {
        return host;
    }

    public String getZone() {
        return zone;
    }

    public String getRegion() {
        return region;
    }

    public ComputedFields getComputedFields() {
        return computedFields;
    }

    public Map<String, String> getTitusContext() {
        return titusContext;
    }

    public String getAsg() {
        return asg;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public String getHostInstanceId() {
        return hostInstanceId;
    }

    public String getContainerIp() {
        return containerIp;
    }

    public String getNetworkInterfaceId() {
        return networkInterfaceId;
    }

    public String getNetworkInterfaceIndex() {
        return networkInterfaceIndex;
    }

    public String getTier() {
        return tier;
    }

    public static class ComputedFields {
        Long msFromSubmittedToLaunched;
        Long msFromLaunchedToStarting;
        Long msToStarting;
        Long msFromStartingToStarted;
        Long msToStarted;
        Long msFromStartedToFinished;
        Long msToFinished;

        public Long getMsFromSubmittedToLaunched() {
            return msFromSubmittedToLaunched;
        }

        public Long getMsFromLaunchedToStarting() {
            return msFromLaunchedToStarting;
        }

        public Long getMsToStarting() {
            return msToStarting;
        }

        public Long getMsFromStartingToStarted() {
            return msFromStartingToStarted;
        }

        public Long getMsToStarted() {
            return msToStarted;
        }

        public Long getMsFromStartedToFinished() {
            return msFromStartedToFinished;
        }

        public Long getMsToFinished() {
            return msToFinished;
        }
    }

    public static TaskDocument fromV3Task(Task task, Job job, SimpleDateFormat dateFormat, Map<String, String> context) {
        TaskDocument taskDocument = new TaskDocument();
        JobDescriptor jobDescriptor = job.getJobDescriptor();
        Container container = jobDescriptor.getContainer();
        Image image = container.getImage();
        ContainerResources containerResources = container.getContainerResources();
        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();

        taskDocument.name = jobDescriptor.getApplicationName();
        taskDocument.applicationName = image.getName();
        taskDocument.appName = jobDescriptor.getApplicationName();
        taskDocument.user = jobDescriptor.getOwner().getTeamEmail();
        taskDocument.labels = container.getAttributes();
        taskDocument.version = image.getTag();
        taskDocument.digest = image.getDigest();
        taskDocument.entryPoint = StringExt.concatenate(container.getEntryPoint(), " ");
        taskDocument.cpu = containerResources.getCpu();
        taskDocument.memory = containerResources.getMemoryMB();
        taskDocument.networkMbps = containerResources.getNetworkMbps();
        taskDocument.disk = containerResources.getDiskMB();
        taskDocument.gpu = containerResources.getGpu();
        taskDocument.allocateIpAddress = containerResources.isAllocateIP();
        taskDocument.env = container.getEnv();
        taskDocument.iamProfile = container.getSecurityProfile().getIamRole();
        taskDocument.securityGroups = container.getSecurityProfile().getSecurityGroups();
        taskDocument.softConstraints = new ArrayList<>(container.getSoftConstraints().keySet());
        taskDocument.hardConstraints = new ArrayList<>(container.getHardConstraints().keySet());
        taskDocument.capacityGroup = jobDescriptor.getCapacityGroup();
        taskDocument.jobGroupStack = jobGroupInfo.getStack();
        taskDocument.jobGroupDetail = jobGroupInfo.getDetail();
        taskDocument.jobGroupSequence = jobGroupInfo.getSequence();

        JobDescriptor.JobDescriptorExt jobDescriptorExt = jobDescriptor.getExtensions();
        if (jobDescriptorExt instanceof BatchJobExt) {
            BatchJobExt batchJobExt = (BatchJobExt) jobDescriptorExt;
            taskDocument.runtimeLimitSecs = batchJobExt.getRuntimeLimitMs();
            taskDocument.type = TitusJobType.batch;
            taskDocument.inService = false;
            taskDocument.instances = batchJobExt.getSize();
            taskDocument.instancesMin = batchJobExt.getSize();
            taskDocument.instancesMax = batchJobExt.getSize();
            taskDocument.instancesDesired = batchJobExt.getSize();
            taskDocument.retries = batchJobExt.getRetryPolicy().getRetries();
            taskDocument.restartOnSuccess = false;
        } else if (jobDescriptorExt instanceof ServiceJobExt) {
            ServiceJobExt serviceJobExt = (ServiceJobExt) jobDescriptorExt;
            taskDocument.runtimeLimitSecs = 0L;
            taskDocument.type = TitusJobType.service;
            taskDocument.inService = serviceJobExt.isEnabled();
            Capacity capacity = serviceJobExt.getCapacity();
            taskDocument.instances = capacity.getDesired();
            taskDocument.instancesMin = capacity.getMin();
            taskDocument.instancesMax = capacity.getMax();
            taskDocument.instancesDesired = capacity.getDesired();
            taskDocument.retries = serviceJobExt.getRetryPolicy().getRetries();
            taskDocument.restartOnSuccess = false;
        }

        Map<String, String> taskContext = task.getTaskContext();
        taskDocument.id = task.getId();
        taskDocument.instanceId = task.getId();
        taskDocument.jobId = task.getJobId();
        taskDocument.state = toV2TaskState(task.getStatus()).name();
        taskDocument.host = taskContext.get(TASK_ATTRIBUTES_AGENT_HOST);
        taskDocument.tier = taskContext.getOrDefault(TASK_ATTRIBUTES_TIER, "Unknown");
        taskDocument.computedFields = new ComputedFields();

        final String region = taskContext.get(TASK_ATTRIBUTES_AGENT_REGION);
        if (region != null) {
            taskDocument.region = region;
        }
        final String zone = taskContext.get(TASK_ATTRIBUTES_AGENT_ZONE);
        if (zone != null) {
            taskDocument.zone = zone;
        }

        final String asg = taskContext.get(TASK_ATTRIBUTES_AGENT_ASG);
        if (asg != null) {
            taskDocument.asg = asg;
        }

        final String instanceType = taskContext.get(TASK_ATTRIBUTES_AGENT_ITYPE);
        if (instanceType != null) {
            taskDocument.instanceType = instanceType;
        }

        final String instanceId = taskContext.get(TASK_ATTRIBUTES_AGENT_ID);
        if (instanceId != null) {
            taskDocument.hostInstanceId = instanceId;
        }

        extractNetworkConfigurationData(taskContext, taskDocument);

        long acceptedAt = findTaskStatus(task, TaskState.Accepted).map(ExecutableStatus::getTimestamp).orElse(0L);
        long launchedAt = findTaskStatus(task, TaskState.Launched).map(ExecutableStatus::getTimestamp).orElse(0L);
        long startingAt = findTaskStatus(task, TaskState.StartInitiated).map(ExecutableStatus::getTimestamp).orElse(0L);
        long startedAt = findTaskStatus(task, TaskState.Started).map(ExecutableStatus::getTimestamp).orElse(0L);
        long completedAt = findTaskStatus(task, TaskState.Finished).map(ExecutableStatus::getTimestamp).orElse(0L);

        if (acceptedAt > 0) {
            taskDocument.submittedAt = dateFormat.format(new Date(acceptedAt));
        }

        if (launchedAt > 0) {
            taskDocument.launchedAt = dateFormat.format(new Date(launchedAt));
            taskDocument.computedFields.msFromSubmittedToLaunched = launchedAt - acceptedAt;
        }

        if (startingAt > 0) {
            taskDocument.startingAt = dateFormat.format(new Date(startingAt));
            taskDocument.computedFields.msFromLaunchedToStarting = startingAt - launchedAt;
            taskDocument.computedFields.msToStarting = startingAt - acceptedAt;
        }

        if (startedAt > 0) {
            taskDocument.startedAt = dateFormat.format(new Date(startedAt));
            taskDocument.computedFields.msFromStartingToStarted = startedAt - startingAt;
            taskDocument.computedFields.msToStarted = startedAt - acceptedAt;
        }

        if (completedAt > 0) {
            taskDocument.finishedAt = dateFormat.format(new Date(completedAt));
            taskDocument.computedFields.msFromStartedToFinished = completedAt - startedAt;
            taskDocument.computedFields.msToFinished = completedAt - acceptedAt;
        }

        taskDocument.message = task.getStatus().getReasonMessage();
        taskDocument.titusContext = context;

        return taskDocument;
    }

    private static Optional<TaskStatus> findTaskStatus(Task task, TaskState taskState) {
        if (task.getStatus().getState() == taskState) {
            return Optional.of(task.getStatus());
        } else {
            return task.getStatusHistory().stream().filter(taskStatus -> taskStatus.getState() == taskState).findFirst();
        }
    }

    private static TitusTaskState toV2TaskState(TaskStatus taskStatus) {
        switch (taskStatus.getState()) {
            case Accepted:
                return TitusTaskState.QUEUED;
            case Launched:
                return TitusTaskState.DISPATCHED;
            case StartInitiated:
                return TitusTaskState.STARTING;
            case Started:
                return TitusTaskState.RUNNING;
            case KillInitiated:
                return TitusTaskState.RUNNING;
            case Finished:
                String reasonCode = taskStatus.getReasonCode();
                if (reasonCode.equalsIgnoreCase(REASON_NORMAL)) {
                    return TitusTaskState.FINISHED;
                } else if (reasonCode.equalsIgnoreCase(REASON_FAILED)) {
                    return TitusTaskState.FAILED;
                } else if (reasonCode.equalsIgnoreCase(REASON_TASK_KILLED) || reasonCode.equalsIgnoreCase(REASON_SCALED_DOWN)) {
                    return TitusTaskState.STOPPED;
                } else if (TaskStatus.isSystemError(taskStatus)) {
                    return TitusTaskState.CRASHED;
                }
                return TitusTaskState.FAILED;
            default:
                return TitusTaskState.FAILED;
        }
    }

    private static void extractNetworkConfigurationData(Map<String, String> taskContext, TaskDocument taskDocument) {
        taskDocument.networkInterfaceId = Strings.nullToEmpty(taskContext.get(TASK_ATTRIBUTES_NETWORK_INTERFACE_ID));
        taskDocument.containerIp = Strings.nullToEmpty(taskContext.get(TASK_ATTRIBUTES_CONTAINER_IP));
        taskDocument.networkInterfaceIndex = Strings.nullToEmpty(taskContext.get(TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX));
    }
}
