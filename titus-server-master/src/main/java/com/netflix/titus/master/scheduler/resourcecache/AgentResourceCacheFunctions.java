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

package com.netflix.titus.master.scheduler.resourcecache;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet.ConsumeResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.scheduler.ScheduledRequest;

public class AgentResourceCacheFunctions {
    public static final String SECURITY_GROUP_ID_DELIMITER = ":";
    public static final String EMPTY_JOINED_SECURITY_GROUP_IDS = "";
    public static final String EMPTY_IP_ADDRESS = "";

    public static AgentResourceCacheNetworkInterface createNetworkInterface(int eniIndex, Map<String, Set<String>> ipAddresses, Set<String> securityGroupIds,
                                                                            boolean hasAvailableIps, long timestamp) {
        String joinedSecurityGroupIds = StringExt.concatenate(securityGroupIds, SECURITY_GROUP_ID_DELIMITER);
        return AgentResourceCacheNetworkInterface.newBuilder()
                .withEniIndex(eniIndex)
                .withIpAddresses(ipAddresses)
                .withSecurityGroupIds(securityGroupIds)
                .withHasAvailableIps(hasAvailableIps)
                .withJoinedSecurityGroupIds(joinedSecurityGroupIds)
                .withTimestamp(timestamp)
                .build();
    }

    public static AgentResourceCacheNetworkInterface updateNetworkInterface(AgentResourceCacheNetworkInterface original, AgentResourceCacheNetworkInterface updated) {
        Preconditions.checkNotNull(original, "original cannot be null");
        Preconditions.checkNotNull(updated, "updated cannot be null");
        Preconditions.checkArgument(original.getIndex() == updated.getIndex(),
                "index does not match, %s (original) != %s (updated)", original.getIndex(), updated.getIndex());

        if (!original.getSecurityGroupIds().equals(updated.getSecurityGroupIds())) {
            //if there is a new set of security groups then the network interface is being replaced instead of updated
            return updated;
        }

        Map<String, Set<String>> newIpAddresses = CollectionsExt.merge(original.getIpAddresses(), updated.getIpAddresses(),
                (firstIpAddresses, secondIpAddresses) -> CollectionsExt.merge(firstIpAddresses, secondIpAddresses));
        Set<String> newSecurityGroupIds = updated.getSecurityGroupIds();
        boolean newHasAvailableIps = false;
        for (Set<String> taskIdsForIp : newIpAddresses.values()) {
            if (taskIdsForIp.isEmpty()) {
                newHasAvailableIps = true;
                break;
            }
        }
        long newTimestamp = Math.max(original.getTimestamp(), updated.getTimestamp());
        return createNetworkInterface(original.getIndex(), newIpAddresses, newSecurityGroupIds, newHasAvailableIps, newTimestamp);
    }

    public static Map<Integer, AgentResourceCacheNetworkInterface> updateNetworkInterface(Map<Integer, AgentResourceCacheNetworkInterface> original,
                                                                                          Map<Integer, AgentResourceCacheNetworkInterface> updated) {
        Preconditions.checkNotNull(original, "original cannot be null");
        Preconditions.checkNotNull(updated, "updated cannot be null");

        return CollectionsExt.merge(original, updated, AgentResourceCacheFunctions::updateNetworkInterface);
    }

    public static AgentResourceCacheImage createImage(String imageName, String imageDigest, String imageTag) {
        return AgentResourceCacheImage.newBuilder()
                .withImageName(imageName)
                .withDigest(imageDigest)
                .withTag(imageTag)
                .build();
    }

    public static AgentResourceCacheInstance createInstance(String hostname,
                                                            Set<AgentResourceCacheImage> images,
                                                            Map<Integer, AgentResourceCacheNetworkInterface> networkInterfaces) {
        return AgentResourceCacheInstance.newBuilder()
                .withHostname(hostname)
                .withImages(images)
                .withNetworkInterfaces(networkInterfaces)
                .build();
    }

    public static AgentResourceCacheImage getImage(TaskRequest taskRequest) {
        if (taskRequest instanceof ScheduledRequest) {
            ScheduledRequest scheduledRequest = (ScheduledRequest) taskRequest;
            V2JobMetadata job = scheduledRequest.getJob();
            return createImage(job);
        } else if (taskRequest instanceof V3QueueableTask) {
            V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
            Job job = v3QueueableTask.getJob();
            return createImage(job);
        }
        return AgentResourceCacheImage.newBuilder().build();
    }

    public static AgentResourceCacheInstance createInstance(String hostname,
                                                            TaskRequest request,
                                                            ConsumeResult consumeResult,
                                                            long timestamp) {
        Preconditions.checkNotNull(consumeResult);
        int networkInterfaceIndex = consumeResult.getIndex();

        if (request instanceof ScheduledRequest) {
            ScheduledRequest scheduledRequest = (ScheduledRequest) request;
            V2JobMetadata job = scheduledRequest.getJob();
            V2WorkerMetadata task = scheduledRequest.getTask();

            AgentResourceCacheImage image = createImage(job);
            AgentResourceCacheNetworkInterface networkInterface = createNetworkInterface(job, task, networkInterfaceIndex, timestamp);
            return createInstance(hostname, Collections.singleton(image), Collections.singletonMap(networkInterfaceIndex, networkInterface));
        } else if (request instanceof V3QueueableTask) {
            V3QueueableTask v3QueueableTask = (V3QueueableTask) request;
            Job job = v3QueueableTask.getJob();
            Task task = v3QueueableTask.getTask();

            AgentResourceCacheImage image = createImage(job);
            AgentResourceCacheNetworkInterface networkInterface = createNetworkInterface(job, task, networkInterfaceIndex, timestamp);
            return createInstance(hostname, Collections.singleton(image), Collections.singletonMap(networkInterfaceIndex, networkInterface));
        } else {
            throw new IllegalArgumentException("Unknown task type");
        }
    }

    public static AgentResourceCacheInstance createInstance(String hostname, V2JobMetadata job, V2WorkerMetadata task, long timestamp) {
        AgentResourceCacheImage image = createImage(job);
        V2WorkerMetadata.TwoLevelResource twoLevelResource = CollectionsExt.first(task.getTwoLevelResources());
        Preconditions.checkNotNull(twoLevelResource, "twoLevelResource cannot be null");
        int networkInterfaceIndex = Integer.parseInt(twoLevelResource.getLabel());
        AgentResourceCacheNetworkInterface networkInterface = createNetworkInterface(job, task, networkInterfaceIndex, timestamp);
        return createInstance(hostname, Collections.singleton(image), Collections.singletonMap(networkInterfaceIndex, networkInterface));
    }

    public static AgentResourceCacheInstance createInstance(String hostname, Job job, Task task, long timestamp) {
        AgentResourceCacheImage image = createImage(job);
        TwoLevelResource twoLevelResource = CollectionsExt.first(task.getTwoLevelResources());
        Preconditions.checkNotNull(twoLevelResource, "twoLevelResource cannot be null");
        int networkInterfaceIndex = twoLevelResource.getIndex();
        AgentResourceCacheNetworkInterface networkInterface = createNetworkInterface(job, task, networkInterfaceIndex, timestamp);
        return createInstance(hostname, Collections.singleton(image), Collections.singletonMap(networkInterfaceIndex, networkInterface));
    }

    public static AgentResourceCacheImage createImage(V2JobMetadata job) {
        List<Parameter> parameters = job.getParameters();
        String imageName = Parameters.getImageName(parameters);
        String imageDigest = Parameters.getImageDigest(parameters);
        String imageTag = Parameters.getVersion(parameters);
        return createImage(imageName, imageDigest, imageTag);
    }

    public static AgentResourceCacheNetworkInterface createNetworkInterface(V2JobMetadata job, V2WorkerMetadata task, int eniIndex, long timestamp) {
        List<Parameter> parameters = job.getParameters();
        Set<String> securityGroupIds = new LinkedHashSet<>(Parameters.getSecurityGroups(parameters));
        String ipAddress = getIpAddress(task);
        String taskId = WorkerNaming.getTaskId(task);
        return createNetworkInterface(eniIndex, Collections.singletonMap(ipAddress, Collections.singleton(taskId)),
                securityGroupIds, false, timestamp);
    }

    public static AgentResourceCacheImage createImage(Job job) {
        Container container = job.getJobDescriptor().getContainer();
        Image taskImage = container.getImage();
        return createImage(taskImage.getName(), taskImage.getDigest(), taskImage.getTag());
    }

    public static AgentResourceCacheNetworkInterface createNetworkInterface(Job job, Task task, int eniIndex, long timestamp) {
        Container container = job.getJobDescriptor().getContainer();
        Set<String> securityGroupIds = new LinkedHashSet<>(container.getSecurityProfile().getSecurityGroups());
        String ipAddress = task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, EMPTY_IP_ADDRESS);
        return createNetworkInterface(eniIndex, Collections.singletonMap(ipAddress, Collections.singleton(task.getId())),
                securityGroupIds, false, timestamp);
    }

    public static AgentResourceCacheInstance updateInstance(AgentResourceCacheInstance original, AgentResourceCacheInstance updated) {
        Preconditions.checkNotNull(original, "original cannot be null");
        Preconditions.checkNotNull(updated, "updated cannot be null");
        Preconditions.checkArgument(original.getHostname().equals(updated.getHostname()),
                "hostnames do not match, %s (original) != %s (updated)", original.getHostname(), updated.getHostname());

        Set<AgentResourceCacheImage> newImages = CollectionsExt.merge(original.getImages(), updated.getImages());
        Map<Integer, AgentResourceCacheNetworkInterface> newEnis = updateNetworkInterface(original.getNetworkInterfaces(), updated.getNetworkInterfaces());
        return createInstance(original.getHostname(), newImages, newEnis);
    }

    public static AgentResourceCacheInstance removeTaskFromInstance(AgentResourceCacheInstance instance, V2WorkerMetadata task, long timestamp) {
        Preconditions.checkNotNull(instance, "instance cannot be null");
        V2WorkerMetadata.TwoLevelResource twoLevelResource = CollectionsExt.first(task.getTwoLevelResources());
        Preconditions.checkNotNull(twoLevelResource, "twoLevelResource cannot be null");
        int networkInterfaceIndex = Integer.parseInt(twoLevelResource.getLabel());
        Map<Integer, AgentResourceCacheNetworkInterface> networkInterfaces = instance.getNetworkInterfaces();
        AgentResourceCacheNetworkInterface networkInterface = networkInterfaces.get(networkInterfaceIndex);
        if (networkInterface == null) {
            return instance;
        }

        String taskId = WorkerNaming.getTaskId(task);
        String ipAddress = getIpAddress(task);
        AgentResourceCacheNetworkInterface newNetworkInterface = removeTaskIdFromNetworkInterface(taskId, ipAddress, networkInterface, timestamp);

        Map<Integer, AgentResourceCacheNetworkInterface> newNetworkInterfaces = CollectionsExt.copyAndAdd(networkInterfaces, networkInterfaceIndex, newNetworkInterface);
        return instance.toBuilder().withNetworkInterfaces(newNetworkInterfaces).build();
    }

    public static AgentResourceCacheInstance removeTaskFromInstance(AgentResourceCacheInstance instance, Task task, long timestamp) {
        Preconditions.checkNotNull(instance, "instance cannot be null");
        TwoLevelResource twoLevelResource = CollectionsExt.first(task.getTwoLevelResources());
        Preconditions.checkNotNull(twoLevelResource, "twoLevelResource cannot be null");
        int networkInterfaceIndex = twoLevelResource.getIndex();
        Map<Integer, AgentResourceCacheNetworkInterface> networkInterfaces = instance.getNetworkInterfaces();
        AgentResourceCacheNetworkInterface networkInterface = networkInterfaces.get(networkInterfaceIndex);
        if (networkInterface == null) {
            return instance;
        }

        String taskId = task.getId();
        String ipAddress = task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP, EMPTY_IP_ADDRESS);
        AgentResourceCacheNetworkInterface newNetworkInterface = removeTaskIdFromNetworkInterface(taskId, ipAddress, networkInterface, timestamp);

        Map<Integer, AgentResourceCacheNetworkInterface> newNetworkInterfaces = CollectionsExt.copyAndAdd(networkInterfaces, networkInterfaceIndex, newNetworkInterface);
        return instance.toBuilder().withNetworkInterfaces(newNetworkInterfaces).build();
    }

    private static String getIpAddress(V2WorkerMetadata task) {
        Object statusData = task.getStatusData();
        if (statusData == null || !(statusData instanceof Map)) {
            return EMPTY_IP_ADDRESS;
        }

        try {
            String stringData = ObjectMappers.defaultMapper().writeValueAsString(statusData);
            return ObjectMappers.defaultMapper().readValue(stringData, TitusExecutorDetails.class).getNetworkConfiguration().getIpAddress();
        } catch (Exception ignored) {
        }

        return EMPTY_IP_ADDRESS;
    }

    public static AgentResourceCacheNetworkInterface removeTaskIdFromNetworkInterface(String taskId,
                                                                                      String ipAddress,
                                                                                      AgentResourceCacheNetworkInterface networkInterface,
                                                                                      long timestamp) {
        Map<String, Set<String>> ipAddresses = networkInterface.getIpAddresses();
        Set<String> taskIdsForUnknownIp = ipAddresses.getOrDefault(EMPTY_IP_ADDRESS, Collections.emptySet());
        Set<String> taskIdsForIp = ipAddresses.getOrDefault(ipAddress, Collections.emptySet());
        Map<String, Set<String>> newIpAddresses = new HashMap<>(ipAddresses);
        boolean hasAvailableIps = false;
        if (taskIdsForUnknownIp.contains(taskId)) {
            Set<String> newTaskIdsForEmptyIp = CollectionsExt.copyAndRemove(taskIdsForUnknownIp, taskId);
            newIpAddresses.put(EMPTY_IP_ADDRESS, newTaskIdsForEmptyIp);
        }
        if (taskIdsForIp.contains(taskId)) {
            hasAvailableIps = true;
            Set<String> newTaskIdsForIp = CollectionsExt.copyAndRemove(taskIdsForIp, taskId);
            newIpAddresses.put(ipAddress, newTaskIdsForIp);
        }
        return networkInterface.toBuilder()
                .withIpAddresses(newIpAddresses)
                .withHasAvailableIps(hasAvailableIps)
                .withTimestamp(timestamp)
                .build();
    }
}
