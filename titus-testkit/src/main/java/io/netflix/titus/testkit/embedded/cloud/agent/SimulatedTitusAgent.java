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

package io.netflix.titus.testkit.embedded.cloud.agent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.testkit.embedded.cloud.resource.ComputeResources;
import io.titanframework.messages.TitanProtos.ContainerInfo;
import io.titanframework.messages.TitanProtos.ContainerInfo.NetworkConfigInfo;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Text;
import org.apache.mesos.Protos.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.subjects.BehaviorSubject;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import static java.util.Arrays.asList;

/**
 */
public class SimulatedTitusAgent {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedTitusAgent.class);

    private final Protos.SlaveID slaveId;
    private final Offer.Builder offerTemplate;
    private final AwsInstanceType instanceType;
    private final double totalCPUs;
    private final double totalGPUs;
    private final int totalMemory;
    private final int totalDisk;
    private final int totalNetworkMbs;
    private final String clusterName;
    private final String hostName;
    private final ComputeResources computeResources;
    private final Worker worker;

    private final long launchTime = System.currentTimeMillis();

    private final NetworkResourceTracker networkResourceTracker;

    private volatile double availableCPUs;
    private volatile double availableGPUs;
    private volatile int availableMemory;
    private volatile int availableDisk;
    private volatile int availableNetworkMbs;
    private volatile int offerIdx;

    private final Subject<OfferChangeEvent, OfferChangeEvent> offerUpdates = new SerializedSubject<>(BehaviorSubject.create());
    private volatile Offer rescindedOffer;
    private volatile Offer lastOffer;

    private final BehaviorSubject<Protos.TaskStatus> taskUpdates = BehaviorSubject.create();

    private final Subject<TaskExecutorHolder, TaskExecutorHolder> launchedTasksSubject = new SerializedSubject<>(PublishSubject.create());
    private final ConcurrentMap<TaskID, TaskExecutorHolder> pendingTasks = new ConcurrentHashMap<>();

    private final Set<Long> allocatedPorts = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Object lock = new Object();

    SimulatedTitusAgent(String clusterName, ComputeResources computeResources, String hostName, Protos.SlaveID slaveId,
                        Offer.Builder offerTemplate, AwsInstanceType instanceType,
                        double cpus, double gpus, int memory, int disk, int totalNetworkMbs,
                        int ipPerEni, Scheduler scheduler) {
        this.computeResources = computeResources;
        this.worker = scheduler.createWorker();
        logger.info("Creating a new agent {} with instance type {} and resources {cpu={}, memory={}, disk={}, networkMbs={}}",
                slaveId.getValue(), instanceType, cpus, memory, disk, totalNetworkMbs);
        this.clusterName = clusterName;
        this.hostName = hostName;
        this.slaveId = slaveId;
        this.offerTemplate = offerTemplate;
        this.instanceType = instanceType;

        this.totalCPUs = cpus;
        this.availableCPUs = cpus;

        this.totalGPUs = gpus;
        this.availableGPUs = gpus;

        this.totalMemory = memory;
        this.availableMemory = memory;

        this.totalDisk = disk;
        this.availableDisk = disk;

        this.totalNetworkMbs = totalNetworkMbs;
        this.availableNetworkMbs = totalNetworkMbs;

        this.networkResourceTracker = new NetworkResourceTracker(ipPerEni);

        emmitAvailableOffers();
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getHostName() {
        return hostName;
    }

    public String getId() {
        return slaveId.getValue();
    }

    public long getLaunchTime() {
        return launchTime;
    }

    void shutdown() {
        worker.unsubscribe();
        if (lastOffer != null) {
            offerUpdates.onNext(OfferChangeEvent.rescind(lastOffer));
            offerUpdates.onCompleted();
            lastOffer = null;
        }
    }

    public Protos.SlaveID getSlaveId() {
        return slaveId;
    }

    public Observable<OfferChangeEvent> observeOffers() {
        return offerUpdates;
    }

    public Observable<Protos.TaskStatus> taskStatusUpdates() {
        return taskUpdates;
    }

    public Observable<TaskExecutorHolder> observeTaskLaunches() {
        return launchedTasksSubject;
    }

    public List<TaskExecutorHolder> getAllTasks() {
        return new ArrayList<>(pendingTasks.values());
    }

    public Optional<TaskExecutorHolder> findTaskById(String taskId) {
        for (Map.Entry<TaskID, TaskExecutorHolder> entry : pendingTasks.entrySet()) {
            if (entry.getKey().getValue().equals(taskId)) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    public List<TaskExecutorHolder> launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        return launchTasks(offerIds, tasks);
    }

    public List<TaskExecutorHolder> launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks) {
        checkOffers(offerIds);
        if (lastOffer != null) {
            for (Protos.OfferID oid : offerIds) {
                if (oid.equals(lastOffer.getId())) {
                    rescindedOffer = lastOffer;
                    lastOffer = null;
                    break;
                }
            }
        }

        List<TaskExecutorHolder> taskExecutorHolders = launchTasksInternal(tasks);
        taskExecutorHolders.forEach(launchedTasksSubject::onNext);
        emmitAvailableOffers();
        return taskExecutorHolders;
    }

    List<TaskExecutorHolder> launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        return launchTasks(offerId, tasks);
    }

    List<TaskExecutorHolder> launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks) {
        return launchTasks(Collections.singletonList(offerId), tasks);
    }

    private List<TaskExecutorHolder> launchTasksInternal(Collection<Protos.TaskInfo> tasks) {
        synchronized (lock) {
            List<TaskExecutorHolder> taskIDs = tasks.stream().map(this::launchTaskInternal).collect(Collectors.toList());

            if (taskIDs.size() == 1) {
                logger.info("Launched task {} on agent {}", taskIDs.get(0).getTaskId(), slaveId.getValue());
            } else {
                StringBuilder sb = new StringBuilder();
                taskIDs.forEach(t -> sb.append(',').append(t.getTaskId()));
                logger.info("Launched tasks {} on agent {}", sb.substring(0), slaveId.getValue());
            }

            return taskIDs;
        }
    }

    private TaskExecutorHolder launchTaskInternal(Protos.TaskInfo task) {
        double taskCPUs = 0;
        double taskGPUs = 0;
        double taskMem = 0;
        double taskDisk = 0;
        double taskNetwork = 0;
        List<EfsMount> efsMounts;
        Set<Long> taskPorts = new HashSet<>();

        ContainerInfo containerInfo;
        try {
            containerInfo = ContainerInfo.parseFrom(task.getData());
        } catch (InvalidProtocolBufferException e) {
            handleTaskStateUpdate(task.getTaskId(), Protos.TaskState.TASK_FAILED, "Invalid ContainerInfo data: " + e.getMessage());

            // TODO Better handle failure scenarios during task launch
            throw new IllegalStateException("Bad container info");
        }

        String containerIp = null;
        if (containerInfo.getAllocateIpAddress()) {
            NetworkConfigInfo networkConfigInfo = containerInfo.getNetworkConfigInfo();
            containerIp = computeResources.allocateIpAddress();
            if (!networkResourceTracker.assign(task.getTaskId(), networkConfigInfo, containerIp)) {
                handleTaskStateUpdate(task.getTaskId(), Protos.TaskState.TASK_FAILED, "Invalid ENI assignment");

                // TODO Better handle failure scenarios during task launch
                throw new IllegalStateException("Invalid ENI assignment");
            }
        }
        efsMounts = extractEfsMounts(containerInfo);

        for (Resource resource : task.getResourcesList()) {
            switch (resource.getName()) {
                case "cpus":
                    taskCPUs = resource.getScalar().getValue();
                    if (availableCPUs < taskCPUs) {
                        throw new IllegalArgumentException(slaveId + " has not sufficient CPU resources: " + availableCPUs + " < " + taskCPUs);
                    }
                    availableCPUs -= taskCPUs;
                    break;
                case "gpu":
                    taskGPUs = resource.getScalar().getValue();
                    if (availableGPUs < taskGPUs) {
                        throw new IllegalArgumentException(slaveId + " has not sufficient GPU resources: " + availableGPUs + " < " + taskGPUs);
                    }
                    availableGPUs -= taskGPUs;
                    break;
                case "mem":
                    taskMem = resource.getScalar().getValue();
                    if (availableMemory < taskMem) {
                        throw new IllegalArgumentException(slaveId + " has not sufficient amount of memory: " + availableMemory + " < " + taskMem);
                    }
                    availableMemory -= taskMem;
                    break;
                case "disk":
                    taskDisk = resource.getScalar().getValue();
                    if (availableDisk < taskDisk) {
                        throw new IllegalArgumentException(slaveId + " has not sufficient amount of disk space: " + availableDisk + " < " + taskDisk);
                    }
                    availableDisk -= taskDisk;
                    break;
                case "ports":
                    for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
                        for (long port = range.getBegin(); port <= range.getEnd(); port++) {
                            taskPorts.add(port);
                            allocatedPorts.add(port);
                        }
                    }
                    break;
                case "network":
                    taskNetwork = resource.getScalar().getValue();
                    if (availableNetworkMbs < taskNetwork) {
                        throw new IllegalArgumentException(slaveId.getValue() + " has not sufficient amount of network bandwidth: " + availableNetworkMbs + " < " + taskNetwork);
                    }
                    availableNetworkMbs -= taskNetwork;
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized resource type " + resource.getName());
            }
        }
        TaskID taskId = TaskID.newBuilder().setValue(task.getName()).build();

        logger.info("Agent {} - allocated resources for task {}: cpu={}, memoryMB={}, networkMB={}, diskMB={}. Left on agent: cpu={}, memoryMB={}, networkMB={}, diskMB={}",
                slaveId.getValue(), taskId.getValue(), taskCPUs, taskMem, taskNetwork, taskDisk, availableCPUs, availableMemory, availableNetworkMbs, availableDisk);

        BehaviorSubject<Protos.TaskStatus> taskStatusSubject = BehaviorSubject.create();
        taskStatusSubject.subscribe(
                taskUpdates::onNext,
                e -> logger.info("Unexpected onError in task state observable stream for task " + task.getName(), e),
                () -> logger.info("Task {} status update stream completed", task.getName())
        );
        TaskExecutorHolder taskHolder = new TaskExecutorHolder(
                extractJobId(task), taskId.getValue(), this, instanceType, taskCPUs, taskGPUs,
                taskMem, taskDisk, taskPorts, containerIp, taskNetwork, efsMounts, taskStatusSubject);
        pendingTasks.put(taskId, taskHolder);

        return taskHolder;
    }

    private List<EfsMount> extractEfsMounts(ContainerInfo containerInfo) {
        return containerInfo.getEfsConfigInfoList().stream().map(efsConfig ->
                EfsMount.newBuilder()
                        .withEfsId(efsConfig.getEfsFsId())
                        .withMountPerm(EfsMount.MountPerm.valueOf(efsConfig.getMntPerms().name()))
                        .withMountPoint(efsConfig.getMountPoint())
                        .withEfsRelativeMountPoint(efsConfig.getEfsFsRelativeMntPoint())
                        .build())
                .collect(Collectors.toList());
    }

    private String extractJobId(Protos.TaskInfo task) {
        String v2TaskId = task.getTaskId().getValue();
        if (JobFunctions.isV2Task(v2TaskId)) {
            return WorkerNaming.getJobAndWorkerId(v2TaskId).jobId;
        }
        ContainerInfo containerInfo;
        try {
            containerInfo = ContainerInfo.parseFrom(task.getData());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid 'data' in TaskInfo object", e);
        }
        String jobId = containerInfo.getTitusProvidedEnvMap().get("TITUS_JOB_ID");
        Preconditions.checkState(jobId != null, "Incomplete 'data' in TaskInfo object (missing TITUS_JOB_ID)");
        return jobId;
    }

    public void killTask(TaskID taskId) {
        synchronized (lock) {
            TaskExecutorHolder taskExecutorHolder = pendingTasks.remove(taskId);
            if (taskExecutorHolder == null) {
                logger.warn(slaveId + " is not running task " + taskId); // don't throw, treat it as no-op
                return;
            }
            Protos.TaskState taskState = taskExecutorHolder.getState();
            if (taskState == Protos.TaskState.TASK_FINISHED || taskState == Protos.TaskState.TASK_FAILED || taskState == Protos.TaskState.TASK_KILLED) {
                taskUpdates.onNext(Protos.TaskStatus.newBuilder()
                        .setTaskId(taskId)
                        .setState(Protos.TaskState.TASK_LOST)
                        .setMessage("Task already terminated: " + taskState)
                        .build()
                );
                return;
            }

            taskExecutorHolder.transitionTo(Protos.TaskState.TASK_KILLED);

            releaseResourcesAndReOffer(taskExecutorHolder);
        }
    }

    public void declineOffer(Protos.OfferID offerId) {
        checkOffer(offerId);
        // To avoid tight loop, re-offer it after some delay
        worker.schedule(this::emmitAvailableOffers, 10, TimeUnit.MILLISECONDS);
    }

    public boolean hasOffer(Protos.OfferID offerID) {
        if (lastOffer == null) {
            return false;
        }
        if (lastOffer.getId().equals(offerID)) {
            return true;
        }
        if (rescindedOffer == null) {
            return false;
        }
        return rescindedOffer.getId().equals(offerID);
    }

    private void checkOffer(Protos.OfferID offerId) {
        if (!offerId.getValue().startsWith(slaveId.getValue())) {
            throw new IllegalArgumentException("Received an offer " + offerId + " not belonging to the agent " + slaveId);
        }
    }

    private void checkOffers(Collection<Protos.OfferID> offerIds) {
        offerIds.forEach(this::checkOffer);
    }

    private void emmitAvailableOffers() {
        if (lastOffer != null) {
            offerUpdates.onNext(OfferChangeEvent.rescind(lastOffer));
            rescindedOffer = lastOffer;
            lastOffer = null;
        }
        if (aboveZero()) {
            lastOffer = createOfferForAvailableResources();
            offerUpdates.onNext(OfferChangeEvent.offer(lastOffer));
        }
    }

    private void releaseResourcesAndReOffer(TaskExecutorHolder taskExecutorHolder) {
        TaskID taskId = TaskID.newBuilder().setValue(taskExecutorHolder.getTaskId()).build();

        availableCPUs += taskExecutorHolder.getTaskCPUs();
        availableGPUs += taskExecutorHolder.getTaskGPUs();
        availableMemory += taskExecutorHolder.getTaskMem();
        availableDisk += taskExecutorHolder.getTaskDisk();
        availableNetworkMbs += taskExecutorHolder.getTaskNetworkMbs();
        allocatedPorts.removeAll(taskExecutorHolder.getAllocatedPorts());

        // TODO Check if IP assignment was requested
        networkResourceTracker.unAssign(taskId);

        logger.info("Agent {} -> released resources of task {}: cpu={}, memoryMB={}, networkMB={}, diskMB={}. Left on agent: cpu={}, memoryMB={}, networkMB={}, diskMB={}",
                slaveId.getValue(), taskId.getValue(), taskExecutorHolder.getTaskCPUs(), taskExecutorHolder.getTaskMem(), taskExecutorHolder.getTaskNetworkMbs(), taskExecutorHolder.getTaskDisk(),
                availableCPUs, availableMemory, availableNetworkMbs, availableDisk);

        Preconditions.checkArgument(availableCPUs <= totalCPUs,
                "CPU inconsistency in resource allocation/reclaim detected for agent " + slaveId.getValue()
        );
        Preconditions.checkArgument(availableGPUs <= totalGPUs,
                "GPU inconsistency in resource allocation/reclaim detected for agent " + slaveId.getValue()
        );
        Preconditions.checkArgument(availableMemory <= totalMemory,
                "Memory inconsistency in resource allocation/reclaim detected for agent " + slaveId.getValue()
        );
        Preconditions.checkArgument(availableDisk <= totalDisk,
                "Disk inconsistency in resource allocation/reclaim detected for agent " + slaveId.getValue()
        );
        Preconditions.checkArgument(availableNetworkMbs <= totalNetworkMbs,
                "Network inconsistency in resource allocation/reclaim detected for agent " + slaveId.getValue()
        );

        emmitAvailableOffers();
    }

    private boolean aboveZero() {
        return availableCPUs > 0 && availableMemory > 0 && availableDisk > 0;
    }

    private Offer createOfferForAvailableResources() {
        Protos.OfferID offerId = Protos.OfferID.newBuilder().setValue(slaveId.getValue() + "_O_" + offerIdx++).build();
        String enis = "ResourceSet-ENIs-7-" + networkResourceTracker.getIpsPerEni();
        return offerTemplate.clone()
                .setId(offerId)
                .setSlaveId(slaveId)
                .addAllResources(asList(
                        Resource.newBuilder().setName("cpus").setType(Type.SCALAR).setScalar(Scalar.newBuilder().setValue(availableCPUs)).build(),
                        Resource.newBuilder().setName("gpu").setType(Type.SCALAR).setScalar(Scalar.newBuilder().setValue(availableGPUs)).build(),
                        Resource.newBuilder().setName("mem").setType(Type.SCALAR).setScalar(Scalar.newBuilder().setValue(availableMemory)).build(),
                        Resource.newBuilder().setName("disk").setType(Type.SCALAR).setScalar(Scalar.newBuilder().setValue(availableDisk)).build(),
                        Resource.newBuilder().setName("ports").setType(Type.RANGES).setRanges(
                                Ranges.newBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(1024).setEnd(65535).build()).build()
                        ).build(),
                        Resource.newBuilder().setName("network").setType(Type.SCALAR).setScalar(Scalar.newBuilder().setValue(availableNetworkMbs)).build()
                ))
                .addAllAttributes(asList(
                        Attribute.newBuilder().setName("cluster").setType(Type.TEXT).setText(Text.newBuilder().setValue(clusterName)).build(),
                        Attribute.newBuilder().setName("asg").setType(Type.TEXT).setText(Text.newBuilder().setValue(clusterName)).build(),
                        Attribute.newBuilder().setName("id").setType(Type.TEXT).setText(Text.newBuilder().setValue(hostName)).build(),
                        Attribute.newBuilder().setName("itype").setType(Type.TEXT).setText(Text.newBuilder().setValue(instanceType.getDescriptor().getId())).build(),
                        Attribute.newBuilder().setName("SLAVE_ID").setType(Type.TEXT).setText(Text.newBuilder().setValue(clusterName)).build(),
                        Attribute.newBuilder().setName("res").setType(Type.TEXT).setText(Text.newBuilder().setValue(enis)).build()
                ))
                .build();
    }

    public Set<String> reconcileOwnedTasksIgnoreOther(Collection<Protos.TaskStatus> statuses) {
        if (statuses.isEmpty()) {
            return pendingTasks.values().stream()
                    .map(h -> {
                        handleTaskStateUpdate(TaskID.newBuilder().setValue(h.getTaskId()).build(), h.getState(), "Triggered by reconciler");
                        return h.getTaskId();
                    }).collect(Collectors.toSet());
        }

        return statuses.stream()
                .filter(status -> pendingTasks.containsKey(status.getTaskId()))
                .map(status -> {
                    TaskExecutorHolder holder = pendingTasks.get(status.getTaskId());
                    handleTaskStateUpdate(status.getTaskId(), holder.getState(), "Triggered by reconciler");
                    return status.getTaskId().getValue();
                }).collect(Collectors.toSet());
    }

    public void removeCompletedTask(TaskExecutorHolder holder) {
        synchronized (lock) {
            TaskExecutorHolder removed = pendingTasks.remove(TaskID.newBuilder().setValue(holder.getTaskId()).build());
            if (removed != null) {
                releaseResourcesAndReOffer(holder);
            }
        }
    }

    private void handleTaskStateUpdate(TaskID taskId, Protos.TaskState taskState, String message) {
        Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setHealthy(true)
                .setState(taskState)
                .setMessage(message)
                .build();
        taskUpdates.onNext(taskStatus);
    }

    private static class EniAndIpAssignment {

        private final String eni;
        private final String ipAddress;

        private EniAndIpAssignment(String eni, String ipAddress) {
            this.eni = eni;
            this.ipAddress = ipAddress;
        }

        private String getEni() {
            return eni;
        }

        private String getIpAddress() {
            return ipAddress;
        }
    }

    private static class NetworkResourceTracker {

        private int ipsPerEni;

        private final Map<String, Pair<Integer, String>> eniAssignments = new HashMap<>();
        private final Map<TaskID, EniAndIpAssignment> eniTaskAssignments = new HashMap<>();

        private NetworkResourceTracker(int ipsPerEni) {
            this.ipsPerEni = ipsPerEni;
        }

        private boolean assign(TaskID taskId, NetworkConfigInfo networkConfigInfo, String containerIp) {
            Preconditions.checkArgument(!eniTaskAssignments.containsKey(taskId));

            String eniLabel = networkConfigInfo.getEniLablel();
            String taskSecurityGroups = StringExt.concatenate(networkConfigInfo.getSecurityGroupsList(), ",");

            Pair<Integer, String> eniState = eniAssignments.computeIfAbsent(eniLabel, l -> Pair.of(ipsPerEni, null));
            String assignedSecurityGroups = eniState.getRight();
            int availableIpCount = eniState.getLeft();

            if (assignedSecurityGroups != null && !assignedSecurityGroups.equals(taskSecurityGroups)) {
                return false;
            }
            if (availableIpCount <= 0) {
                return false;
            }

            eniAssignments.put(eniLabel, Pair.of(availableIpCount - 1, taskSecurityGroups));
            eniTaskAssignments.put(taskId, new EniAndIpAssignment(eniLabel, containerIp));

            return true;
        }

        private boolean unAssign(TaskID taskId) {
            EniAndIpAssignment assignment = eniTaskAssignments.remove(taskId);
            if (assignment == null) {
                return false;
            }
            String eniLabel = assignment.getEni();
            Pair<Integer, String> eniState = eniAssignments.get(eniLabel);
            int availableIpCount = eniState.getLeft() + 1;
            if (availableIpCount == ipsPerEni) {
                eniAssignments.remove(eniLabel);
            } else {
                eniAssignments.put(eniLabel, Pair.of(availableIpCount, eniState.getRight()));
            }
            return true;
        }

        private int getIpsPerEni() {
            return ipsPerEni;
        }
    }
}
