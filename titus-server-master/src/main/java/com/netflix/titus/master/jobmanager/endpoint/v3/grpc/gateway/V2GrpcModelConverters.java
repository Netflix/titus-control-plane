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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.api.model.SelfManagedMigrationPolicy;
import com.netflix.titus.api.model.v2.JobCompletedReason;
import com.netflix.titus.api.model.v2.JobConstraints;
import com.netflix.titus.api.model.v2.JobSla;
import com.netflix.titus.api.model.v2.MachineDefinition;
import com.netflix.titus.api.model.v2.ServiceJobProcesses;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.V2JobDurationType;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import com.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import com.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2StageMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.BatchJobSpec;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Constraints;
import com.netflix.titus.grpc.protogen.Container;
import com.netflix.titus.grpc.protogen.ContainerResources;
import com.netflix.titus.grpc.protogen.Image;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobGroupInfo;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.JobStatus.JobState;
import com.netflix.titus.grpc.protogen.MigrationPolicy;
import com.netflix.titus.grpc.protogen.MigrationPolicy.SelfManaged;
import com.netflix.titus.grpc.protogen.MigrationPolicy.SystemDefault;
import com.netflix.titus.grpc.protogen.MountPerm;
import com.netflix.titus.grpc.protogen.Owner;
import com.netflix.titus.grpc.protogen.RetryPolicy;
import com.netflix.titus.grpc.protogen.SecurityProfile;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.store.NamedJobs;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_REGION;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_CELL;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_RESUBMIT_NUMBER;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_STACK;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TASK_ORIGINAL_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TASK_RESUBMIT_OF;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_V2_TASK_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_V2_TASK_INSTANCE_ID;
import static com.netflix.titus.api.jobmanager.model.job.Container.RESOURCE_GPU;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_UNKNOWN;
import static com.netflix.titus.common.util.CollectionsExt.applyNotEmpty;
import static com.netflix.titus.common.util.Evaluators.applyNotNull;
import static com.netflix.titus.common.util.StringExt.isNotEmpty;
import static com.netflix.titus.master.job.worker.WorkerRequest.V2_NETFLIX_APP_METADATA;
import static com.netflix.titus.master.job.worker.WorkerRequest.V2_NETFLIX_APP_METADATA_SIG;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.LABEL_LEGACY_NAME;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.SPACE_SPLIT_RE;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.TASK_CONTEXT_AGENT_ATTRIBUTES;
import static java.util.Arrays.asList;

public final class V2GrpcModelConverters {

    private static final Logger logger = LoggerFactory.getLogger(V2GrpcModelConverters.class);

    private V2GrpcModelConverters() {
    }

    public static Job toGrpcJob(V2JobMetadata jobMetadata) {
        return Job.newBuilder()
                .setId(jobMetadata.getJobId())
                .setJobDescriptor(toGrpcJobDescriptor(jobMetadata))
                .setStatus(toGrpcJobStatus(jobMetadata))
                .addAllStatusHistory(toGrpcJobStatusHistory(jobMetadata))
                .build();
    }

    public static JobDescriptor toGrpcJobDescriptor(V2JobMetadata jobMetadata) {
        List<Parameter> parameters = jobMetadata.getParameters();
        V2StageMetadata stageMetadata = jobMetadata.getStageMetadata(1);
        MachineDefinition machineDefinition = stageMetadata.getMachineDefinition();
        StageScalingPolicy scalingPolicy = stageMetadata.getScalingPolicy();
        JobSla sla = jobMetadata.getSla();

        JobDescriptor.Builder descriptorBuilder = JobDescriptor.newBuilder();
        Container.Builder containerBuilder = Container.newBuilder();

        applyNotNull(Parameters.getAppName(parameters), descriptorBuilder::setApplicationName);
        applyNotNull(jobMetadata.getUser(), v -> descriptorBuilder.setOwner(Owner.newBuilder().setTeamEmail(v)));
        Image.Builder imageBuilder = Image.newBuilder();
        applyNotNull(Parameters.getImageName(parameters), imageBuilder::setName);
        applyNotNull(Parameters.getImageDigest(parameters), imageBuilder::setDigest);
        applyNotNull(Parameters.getVersion(parameters), imageBuilder::setTag);
        containerBuilder.setImage(imageBuilder);

        // Resources
        com.netflix.titus.grpc.protogen.ContainerResources.Builder resourcesBuilder = com.netflix.titus.grpc.protogen.ContainerResources.newBuilder()
                .setCpu(machineDefinition.getCpuCores())
                .setMemoryMB((int) machineDefinition.getMemoryMB())
                .setDiskMB((int) machineDefinition.getDiskMB())
                .setNetworkMbps((int) machineDefinition.getNetworkMbps())
                .setAllocateIP(stageMetadata.getAllocateIP());
        applyNotNull(machineDefinition.getScalars(), scalars -> applyNotNull(scalars.get(RESOURCE_GPU), gpu -> resourcesBuilder.setGpu(gpu.intValue())));

        List<com.netflix.titus.grpc.protogen.ContainerResources.EfsMount> efsMounts = toGrpcEfsMounts(parameters);
        if (!efsMounts.isEmpty()) {
            resourcesBuilder.addAllEfsMounts(efsMounts);
        }
        containerBuilder.setResources(resourcesBuilder);

        applyNotNull(Parameters.getCapacityGroup(parameters), descriptorBuilder::setCapacityGroup);

        // Entry point / environment
        applyNotNull(Parameters.getEntryPoint(parameters), v -> containerBuilder.addAllEntryPoint(asList(SPACE_SPLIT_RE.split(v))));
        applyNotNull(Parameters.getEnv(parameters), containerBuilder::putAllEnv);

        // Labels
        HashMap<String, String> labels = new HashMap<>();
        Evaluators.acceptNotNull(Parameters.getLabels(parameters), labels::putAll);

        applyNotNull(Parameters.getName(parameters), v -> labels.put(LABEL_LEGACY_NAME, v));
        descriptorBuilder.putAllAttributes(labels);

        // Constraints
        applyNotEmpty(stageMetadata.getHardConstraints(), v -> containerBuilder.setSoftConstraints(toGrpcJobConstraintList(v)));
        applyNotEmpty(stageMetadata.getSoftConstraints(), v -> containerBuilder.setSoftConstraints(toGrpcJobConstraintList(v)));

        SecurityProfile.Builder securityProfileBuilder = SecurityProfile.newBuilder();
        applyNotNull(Parameters.getIamProfile(parameters), securityProfileBuilder::setIamRole);
        applyNotNull(Parameters.getSecurityGroups(parameters), securityProfileBuilder::addAllSecurityGroups);

        Map<String, String> securityAttributes = new HashMap<>();
        if (labels.containsKey(V2_NETFLIX_APP_METADATA)) {
            securityAttributes.put(V2_NETFLIX_APP_METADATA, labels.get(V2_NETFLIX_APP_METADATA));
        }
        if (labels.containsKey(V2_NETFLIX_APP_METADATA_SIG)) {
            securityAttributes.put(V2_NETFLIX_APP_METADATA_SIG, labels.get(V2_NETFLIX_APP_METADATA_SIG));
        }
        securityProfileBuilder.putAllAttributes(securityAttributes);

        containerBuilder.setSecurityProfile(securityProfileBuilder);

        // JobGroupInfo
        JobGroupInfo.Builder jobGroupInfoBuilder = JobGroupInfo.newBuilder();
        applyNotNull(Parameters.getJobGroupStack(parameters), jobGroupInfoBuilder::setStack);
        applyNotNull(Parameters.getJobGroupDetail(parameters), jobGroupInfoBuilder::setDetail);
        applyNotNull(Parameters.getJobGroupSeq(parameters), jobGroupInfoBuilder::setSequence);
        descriptorBuilder.setJobGroupInfo(jobGroupInfoBuilder);

        // Job types (service|batch)
        Parameters.JobType jobType = Parameters.getJobType(parameters);
        if (jobType == Parameters.JobType.Service) {
            ServiceJobSpec.Builder serviceJobBuilder = ServiceJobSpec.newBuilder();
            Capacity.Builder capacityBuilder = Capacity.newBuilder();
            if (scalingPolicy != null) {
                capacityBuilder.setDesired(scalingPolicy.getDesired());
                capacityBuilder.setMin(scalingPolicy.getMin());
                capacityBuilder.setMax(scalingPolicy.getMax());
            }
            serviceJobBuilder.setCapacity(capacityBuilder);
            serviceJobBuilder.setRetryPolicy(RetryPolicy.newBuilder().setImmediate(RetryPolicy.Immediate.getDefaultInstance()));
            serviceJobBuilder.setEnabled(Parameters.getInService(parameters));

            ServiceJobProcesses jobProcesses = stageMetadata.getJobProcesses();
            if (jobProcesses != null) {
                ServiceJobSpec.ServiceJobProcesses serviceJobProcesses = ServiceJobSpec.ServiceJobProcesses.newBuilder()
                        .setDisableDecreaseDesired(jobProcesses.isDisableDecreaseDesired())
                        .setDisableIncreaseDesired(jobProcesses.isDisableIncreaseDesired())
                        .build();
                serviceJobBuilder.setServiceJobProcesses(serviceJobProcesses);
            }

            descriptorBuilder.setService(serviceJobBuilder);

            com.netflix.titus.api.model.MigrationPolicy v2MigrationPolicy = Parameters.getMigrationPolicy(parameters);
            MigrationPolicy migrationPolicy = v2MigrationPolicy instanceof SelfManagedMigrationPolicy ?
                    MigrationPolicy.newBuilder().setSelfManaged(SelfManaged.newBuilder()).build()
                    :
                    MigrationPolicy.newBuilder().setSystemDefault(SystemDefault.newBuilder()).build();
            serviceJobBuilder.setMigrationPolicy(migrationPolicy);
        } else {
            BatchJobSpec.Builder batchJobBuilder = BatchJobSpec.newBuilder();
            batchJobBuilder.setSize(stageMetadata.getNumWorkers());
            batchJobBuilder.setRetryPolicy(RetryPolicy.newBuilder().setImmediate(
                    RetryPolicy.Immediate.newBuilder().setRetries(sla.getRetries())
            ));
            batchJobBuilder.setRuntimeLimitSec(sla.getRuntimeLimitSecs());
            descriptorBuilder.setBatch(batchJobBuilder);
        }

        descriptorBuilder.setContainer(containerBuilder);

        return descriptorBuilder.build();
    }

    public static Task toGrpcTask(MasterConfiguration configuration, V2WorkerMetadata worker, LogStorageInfo<V2WorkerMetadata> logStorageInfo) {
        Task.Builder taskBuilder = Task.newBuilder();
        String v2TaskId = WorkerNaming.getTaskId(worker);

        taskBuilder.setId(v2TaskId);
        taskBuilder.setJobId(worker.getJobId());
        taskBuilder.setStatus(toGrpcTaskStatus(worker));
        taskBuilder.addAllStatusHistory(toGrpcTaskStatusHistory(worker));
        taskBuilder.setLogLocation(V3GrpcModelConverters.toGrpcLogLocation(worker, logStorageInfo));
        taskBuilder.setMigrationDetails(toGrpcMigrationDetails(worker));

        // Task context data
        taskBuilder.putTaskContext(TASK_ATTRIBUTES_V2_TASK_ID, v2TaskId);
        taskBuilder.putTaskContext(TASK_ATTRIBUTES_V2_TASK_INSTANCE_ID, worker.getWorkerInstanceId());
        taskBuilder.putTaskContext(TASK_ATTRIBUTES_TASK_INDEX, Integer.toString(worker.getWorkerIndex()));
        taskBuilder.putTaskContext(TASK_ATTRIBUTES_TASK_ORIGINAL_ID, v2TaskId);
        if (worker.getResubmitOf() >= 0) {
            taskBuilder.putTaskContext(TASK_ATTRIBUTES_TASK_RESUBMIT_OF, worker.getJobId() + "-worker-" + worker.getWorkerIndex() + "-" + worker.getResubmitOf());
        }
        taskBuilder.putTaskContext(TASK_ATTRIBUTES_RESUBMIT_NUMBER, Integer.toString(worker.getTotalResubmitCount()));
        if (worker.getCell() != null && !worker.getCell().isEmpty()) {
            taskBuilder.putTaskContext(TASK_ATTRIBUTES_CELL, worker.getCell());
            taskBuilder.putTaskContext(TASK_ATTRIBUTES_STACK, worker.getCell());
        }

        parseTitusExecutorDetails(worker.getStatusData()).ifPresent(details -> {
            if (details.getNetworkConfiguration() != null && !StringExt.isEmpty(details.getNetworkConfiguration().getIpAddress())) {
                taskBuilder.putTaskContext(TASK_ATTRIBUTES_CONTAINER_IP, details.getNetworkConfiguration().getIpAddress());
            }
        });

        TASK_CONTEXT_AGENT_ATTRIBUTES.forEach(name -> applyNotNull(
                worker.getSlaveAttributes().get(name),
                value -> taskBuilder.putTaskContext("agent." + name, value)
        ));

        applyNotNull(configuration.getRegion(), r -> taskBuilder.putTaskContext(TASK_ATTRIBUTES_AGENT_REGION, r));
        applyNotNull(getWorkerZone(configuration, worker), r -> taskBuilder.putTaskContext(TASK_ATTRIBUTES_AGENT_ZONE, r));
        applyNotNull(worker.getSlave(), r -> taskBuilder.putTaskContext(TASK_ATTRIBUTES_AGENT_HOST, r));
        applyNotNull(worker.getSlaveAttributes().get("id"), r -> taskBuilder.putTaskContext(TASK_ATTRIBUTES_AGENT_INSTANCE_ID, r));

        return taskBuilder.build();
    }

    public static Optional<TitusExecutorDetails> parseTitusExecutorDetails(Object statusData) {
        if (statusData == null || !(statusData instanceof Map)) {
            return Optional.empty();
        }

        try {
            String stringData = ObjectMappers.defaultMapper().writeValueAsString(statusData);
            return Optional.of(ObjectMappers.defaultMapper().readValue(stringData, TitusExecutorDetails.class));
        } catch (Exception e) {
            logger.warn("Incompatible task status data format: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public static List<JobStatus> toGrpcJobStatusHistory(V2JobMetadata jobMetadata) {
        List<com.netflix.titus.grpc.protogen.JobStatus> statusHistory = new ArrayList<>();
        JobStatus jobStatus = toGrpcJobStatus(jobMetadata);
        if (jobStatus.getState() != JobState.Accepted && jobMetadata.getSubmittedAt() > 0) {
            JobStatus status = JobStatus.newBuilder()
                    .setState(JobState.Accepted)
                    .setReasonMessage("state change")
                    .setTimestamp(jobMetadata.getSubmittedAt())
                    .build();
            statusHistory.add(status);
        }
        return statusHistory;
    }

    public static List<com.netflix.titus.grpc.protogen.TaskStatus> toGrpcTaskStatusHistory(V2WorkerMetadata worker) {
        List<com.netflix.titus.grpc.protogen.TaskStatus> statusHistory = new ArrayList<>();
        if (worker.getState() != V2JobState.Accepted && worker.getAcceptedAt() > 0) {
            TaskStatus status = TaskStatus.newBuilder()
                    .setState(TaskState.Accepted)
                    .setReasonMessage("state change")
                    .setTimestamp(worker.getAcceptedAt())
                    .build();
            statusHistory.add(status);
        }
        if (worker.getState() != V2JobState.Launched && worker.getLaunchedAt() > 0) {
            TaskStatus status = TaskStatus.newBuilder()
                    .setState(TaskState.Launched)
                    .setReasonMessage("state change")
                    .setTimestamp(worker.getLaunchedAt())
                    .build();
            statusHistory.add(status);
        }
        if (worker.getState() != V2JobState.StartInitiated && worker.getStartingAt() > 0) {
            TaskStatus status = TaskStatus.newBuilder()
                    .setState(TaskState.StartInitiated)
                    .setReasonMessage("state change")
                    .setTimestamp(worker.getStartedAt())
                    .build();
            statusHistory.add(status);
        }
        if (worker.getState() != V2JobState.Started && worker.getStartedAt() > 0) {
            TaskStatus status = TaskStatus.newBuilder()
                    .setState(TaskState.Started)
                    .setReasonMessage("state change")
                    .setTimestamp(worker.getStartedAt())
                    .build();
            statusHistory.add(status);
        }
        return statusHistory;
    }

    public static V2JobState toV2JobState(TaskState grpcTaskState) {
        switch (grpcTaskState) {
            case Accepted:
                return V2JobState.Accepted;
            case Launched:
                return V2JobState.Launched;
            case StartInitiated:
                return V2JobState.StartInitiated;
            case Started:
                return V2JobState.Started;
            case Finished:
                return V2JobState.Completed;
        }
        throw new IllegalArgumentException("Unrecognized state " + grpcTaskState);
    }

    public static JobCompletedReason toV2JobCompletedReason(String reason) {
        switch (reason) {
            case "normal":
                return JobCompletedReason.Normal;
            case "failed":
                return JobCompletedReason.Failed;
            case "crashed":
                return JobCompletedReason.Failed;
            case "lost":
                return JobCompletedReason.Lost;
            case "killed":
                return JobCompletedReason.Killed;
        }
        throw new IllegalArgumentException("Unrecognized V2 reason " + reason);
    }

    public static JobStatus toGrpcJobStatus(V2JobMetadata jobMetadata) {
        V2JobState v2JobState = jobMetadata.getState();
        JobStatus.Builder builder = JobStatus.newBuilder();
        switch (v2JobState) {
            case Accepted:
            case StartInitiated:
            case Launched:
            case Started:
                builder.setState(JobState.Accepted)
                        .setTimestamp(jobMetadata.getSubmittedAt());
                break;
            case Completed:
                builder.setState(JobState.Finished)
                        .setReasonCode(REASON_NORMAL);
                break;
            case Failed:
                builder.setState(JobState.Finished)
                        .setReasonCode(REASON_FAILED);
                break;
            default:
                builder.setState(JobState.UNRECOGNIZED);
        }

        return builder.setReasonMessage("state change").build();
    }

    public static TaskStatus toGrpcTaskStatus(V2WorkerMetadata worker) {
        V2JobState v2JobState = worker.getState();
        JobCompletedReason reasonCode = worker.getReason();
        TaskStatus.Builder stateBuilder = TaskStatus.newBuilder();
        String finalReason = null;
        switch (v2JobState) {
            case Accepted:
                stateBuilder.setState(TaskState.Accepted)
                        .setTimestamp(worker.getAcceptedAt());
                break;
            case Launched:
                stateBuilder.setState(TaskState.Launched)
                        .setTimestamp(worker.getLaunchedAt());
                break;
            case StartInitiated:
                stateBuilder.setState(TaskState.StartInitiated)
                        .setTimestamp(worker.getStartingAt());
                break;
            case Started:
                stateBuilder.setState(TaskState.Started)
                        .setTimestamp(worker.getStartedAt());
                break;
            case Completed:
                stateBuilder.setState(TaskState.Finished)
                        .setTimestamp(worker.getCompletedAt())
                        .setReasonCode(REASON_NORMAL);
                break;
            case Failed:
                String grpcReason;
                switch (reasonCode) {
                    case Normal:
                        grpcReason = REASON_NORMAL;
                        break;
                    case Error:
                        grpcReason = REASON_FAILED;
                        break;
                    case Lost:
                        grpcReason = REASON_TASK_LOST;
                        break;
                    case TombStone:
                    case Killed:
                        grpcReason = REASON_TASK_KILLED;
                        break;
                    case Failed:
                        grpcReason = REASON_FAILED;
                        break;
                    default:
                        grpcReason = REASON_UNKNOWN;
                }
                stateBuilder.setState(TaskState.Finished)
                        .setTimestamp(worker.getCompletedAt())
                        .setReasonCode(grpcReason);
                break;
            default:
                stateBuilder.setState(TaskState.UNRECOGNIZED).setReasonCode(REASON_UNKNOWN);
                finalReason = "Unrecognized job state " + v2JobState;
        }
        if (finalReason != null) {
            stateBuilder.setReasonMessage(finalReason);
        } else {
            stateBuilder.setReasonMessage("v2ReasonCode=" + (reasonCode == null ? "<not set>" : reasonCode) +
                    ", v2ReasonMessage=" + (worker.getCompletionMessage() == null ? "<not set>" : worker.getCompletionMessage())
            );
        }
        return stateBuilder.build();
    }

    public static JobChangeNotification toGrpcJobNotification(V2JobMetadata jobMetadata) {
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder().setJob(toGrpcJob(jobMetadata)))
                .build();
    }

    public static List<com.netflix.titus.grpc.protogen.ContainerResources.EfsMount> toGrpcEfsMounts(List<Parameter> parameters) {
        List<EfsMount> efsMounts = Parameters.getEfsMounts(parameters);
        if (efsMounts == null) {
            EfsMount efsMount = Parameters.getEfs(parameters);
            if (efsMount == null) {
                return Collections.emptyList();
            }
            efsMounts = Collections.singletonList(efsMount);
        }
        return efsMounts.stream().map(V2GrpcModelConverters::toGrpcEfsMountFromV2).collect(Collectors.toList());
    }

    private static com.netflix.titus.grpc.protogen.ContainerResources.EfsMount toGrpcEfsMountFromV2(EfsMount coreEfsMount) {
        ContainerResources.EfsMount.Builder builder = ContainerResources.EfsMount.newBuilder()
                .setEfsId(coreEfsMount.getEfsId())
                .setMountPerm(MountPerm.valueOf(coreEfsMount.getMountPerm().name()))
                .setMountPoint(coreEfsMount.getMountPoint());
        if (isNotEmpty(coreEfsMount.getEfsRelativeMountPoint())) {
            builder.setEfsRelativeMountPoint(coreEfsMount.getEfsRelativeMountPoint());
        }
        return builder.build();
    }


    private static EfsMount toV2EfsMount(com.netflix.titus.grpc.protogen.ContainerResources.EfsMount grpcEfsMount) {
        return EfsMount.newBuilder()
                .withEfsId(grpcEfsMount.getEfsId())
                .withMountPerm(EfsMount.MountPerm.valueOf(grpcEfsMount.getMountPerm().name()))
                .withMountPoint(grpcEfsMount.getMountPoint())
                .withEfsRelativeMountPoint(grpcEfsMount.getEfsRelativeMountPoint())
                .build();
    }

    public static List<EfsMount> toV2EfsMounts(List<com.netflix.titus.grpc.protogen.ContainerResources.EfsMount> efsResource) {
        return efsResource.stream().map(V2GrpcModelConverters::toV2EfsMount).collect(Collectors.toList());
    }

    public static com.netflix.titus.grpc.protogen.MigrationDetails toGrpcMigrationDetails(V2WorkerMetadata worker) {
        boolean needsMigration = worker.getMigrationDeadline() > 0;
        return com.netflix.titus.grpc.protogen.MigrationDetails.newBuilder()
                .setNeedsMigration(needsMigration)
                .setDeadline(worker.getMigrationDeadline())
                .build();
    }

    /**
     * Mapping from pod based model to the legacy V2 job definition. We expect single pod with single container.
     */
    public static V2JobDefinition toV2JobDefinition(JobDescriptor jobDescriptor) {
        Preconditions.checkArgument(jobDescriptor.getOwner() != null && jobDescriptor.getOwner().getTeamEmail() != null, "Job creator not provided");
        Preconditions.checkArgument(jobDescriptor.getContainer() != null, "Single container expected");

        Container container = jobDescriptor.getContainer();

        int instances;
        int instancesMin;
        int instancesMax;
        int instancesDesired;
        int retries;
        long runtimeLimit;
        switch (jobDescriptor.getJobSpecCase()) {
            case BATCH:
                BatchJobSpec batchJob = jobDescriptor.getBatch();
                instances = instancesMin = instancesMax = instancesDesired = batchJob.getSize();
                RetryPolicy retryPolicy = batchJob.getRetryPolicy();
                if (retryPolicy == null) {
                    retries = 0;
                } else {
                    switch (retryPolicy.getPolicyCase()) {
                        case IMMEDIATE:
                            retries = retryPolicy.getImmediate().getRetries();
                            break;
                        case DELAYED:
                            retries = retryPolicy.getDelayed().getRetries();
                            break;
                        case EXPONENTIALBACKOFF:
                            retries = retryPolicy.getExponentialBackOff().getRetries();
                            break;
                        default:
                            retries = 0;
                    }
                }
                runtimeLimit = batchJob.getRuntimeLimitSec();
                break;
            case SERVICE:
                ServiceJobSpec serviceJob = jobDescriptor.getService();
                instances = instancesDesired = serviceJob.getCapacity().getDesired();
                instancesMin = serviceJob.getCapacity().getMin();
                instancesMax = serviceJob.getCapacity().getMax();
                retries = Integer.MAX_VALUE;
                runtimeLimit = Integer.MAX_VALUE;
                break;
            default:
                throw new RuntimeException("mapping error");
        }

        JobSla jobSla = new JobSla(
                retries,
                runtimeLimit,
                0L,
                JobSla.StreamSLAType.Lossy,
                jobDescriptor.getJobSpecCase() == JobSpecCase.BATCH
                        ? V2JobDurationType.Transient
                        : V2JobDurationType.Perpetual,
                null
        );

        List<Parameter> parameters = toParameterList(jobDescriptor);

        MachineDefinition machineDefinition = toMachineDefinition(jobDescriptor.getContainer().getResources());

        StageScalingPolicy scalingPolicy = new StageScalingPolicy(
                1,
                instancesMin,
                instancesMax,
                instancesDesired,
                1, 1, 600, Collections.emptyMap()
        );

        StageSchedulingInfo stageSchedulingInfo = new StageSchedulingInfo(
                instances,
                machineDefinition,
                container.getHardConstraints() != null ? toJobConstraintList(container.getHardConstraints()) : Collections.emptyList(),
                container.getSoftConstraints() != null ? toJobConstraintList(container.getSoftConstraints()) : Collections.emptyList(),
                container.getSecurityProfile().getSecurityGroupsList(),
                container.getResources().getAllocateIP(),
                scalingPolicy,
                jobDescriptor.getJobSpecCase() == JobSpecCase.SERVICE
        );

        SchedulingInfo schedulingInfo = new SchedulingInfo(
                Collections.singletonMap(1, stageSchedulingInfo)
        );

        return new V2JobDefinition(
                NamedJobs.TitusJobName,
                jobDescriptor.getOwner().getTeamEmail(),
                null,
                "latest", // FIXME how do we handle versioning in v3?
                parameters,
                jobSla,
                0L,
                schedulingInfo,
                0,
                0,
                null,
                null
        );
    }

    public static MachineDefinition toMachineDefinition(com.netflix.titus.grpc.protogen.ContainerResources resources) {
        double gpu = resources.getGpu();
        final Map<String, Double> scalars = gpu > 0 ? Collections.singletonMap(RESOURCE_GPU, gpu) : Collections.emptyMap();

        return new MachineDefinition(
                resources.getCpu(),
                resources.getMemoryMB(),
                resources.getNetworkMbps(),
                resources.getDiskMB(),
                0,
                scalars
        );
    }

    private static List<Parameter> toParameterList(JobDescriptor jobDescriptor) {
        Container container = jobDescriptor.getContainer();

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(Parameters.newCmdTypeParameter("titus"));
        parameters.add(Parameters.newNameParameter(jobDescriptor.getApplicationName()));
        parameters.add(Parameters.newAppNameParameter(jobDescriptor.getApplicationName()));
        parameters.add(Parameters.newCapacityGroup(jobDescriptor.getCapacityGroup()));

        Image image = container.getImage();
        parameters.add(Parameters.newImageNameParameter(image.getName()));
        parameters.add(Parameters.newImageDigestParameter(image.getDigest()));
        parameters.add(Parameters.newVersionParameter(image.getTag()));

        // EFS
        if (container.getResources().getEfsMountsCount() > 0) {
            parameters.add(Parameters.newEfsMountsParameter(toV2EfsMounts(container.getResources().getEfsMountsList())));
        }

        parameters.add(Parameters.newEntryPointParameter(StringExt.concatenate(container.getEntryPointList(), " ")));

        if (container.getEnvMap() != null) {
            parameters.add(Parameters.newEnvParameter(container.getEnvMap()));
        }

        String iamRole = container.getSecurityProfile().getIamRole();
        if (isNotEmpty(iamRole)) {
            parameters.add(Parameters.newIamProfileParameter(iamRole));
        }

        switch (jobDescriptor.getJobSpecCase()) {
            case BATCH:
                parameters.add(Parameters.newJobTypeParameter(Parameters.JobType.Batch));
                break;
            case SERVICE:
                parameters.add(Parameters.newJobTypeParameter(Parameters.JobType.Service));
                parameters.add(Parameters.newInServiceParameter(jobDescriptor.getService().getEnabled()));
                MigrationPolicy migrationPolicy = jobDescriptor.getService().getMigrationPolicy();
                if (migrationPolicy != null && migrationPolicy.getPolicyCase() == MigrationPolicy.PolicyCase.SELFMANAGED) {
                    parameters.add(Parameters.newMigrationPolicy(new SelfManagedMigrationPolicy()));
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported job type " + jobDescriptor.getJobSpecCase());
        }

        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        if (jobGroupInfo != null) {
            if (jobGroupInfo.getStack() != null) {
                parameters.add(Parameters.newJobGroupStack(jobGroupInfo.getStack()));
            }
            if (jobGroupInfo.getDetail() != null) {
                parameters.add(Parameters.newJobGroupDetail(jobGroupInfo.getDetail()));
            }
            if (jobGroupInfo.getSequence() != null) {
                parameters.add(Parameters.newJobGroupSequence(jobGroupInfo.getSequence()));
                if (!jobGroupInfo.getSequence().isEmpty()) {
                    parameters.add(Parameters.newJobIdSequence(String.join("-",
                            jobDescriptor.getApplicationName(),
                            jobGroupInfo.getStack(),
                            jobGroupInfo.getDetail(),
                            jobGroupInfo.getSequence()
                    )));
                }
            }
        }

        Map<String, String> labels = new HashMap<>(jobDescriptor.getAttributesMap());
        labels.putAll(container.getSecurityProfile().getAttributesMap());
        parameters.add(Parameters.newLabelsParameter(labels));

        if (container.getSecurityProfile().getSecurityGroupsCount() > 0) {
            parameters.add(Parameters.newSecurityGroupsParameter(container.getSecurityProfile().getSecurityGroupsList()));
        }

        return parameters;
    }

    public static Constraints toGrpcJobConstraintList(List<JobConstraints> constraints) {
        Constraints.Builder builder = Constraints.newBuilder();
        constraints.forEach(c -> builder.putConstraints(c.name(), "true"));
        return builder.build();
    }

    public static List<JobConstraints> toJobConstraintList(Constraints constraints) {
        List<JobConstraints> result = new ArrayList<>();
        constraints.getConstraintsMap().forEach((name, value) -> {
            if (Boolean.valueOf(value)) {
                result.add(toJobConstraint(name));
            }
        });
        return result;
    }

    private static JobConstraints toJobConstraint(String constraint) {
        return JobConstraints.valueOf(constraint);
    }

    private static String getWorkerZone(MasterConfiguration configuration, V2WorkerMetadata mwmd) {
        if (mwmd.getSlave() == null) {
            return null;
        }
        final Map<String, String> slaveAttributes = mwmd.getSlaveAttributes();
        if (!slaveAttributes.isEmpty()) {
            String val = slaveAttributes.get(configuration.getHostZoneAttributeName());
            if (val != null) {
                return val;
            }
        }
        return "Unknown";
    }
}
