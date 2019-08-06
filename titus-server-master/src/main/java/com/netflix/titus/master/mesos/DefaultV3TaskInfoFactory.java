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

package com.netflix.titus.master.mesos;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.model.job.TitusQueuableTask;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.titanframework.messages.TitanProtos.ContainerInfo;
import io.titanframework.messages.TitanProtos.ContainerInfo.EfsConfigInfo;
import org.apache.mesos.Protos;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NESTED_CONTAINERS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobType;
import static com.netflix.titus.common.util.Evaluators.applyNotNull;

/**
 * Converts Titus Models into the Mesos proto objects
 */
@Singleton
public class DefaultV3TaskInfoFactory implements TaskInfoFactory<Protos.TaskInfo> {

    private static final String PASSTHROUGH_ATTRIBUTES_PREFIX = "titusParameter.agent.";
    private static final String OWNER_EMAIL_ATTRIBUTE = "titus.agent.ownerEmail";
    private static final String JOB_TYPE_ATTRIBUTE = "titus.agent.jobType";
    private static final String EXECUTOR_PER_TASK_LABEL = "executorpertask";
    private static final String LEGACY_EXECUTOR_NAME = "docker-executor";
    private static final String EXECUTOR_PER_TASK_EXECUTOR_NAME = "docker-per-task-executor";

    private static final String ARN_PREFIX = "arn:aws:iam::";
    private static final String ARN_SUFFIX = ":role/";
    private static final Pattern IAM_PROFILE_RE = Pattern.compile(ARN_PREFIX + "(\\d+)" + ARN_SUFFIX + "\\S+");

    private final MasterConfiguration masterConfiguration;
    private final MesosConfiguration mesosConfiguration;
    private final String iamArnPrefix;

    @Inject
    public DefaultV3TaskInfoFactory(MasterConfiguration masterConfiguration,
                                    MesosConfiguration mesosConfiguration) {
        this.masterConfiguration = masterConfiguration;
        this.mesosConfiguration = mesosConfiguration;
        // Get the AWS account ID to use for building IAM ARNs.
        String accountId = Evaluators.getOrDefault(System.getenv("EC2_OWNER_ID"), "default");
        this.iamArnPrefix = ARN_PREFIX + accountId + ARN_SUFFIX;
    }

    @Override
    public Protos.TaskInfo newTaskInfo(TitusQueuableTask<Job, Task> fenzoTask,
                                       Job<?> job,
                                       Task task,
                                       String hostname,
                                       Map<String, String> attributesMap,
                                       Protos.SlaveID slaveID,
                                       PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult,
                                       Optional<String> executorUriOverrideOpt) {
        String taskId = task.getId();
        Protos.TaskID protoTaskId = Protos.TaskID.newBuilder().setValue(taskId).build();
        Protos.ExecutorInfo executorInfo = newExecutorInfo(task, attributesMap, executorUriOverrideOpt);
        Protos.TaskInfo.Builder taskInfoBuilder = newTaskInfoBuilder(protoTaskId, executorInfo, slaveID, fenzoTask);
        ContainerInfo.Builder containerInfoBuilder = newContainerInfoBuilder(job, task, fenzoTask);
        taskInfoBuilder.setData(containerInfoBuilder.build().toByteString());
        return taskInfoBuilder.build();
    }

    private ContainerInfo.Builder newContainerInfoBuilder(Job job, Task task, TitusQueuableTask<Job, Task> fenzoTask) {
        JobDescriptor jobDescriptor = job.getJobDescriptor();
        ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
        Container container = jobDescriptor.getContainer();
        Map<String, String> containerAttributes = container.getAttributes();
        ContainerResources containerResources = container.getContainerResources();
        SecurityProfile v3SecurityProfile = container.getSecurityProfile();

        // Docker Values (Image, entrypoint, and command)
        setImage(containerInfoBuilder, container.getImage());
        setEntryPointCommand(containerInfoBuilder, container);

        // Netflix Values
        // Configure Netflix Metadata
        containerInfoBuilder.setAppName(jobDescriptor.getApplicationName());
        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        if (jobGroupInfo != null) {
            applyNotNull(jobGroupInfo.getStack(), containerInfoBuilder::setJobGroupStack);
            applyNotNull(jobGroupInfo.getDetail(), containerInfoBuilder::setJobGroupDetail);
            applyNotNull(jobGroupInfo.getSequence(), containerInfoBuilder::setJobGroupSequence);
        }

        // Configure Metatron
        String metatronAppMetadata = v3SecurityProfile.getAttributes().get(Container.ATTRIBUTE_NETFLIX_APP_METADATA);
        String metatronAppSignature = v3SecurityProfile.getAttributes().get(Container.ATTRIBUTE_NETFLIX_APP_METADATA_SIG);
        if (metatronAppMetadata != null && metatronAppSignature != null) {
            ContainerInfo.MetatronCreds.Builder metatronBuilder = ContainerInfo.MetatronCreds.newBuilder()
                    .setAppMetadata(metatronAppMetadata)
                    .setMetadataSig(metatronAppSignature);
            containerInfoBuilder.setMetatronCreds(metatronBuilder.build());
        }

        // Configure agent job attributes
        containerInfoBuilder.setAllowCpuBursting(Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING)));
        containerInfoBuilder.setAllowNetworkBursting(Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING)));
        containerInfoBuilder.setBatch(Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH)));

        boolean allowNestedContainers = mesosConfiguration.isNestedContainersEnabled() && Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_ALLOW_NESTED_CONTAINERS));
        containerInfoBuilder.setAllowNestedContainers(allowNestedContainers);

        String attributeKillWaitSeconds = containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS);
        Integer killWaitSeconds = attributeKillWaitSeconds == null ? null : Ints.tryParse(attributeKillWaitSeconds);
        if (killWaitSeconds == null || killWaitSeconds < mesosConfiguration.getMinKillWaitSeconds() || killWaitSeconds > mesosConfiguration.getMaxKillWaitSeconds()) {
            killWaitSeconds = mesosConfiguration.getDefaultKillWaitSeconds();
        }
        containerInfoBuilder.setKillWaitSeconds(killWaitSeconds);

        // Send passthrough attributes that begin with the agent prefix
        containerAttributes.forEach((k, v) -> {
            if (k.startsWith(PASSTHROUGH_ATTRIBUTES_PREFIX)) {
                containerInfoBuilder.putPassthroughAttributes(k, v);
            }
        });

        // Add owner email
        containerInfoBuilder.putPassthroughAttributes(OWNER_EMAIL_ATTRIBUTE, jobDescriptor.getOwner().getTeamEmail());

        // Add job type
        containerInfoBuilder.putPassthroughAttributes(JOB_TYPE_ATTRIBUTE, getJobType(jobDescriptor).name());

        // Configure Environment Variables
        container.getEnv().forEach((k, v) -> {
            if (v != null) {
                containerInfoBuilder.putUserProvidedEnv(k, v);
            }
        });

        containerInfoBuilder.putTitusProvidedEnv("TITUS_JOB_ID", task.getJobId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("NETFLIX_INSTANCE_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INSTANCE_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ORIGINAL_ID", task.getOriginalId());
        if (task instanceof BatchJobTask) {
            BatchJobTask batchJobTask = (BatchJobTask) task;
            containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INDEX", "" + batchJobTask.getIndex());
        }

        // Always set this to true until it is removed from the executor
        containerInfoBuilder.setIgnoreLaunchGuard(true);

        // AWS Values
        // Configure IAM Role
        Evaluators.acceptNotNull(v3SecurityProfile.getIamRole(), iam -> {
            String qualifiedIam = IAM_PROFILE_RE.matcher(iam).matches() ? iam : iamArnPrefix + iam;
            containerInfoBuilder.setIamProfile(qualifiedIam);
        });

        // Configure ENI (IP Address, SGs)
        List<String> securityGroups = v3SecurityProfile.getSecurityGroups();
        final TaskRequest.AssignedResources assignedResources = fenzoTask.getAssignedResources();
        String eniLabel = assignedResources == null ? "0" : "" + assignedResources.getConsumedNamedResources().get(0).getIndex();
        ContainerInfo.NetworkConfigInfo.Builder networkConfigInfoBuilder = ContainerInfo.NetworkConfigInfo.newBuilder()
                .setEniLabel(eniLabel)
                .setEniLablel(eniLabel)
                .addAllSecurityGroups(securityGroups)
                .setBandwidthLimitMbps((int) fenzoTask.getNetworkMbps());

        containerInfoBuilder.setNetworkConfigInfo(networkConfigInfoBuilder.build());

        // Configure GPU
        containerInfoBuilder.setNumGpus(containerResources.getGpu());

        // Configure EFS
        containerInfoBuilder.addAllEfsConfigInfo(setupEfsMounts(containerResources.getEfsMounts()));

        // Configure shared memory size
        containerInfoBuilder.setShmSizeMB(containerResources.getShmMB());

        // Configure IP address allocation
        setSignedAddressAllocation(containerInfoBuilder, task, containerResources);

        return containerInfoBuilder;
    }

    private void setImage(ContainerInfo.Builder containerInfoBuilder, Image image) {
        containerInfoBuilder.setImageName(image.getName());
        String registryUrl = mesosConfiguration.getRegistryUrl();
        if (!Strings.isNullOrEmpty(registryUrl)) {
            String updatedRegistryUrl = StringExt.appendToEndIfMissing(registryUrl, "/");
            String fullQualifiedImage = updatedRegistryUrl + image.getName();
            containerInfoBuilder.setFullyQualifiedImage(fullQualifiedImage);
        }
        applyNotNull(image.getDigest(), containerInfoBuilder::setImageDigest);
        applyNotNull(image.getTag(), containerInfoBuilder::setVersion);
    }

    private void setEntryPointCommand(ContainerInfo.Builder containerInfoBuilder, Container container) {
        if (CollectionsExt.isNullOrEmpty(container.getCommand())) {
            // fallback to the old behavior when no command is set to avoid breaking existing jobs relying on shell
            // parsing and word splitting being done by the executor for flat string entrypoints
            containerInfoBuilder.setEntrypointStr(StringExt.concatenate(container.getEntryPoint(), " "));
            return;
        }
        containerInfoBuilder.setProcess(ContainerInfo.Process.newBuilder()
                .addAllEntrypoint(container.getEntryPoint())
                .addAllCommand(container.getCommand())
        );
    }

    private void setSignedAddressAllocation(ContainerInfo.Builder containerInfoBuilder, Task task, ContainerResources containerResources) {
        if (task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID)) {
            String addressAllocationId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID);
            for (SignedIpAddressAllocation signedIpAddressAllocation : containerResources.getSignedIpAddressAllocations()) {
                if (signedIpAddressAllocation.getIpAddressAllocation().getAllocationId().equals(addressAllocationId)) {
                    containerInfoBuilder.setSignedAddressAllocation(V3GrpcModelConverters.toGrpcSignedAddressAllocation(signedIpAddressAllocation));
                    break;
                }
            }
        }
    }

    private Protos.TaskInfo.Builder newTaskInfoBuilder(Protos.TaskID taskId,
                                                       Protos.ExecutorInfo executorInfo,
                                                       Protos.SlaveID slaveID,
                                                       TitusQueuableTask<Job, Task> fenzoTask) {

        Protos.TaskInfo.Builder builder = Protos.TaskInfo.newBuilder()
                .setTaskId(taskId)
                .setName(taskId.getValue())
                .setExecutor(executorInfo)
                .setSlaveId(slaveID)
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getCPUs()).build()))
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getMemory()).build()))
                .addResources(Protos.Resource.newBuilder()
                        .setName("disk")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getDisk()).build()))
                .addResources(Protos.Resource.newBuilder()
                        .setName("network")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getNetworkMbps())));

        // set scalars other than cpus, mem, disk
        final Map<String, Double> scalars = fenzoTask.getScalarRequests();
        if (scalars != null && !scalars.isEmpty()) {
            for (Map.Entry<String, Double> entry : scalars.entrySet()) {
                if (!Container.PRIMARY_RESOURCES.contains(entry.getKey())) { // Already set above
                    builder.addResources(Protos.Resource.newBuilder()
                            .setName(entry.getKey())
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder().setValue(entry.getValue()).build())
                    );
                }
            }
        }

        return builder;
    }

    private Protos.ExecutorInfo newExecutorInfo(Task task,
                                                Map<String, String> attributesMap,
                                                Optional<String> executorUriOverrideOpt) {

        boolean executorPerTask = attributesMap.containsKey(EXECUTOR_PER_TASK_LABEL);
        String executorName = LEGACY_EXECUTOR_NAME;
        String executorId = LEGACY_EXECUTOR_NAME;
        if (executorPerTask) {
            executorName = EXECUTOR_PER_TASK_EXECUTOR_NAME;
            executorId = EXECUTOR_PER_TASK_EXECUTOR_NAME + "-" + task.getId();
        }

        Protos.CommandInfo commandInfo = newCommandInfo(executorPerTask, executorUriOverrideOpt);
        return Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(executorId).build())
                .setName(executorName)
                .setCommand(commandInfo)
                .build();
    }

    private Protos.CommandInfo newCommandInfo(boolean executorPerTask, Optional<String> executorUriOverrideOpt) {
        Protos.CommandInfo.URI.Builder uriBuilder = Protos.CommandInfo.URI.newBuilder();
        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder();

        if (executorPerTask && mesosConfiguration.isExecutorUriOverrideEnabled() && executorUriOverrideOpt.isPresent()) {
            commandInfoBuilder.setShell(false);
            commandInfoBuilder.setValue(mesosConfiguration.getExecutorUriOverrideCommand());
            uriBuilder.setValue(executorUriOverrideOpt.get());
            uriBuilder.setExtract(true);
            uriBuilder.setCache(true);
            commandInfoBuilder.addUris(uriBuilder.build());
        } else {
            commandInfoBuilder.setValue(masterConfiguration.pathToTitusExecutor());
        }

        return commandInfoBuilder.build();
    }

    private List<EfsConfigInfo> setupEfsMounts(List<EfsMount> efsMounts) {
        if (efsMounts.isEmpty()) {
            return Collections.emptyList();
        }
        return efsMounts.stream().map(efsMount -> EfsConfigInfo.newBuilder()
                .setEfsFsId(efsMount.getEfsId())
                .setMntPerms(EfsConfigInfo.MountPerms.valueOf(efsMount.getMountPerm().name()))
                .setMountPoint(efsMount.getMountPoint())
                .setEfsFsRelativeMntPoint(efsMount.getEfsRelativeMountPoint())
                .build()
        ).collect(Collectors.toList());
    }
}
