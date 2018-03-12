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

package io.netflix.titus.master.jobmanager.service.common;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.TaskRequest;
import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Container;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import io.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.common.util.Evaluators;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.worker.WorkerRequest;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.TaskInfoFactory;
import io.netflix.titus.master.model.job.TitusQueuableTask;
import io.titanframework.messages.TitanProtos;
import io.titanframework.messages.TitanProtos.ContainerInfo.EfsConfigInfo;
import org.apache.mesos.Protos;

import static io.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_ALLOW_NESTED_CONTAINERS;
import static io.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_ALLOW_NETWORK_BURSTING;
import static io.netflix.titus.common.util.Evaluators.acceptIfTrue;
import static io.netflix.titus.common.util.Evaluators.applyNotNull;

/**
 * Converts Titus Models into the Mesos proto objects
 */
@Singleton
public class DefaultV3TaskInfoFactory implements TaskInfoFactory<Protos.TaskInfo> {

    private static final String EXECUTOR_PER_TASK_LABEL = "executorpertask";
    private static final String LEGACY_EXECUTOR_NAME = "docker-executor";
    private static final String EXECUTOR_PER_TASK_EXECUTOR_NAME = "docker-per-task-executor";

    private static final String ARN_PREFIX = "arn:aws:iam::";
    private static final String ARN_SUFFIX = ":role/";
    private static final Pattern IAM_PROFILE_RE = Pattern.compile(ARN_PREFIX + "(\\d+)" + ARN_SUFFIX + "\\S+");

    private final MasterConfiguration config;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final String iamArnPrefix;

    @Inject
    public DefaultV3TaskInfoFactory(MasterConfiguration config,
                                    JobManagerConfiguration jobManagerConfiguration) {
        this.config = config;
        this.jobManagerConfiguration = jobManagerConfiguration;
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
                                       PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult) {
        String taskId = task.getId();
        Protos.TaskID protoTaskId = Protos.TaskID.newBuilder().setValue(taskId).build();

        Protos.CommandInfo commandInfo = Protos.CommandInfo.newBuilder().setValue(config.pathToTitusExecutor()).build();
        Protos.ExecutorInfo executorInfo = newExecutorInfo(taskId, attributesMap, commandInfo);
        Protos.TaskInfo.Builder taskInfoBuilder = newTaskInfoBuilder(protoTaskId, executorInfo, slaveID);
        taskInfoBuilder = setupPrimaryResources(taskInfoBuilder, fenzoTask);

        TitanProtos.ContainerInfo.Builder containerInfoBuilder = newContainerInfoBuilder(job, task, fenzoTask);
        taskInfoBuilder.setData(containerInfoBuilder.build().toByteString());
        return taskInfoBuilder.build();
    }

    private TitanProtos.ContainerInfo.Builder newContainerInfoBuilder(Job job, Task task, TitusQueuableTask<Job, Task> fenzoTask) {
        TitanProtos.ContainerInfo.Builder containerInfoBuilder = TitanProtos.ContainerInfo.newBuilder();
        Container container = job.getJobDescriptor().getContainer();
        Map<String, String> attributes = container.getAttributes();
        ContainerResources containerResources = container.getContainerResources();
        SecurityProfile v3SecurityProfile = container.getSecurityProfile();

        // Docker Values (Image and Entrypoint)
        Image image = container.getImage();
        containerInfoBuilder.setImageName(image.getName());
        applyNotNull(image.getDigest(), containerInfoBuilder::setImageDigest);
        applyNotNull(image.getTag(), containerInfoBuilder::setVersion);
        containerInfoBuilder.setEntrypointStr(StringExt.concatenate(container.getEntryPoint(), " "));

        // Netflix Values
        // Configure Netflix Metadata
        containerInfoBuilder.setAppName(job.getJobDescriptor().getApplicationName());
        JobGroupInfo jobGroupInfo = job.getJobDescriptor().getJobGroupInfo();
        if (jobGroupInfo != null) {
            applyNotNull(jobGroupInfo.getStack(), containerInfoBuilder::setJobGroupStack);
            applyNotNull(jobGroupInfo.getDetail(), containerInfoBuilder::setJobGroupDetail);
            applyNotNull(jobGroupInfo.getSequence(), containerInfoBuilder::setJobGroupSequence);
        }

        // Configure Metatron
        String metatronAppMetadata = v3SecurityProfile.getAttributes().get(WorkerRequest.V2_NETFLIX_APP_METADATA);
        String metatronAppSignature = v3SecurityProfile.getAttributes().get(WorkerRequest.V2_NETFLIX_APP_METADATA_SIG);
        if (metatronAppMetadata != null && metatronAppSignature != null) {
            TitanProtos.ContainerInfo.MetatronCreds.Builder metatronBuilder = TitanProtos.ContainerInfo.MetatronCreds.newBuilder()
                    .setAppMetadata(metatronAppMetadata)
                    .setMetadataSig(metatronAppSignature);
            containerInfoBuilder.setMetatronCreds(metatronBuilder.build());
        }

        // Configure attribute features
        acceptIfTrue(attributes.get(JOB_ATTRIBUTES_ALLOW_NETWORK_BURSTING), containerInfoBuilder::setAllowNetworkBursting);
        acceptIfTrue(attributes.get(JOB_ATTRIBUTES_ALLOW_NESTED_CONTAINERS), containerInfoBuilder::setAllowNestedContainers);

        // Configure Environment Variables
        Map<String, String> userProvidedEnv = container.getEnv().entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        containerInfoBuilder.putAllUserProvidedEnv(userProvidedEnv);
        containerInfoBuilder.putTitusProvidedEnv("TITUS_JOB_ID", task.getJobId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INSTANCE_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ORIGINAL_ID", task.getOriginalId());
        if (task instanceof BatchJobTask) {
            BatchJobTask batchJobTask = (BatchJobTask) task;
            containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INDEX", "" + batchJobTask.getIndex());
        }

        // Set whether or not to ignore the launch guard
        if (jobManagerConfiguration.isV3IgnoreLaunchGuardEnabled()) {
            containerInfoBuilder.setIgnoreLaunchGuard(true);
        }

        // AWS Values
        // Configure IAM Role
        Evaluators.acceptNotNull(v3SecurityProfile.getIamRole(), iam -> {
            String qualifiedIam = IAM_PROFILE_RE.matcher(iam).matches() ? iam : iamArnPrefix + iam;
            containerInfoBuilder.setIamProfile(qualifiedIam);
        });

        // Configure ENI (IP Address, SGs)
        containerInfoBuilder.setAllocateIpAddress(containerResources.isAllocateIP());

        List<String> securityGroups = v3SecurityProfile.getSecurityGroups();
        final TaskRequest.AssignedResources assignedResources = fenzoTask.getAssignedResources();
        String eniLabel = assignedResources == null ? "0" : "" + assignedResources.getConsumedNamedResources().get(0).getIndex();
        TitanProtos.ContainerInfo.NetworkConfigInfo.Builder networkConfigInfoBuilder = TitanProtos.ContainerInfo.NetworkConfigInfo.newBuilder()
                .setAllocateIpAddress(containerResources.isAllocateIP())
                .setEniLabel(eniLabel)
                .setEniLablel(eniLabel)
                .addAllSecurityGroups(securityGroups)
                .setBandwidthLimitMbps((int) fenzoTask.getNetworkMbps());

        containerInfoBuilder.setNetworkConfigInfo(networkConfigInfoBuilder.build());

        // Configure GPU
        containerInfoBuilder.setNumGpus(containerResources.getGpu());

        // Configure EFS
        containerInfoBuilder.addAllEfsConfigInfo(setupEfsMounts(containerResources.getEfsMounts()));

        return containerInfoBuilder;
    }

    private Protos.TaskInfo.Builder newTaskInfoBuilder(Protos.TaskID taskId, Protos.ExecutorInfo executorInfo, Protos.SlaveID slaveID) {
        return Protos.TaskInfo.newBuilder()
                .setTaskId(taskId)
                .setName(taskId.getValue())
                .setExecutor(executorInfo)
                .setSlaveId(slaveID);
    }

    private Protos.TaskInfo.Builder setupPrimaryResources(Protos.TaskInfo.Builder builder, TitusQueuableTask<Job, Task> fenzoTask) {
        return builder.addResources(Protos.Resource.newBuilder()
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
    }

    private Protos.ExecutorInfo newExecutorInfo(String taskId,
                                                Map<String, String> attributesMap,
                                                Protos.CommandInfo commandInfo) {

        boolean executorPerTask = attributesMap.containsKey(EXECUTOR_PER_TASK_LABEL);
        String executorName = LEGACY_EXECUTOR_NAME;
        String executorId = LEGACY_EXECUTOR_NAME;
        if (executorPerTask) {
            executorName = EXECUTOR_PER_TASK_EXECUTOR_NAME;
            executorId = EXECUTOR_PER_TASK_EXECUTOR_NAME + "-" + taskId;
        }

        return Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(executorId).build())
                .setName(executorName)
                .setCommand(commandInfo)
                .build();
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
