/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver.direct;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.titanframework.messages.TitanProtos;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NESTED_CONTAINERS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobType;
import static com.netflix.titus.common.util.Evaluators.applyNotNull;

@Singleton
public class DefaultTaskToPodConverter implements TaskToPodConverter {

    private static final String PASSTHROUGH_ATTRIBUTES_PREFIX = "titusParameter.agent.";
    private static final String OWNER_EMAIL_ATTRIBUTE = "titus.agent.ownerEmail";
    private static final String JOB_TYPE_ATTRIBUTE = "titus.agent.jobType";
    private static final String RUNTIME_PREDICTION_ATTRIBUTE = "titus.agent.runtimePredictionSec";
    private static final String RUNTIME_PREDICTIONS_AVAILABLE_ATTRIBUTE = "titus.agent.runtimePredictionsAvailable";

    private static final long POD_TERMINATION_GRACE_PERIOD_SECONDS = 600L;
    private static final String NEVER_RESTART_POLICY = "Never";

    private static final String ARN_PREFIX = "arn:aws:iam::";
    private static final String ARN_SUFFIX = ":role/";
    private static final Pattern IAM_PROFILE_RE = Pattern.compile(ARN_PREFIX + "(\\d+)" + ARN_SUFFIX + "\\S+");

    private final DirectKubeConfiguration configuration;
    private final String iamArnPrefix;

    @Inject
    public DefaultTaskToPodConverter(DirectKubeConfiguration configuration) {
        this.configuration = configuration;

        // Get the AWS account ID to use for building IAM ARNs.
        String accountId = Evaluators.getOrDefault(System.getenv("EC2_OWNER_ID"), "default");
        this.iamArnPrefix = ARN_PREFIX + accountId + ARN_SUFFIX;
    }

    @Override
    public V1Pod apply(Job<?> job, Task task) {

        String taskId = task.getId();
        TitanProtos.ContainerInfo containerInfo = buildContainerInfo(job, task);
        String encodedContainerInfo = Base64.getEncoder().encodeToString(containerInfo.toByteArray());

        Map<String, String> annotations = new HashMap<>();
        annotations.put("containerInfo", encodedContainerInfo);

        V1ObjectMeta metadata = new V1ObjectMeta()
                .name(taskId)
                .annotations(annotations);

        V1Container container = new V1Container()
                .name(taskId)
                .image("imageIsInContainerInfo")
                .resources(buildV1ResourceRequirements(job.getJobDescriptor().getContainer().getContainerResources()));

        V1PodSpec spec = new V1PodSpec()
                .schedulerName("default-scheduler")
                .containers(Collections.singletonList(container))
                .terminationGracePeriodSeconds(POD_TERMINATION_GRACE_PERIOD_SECONDS)
                .restartPolicy(NEVER_RESTART_POLICY);

        return new V1Pod().metadata(metadata).spec(spec);
    }

    private V1ResourceRequirements buildV1ResourceRequirements(ContainerResources containerResources) {
        Map<String, Quantity> requests = new HashMap<>();
        Map<String, Quantity> limits = new HashMap<>();

        requests.put("cpu", new Quantity(String.valueOf(containerResources.getCpu())));
        limits.put("cpu", new Quantity(String.valueOf(containerResources.getCpu())));

        requests.put("memory", new Quantity(String.valueOf(containerResources.getMemoryMB())));
        limits.put("memory", new Quantity(String.valueOf(containerResources.getMemoryMB())));

        requests.put("ephemeral-storage", new Quantity(String.valueOf(containerResources.getDiskMB())));
        limits.put("ephemeral-storage", new Quantity(String.valueOf(containerResources.getDiskMB())));

        requests.put("titus/network", new Quantity(String.valueOf(containerResources.getNetworkMbps())));
        limits.put("titus/network", new Quantity(String.valueOf(containerResources.getNetworkMbps())));

//        requests.put("titus/gpu", new Quantity(String.valueOf(containerResources.getGpu())));
//        limits.put("titus/gpu", new Quantity(String.valueOf(containerResources.getGpu())));

        return new V1ResourceRequirements().requests(requests).limits(limits);
    }

    private TitanProtos.ContainerInfo buildContainerInfo(Job<?> job, Task task) {
        JobDescriptor<?> jobDescriptor = job.getJobDescriptor();
        Map<String, String> jobAttributes = jobDescriptor.getAttributes();
        TitanProtos.ContainerInfo.Builder containerInfoBuilder = TitanProtos.ContainerInfo.newBuilder();
        Container container = jobDescriptor.getContainer();
        Map<String, String> containerAttributes = container.getAttributes();
        ContainerResources containerResources = container.getContainerResources();
        SecurityProfile v3SecurityProfile = container.getSecurityProfile();

        // Docker Values (Image, entrypoint, and command)
        setImage(containerInfoBuilder, container.getImage());
        setEntryPointCommand(containerInfoBuilder, container, jobAttributes);

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
            TitanProtos.ContainerInfo.MetatronCreds.Builder metatronBuilder = TitanProtos.ContainerInfo.MetatronCreds.newBuilder()
                    .setAppMetadata(metatronAppMetadata)
                    .setMetadataSig(metatronAppSignature);
            containerInfoBuilder.setMetatronCreds(metatronBuilder.build());
        }

        // Configure agent job attributes
        containerInfoBuilder.setAllowCpuBursting(Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING)));
        containerInfoBuilder.setAllowNetworkBursting(Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING)));
        containerInfoBuilder.setBatch(Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH)));

        boolean allowNestedContainers = configuration.isNestedContainersEnabled() && Boolean.parseBoolean(containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_ALLOW_NESTED_CONTAINERS));
        containerInfoBuilder.setAllowNestedContainers(allowNestedContainers);

        String attributeKillWaitSeconds = containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS);
        Integer killWaitSeconds = attributeKillWaitSeconds == null ? null : Ints.tryParse(attributeKillWaitSeconds);
        if (killWaitSeconds == null || killWaitSeconds < configuration.getMinKillWaitSeconds() || killWaitSeconds > configuration.getMaxKillWaitSeconds()) {
            killWaitSeconds = configuration.getDefaultKillWaitSeconds();
        }
        containerInfoBuilder.setKillWaitSeconds(killWaitSeconds);

        // Send passthrough attributes that begin with the agent prefix
        containerAttributes.forEach((k, v) -> {
            if (k.startsWith(PASSTHROUGH_ATTRIBUTES_PREFIX)) {
                containerInfoBuilder.putPassthroughAttributes(k, v);
            }
        });

        containerInfoBuilder.putPassthroughAttributes(OWNER_EMAIL_ATTRIBUTE, jobDescriptor.getOwner().getTeamEmail());
        containerInfoBuilder.putPassthroughAttributes(JOB_TYPE_ATTRIBUTE, getJobType(jobDescriptor).name());
        Evaluators.acceptNotNull(jobAttributes.get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC),
                v -> containerInfoBuilder.putPassthroughAttributes(RUNTIME_PREDICTION_ATTRIBUTE, v)
        );
        Evaluators.acceptNotNull(jobAttributes.get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE),
                v -> containerInfoBuilder.putPassthroughAttributes(RUNTIME_PREDICTIONS_AVAILABLE_ATTRIBUTE, v)
        );

        // Configure Environment Variables
        container.getEnv().forEach((k, v) -> {
            if (v != null) {
                containerInfoBuilder.putUserProvidedEnv(k, v);
            }
        });

        containerInfoBuilder.putTitusProvidedEnv("TITUS_JOB_ID", task.getJobId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("NETFLIX_EXECUTOR", "titus");
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

        // Configure ENI (IP Address, SGs). ENI management is done by Titus Agent, so we set default value.
        List<String> securityGroups = v3SecurityProfile.getSecurityGroups();
        TitanProtos.ContainerInfo.NetworkConfigInfo.Builder networkConfigInfoBuilder = TitanProtos.ContainerInfo.NetworkConfigInfo.newBuilder()
                .setEniLabel("0")
                .setEniLablel("0")
                .addAllSecurityGroups(securityGroups)
                .setBandwidthLimitMbps(job.getJobDescriptor().getContainer().getContainerResources().getNetworkMbps());

        containerInfoBuilder.setNetworkConfigInfo(networkConfigInfoBuilder.build());

        // Configure GPU
        containerInfoBuilder.setNumGpus(containerResources.getGpu());

        // Configure EFS
        containerInfoBuilder.addAllEfsConfigInfo(setupEfsMounts(containerResources.getEfsMounts()));

        // Configure shared memory size
        containerInfoBuilder.setShmSizeMB(containerResources.getShmMB());

        // Configure IP address allocation
        setSignedAddressAllocation(containerInfoBuilder, task, containerResources);

        // Configure job accepted timestamp
        setJobAcceptedTimestamp(containerInfoBuilder, job);

        return containerInfoBuilder.build();
    }

    private void setImage(TitanProtos.ContainerInfo.Builder containerInfoBuilder, Image image) {
        containerInfoBuilder.setImageName(image.getName());
        String registryUrl = configuration.getRegistryUrl();
        if (!Strings.isNullOrEmpty(registryUrl)) {
            String updatedRegistryUrl = StringExt.appendToEndIfMissing(registryUrl, "/");
            String fullQualifiedImage = updatedRegistryUrl + image.getName();
            containerInfoBuilder.setFullyQualifiedImage(fullQualifiedImage);
        }
        applyNotNull(image.getDigest(), containerInfoBuilder::setImageDigest);
        applyNotNull(image.getTag(), containerInfoBuilder::setVersion);
    }

    private void setEntryPointCommand(TitanProtos.ContainerInfo.Builder containerInfoBuilder, Container container, Map<String, String> jobAttributes) {
        if (CollectionsExt.isNullOrEmpty(container.getCommand()) && !shouldSkipEntryPointJoin(jobAttributes)) {
            // fallback to the old behavior when no command is set to avoid breaking existing jobs relying on shell
            // parsing and word splitting being done by the executor for flat string entrypoints
            containerInfoBuilder.setEntrypointStr(StringExt.concatenate(container.getEntryPoint(), " "));
            return;
        }
        containerInfoBuilder.setProcess(TitanProtos.ContainerInfo.Process.newBuilder()
                .addAllEntrypoint(container.getEntryPoint())
                .addAllCommand(container.getCommand())
        );
    }

    private boolean shouldSkipEntryPointJoin(Map<String, String> jobAttributes) {
        return Boolean.parseBoolean(jobAttributes.getOrDefault(JobAttributes.JOB_PARAMETER_ATTRIBUTES_ENTRY_POINT_SKIP_SHELL_PARSING,
                "false").trim());

    }

    private void setSignedAddressAllocation(TitanProtos.ContainerInfo.Builder containerInfoBuilder, Task task, ContainerResources containerResources) {
        if (task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID)) {
            String addressAllocationId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID);
            for (SignedIpAddressAllocation signedIpAddressAllocation : containerResources.getSignedIpAddressAllocations()) {
                if (signedIpAddressAllocation.getIpAddressAllocation().getAllocationId().equals(addressAllocationId)) {
                    containerInfoBuilder.setSignedAddressAllocation(GrpcJobManagementModelConverters.toGrpcSignedAddressAllocation(signedIpAddressAllocation));
                    break;
                }
            }
        }
    }

    private void setJobAcceptedTimestamp(TitanProtos.ContainerInfo.Builder containerInfoBuilder, Job<?> job) {
        JobFunctions.findJobStatus(job, JobState.Accepted).ifPresent(jobStatus -> containerInfoBuilder.setJobAcceptedTimestampMs(jobStatus.getTimestamp()));
    }

    private List<TitanProtos.ContainerInfo.EfsConfigInfo> setupEfsMounts(List<EfsMount> efsMounts) {
        if (efsMounts.isEmpty()) {
            return Collections.emptyList();
        }
        return efsMounts.stream().map(efsMount -> TitanProtos.ContainerInfo.EfsConfigInfo.newBuilder()
                .setEfsFsId(efsMount.getEfsId())
                .setMntPerms(TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms.valueOf(efsMount.getMountPerm().name()))
                .setMountPoint(efsMount.getMountPoint())
                .setEfsFsRelativeMntPoint(efsMount.getEfsRelativeMountPoint())
                .build()
        ).collect(Collectors.toList());
    }
}
