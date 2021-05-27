/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.legacy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfos;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.mesos.ContainerInfoUtil;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import io.titanframework.messages.TitanProtos;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobType;
import static com.netflix.titus.common.util.Evaluators.applyNotNull;

@Singleton
public class PodContainerInfoFactory {

    private static final String PASSTHROUGH_ATTRIBUTES_PREFIX = "titusParameter.agent.";

    /**
     * If a user specifies a custom bucket location, the S3 writer role will include the container's IAM role.
     * Otherwise default role will be used which has access to a default S3 bucket.
     */
    static final String S3_WRITER_ROLE = PASSTHROUGH_ATTRIBUTES_PREFIX + "log.s3WriterRole";
    static final String S3_BUCKET_NAME = PASSTHROUGH_ATTRIBUTES_PREFIX + "log.s3BucketName";

    private static final String TITUS_AGENT_ATTRIBUTE_PREFIX = "titus.agent.";
    private static final String OWNER_EMAIL_ATTRIBUTE = TITUS_AGENT_ATTRIBUTE_PREFIX + "ownerEmail";
    private static final String JOB_TYPE_ATTRIBUTE = TITUS_AGENT_ATTRIBUTE_PREFIX + "jobType";
    private static final String JOB_ID_ATTRIBUTE = TITUS_AGENT_ATTRIBUTE_PREFIX + "jobId";
    private static final String APPLICATION_NAME_ATTRIBUTE = TITUS_AGENT_ATTRIBUTE_PREFIX + "applicationName";
    private static final String RUNTIME_PREDICTION_ATTRIBUTE = TITUS_AGENT_ATTRIBUTE_PREFIX + "runtimePredictionSec";
    private static final String RUNTIME_PREDICTIONS_AVAILABLE_ATTRIBUTE = TITUS_AGENT_ATTRIBUTE_PREFIX + "runtimePredictionsAvailable";

    private static final String ARN_PREFIX = "arn:aws:iam::";
    private static final String ARN_SUFFIX = ":role/";
    private static final Pattern IAM_PROFILE_RE = Pattern.compile(ARN_PREFIX + "(\\d+)" + ARN_SUFFIX + "\\S+");

    private final KubePodConfiguration configuration;
    private final LogStorageInfo<Task> logStorageInfo;
    private final String iamArnPrefix;

    @Inject
    public PodContainerInfoFactory(KubePodConfiguration configuration, LogStorageInfo<Task> logStorageInfo) {
        this.configuration = configuration;
        this.logStorageInfo = logStorageInfo;
        String accountId = Evaluators.getOrDefault(System.getenv("EC2_OWNER_ID"), "default");
        this.iamArnPrefix = ARN_PREFIX + accountId + ARN_SUFFIX;
    }

    public TitanProtos.ContainerInfo buildContainerInfo(Job<?> job, Task task, boolean includeEnv) {
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
        containerInfoBuilder.setAllowNestedContainers(true);

        String attributeKillWaitSeconds = containerAttributes.get(JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS);
        Integer killWaitSeconds = attributeKillWaitSeconds == null ? null : Ints.tryParse(attributeKillWaitSeconds);
        if (killWaitSeconds == null || killWaitSeconds < configuration.getMinKillWaitSeconds() || killWaitSeconds > configuration.getMaxKillWaitSeconds()) {
            if (JobFunctions.isBatchJob(job)) {
                killWaitSeconds = configuration.getBatchDefaultKillWaitSeconds();
            } else if (JobFunctions.isServiceJob(job)) {
                killWaitSeconds = configuration.getServiceDefaultKillWaitSeconds();
            } else {
                killWaitSeconds = configuration.getMinKillWaitSeconds();
            }
        }
        containerInfoBuilder.setKillWaitSeconds(killWaitSeconds);

        // Send passthrough attributes that begin with the agent prefix
        containerAttributes.forEach((k, v) -> {
            if (k.startsWith(PASSTHROUGH_ATTRIBUTES_PREFIX)) {
                containerInfoBuilder.putPassthroughAttributes(k, v);
            }
        });
        appendS3WriterRole(containerInfoBuilder, job, task);

        containerInfoBuilder.putPassthroughAttributes(OWNER_EMAIL_ATTRIBUTE, jobDescriptor.getOwner().getTeamEmail());
        containerInfoBuilder.putPassthroughAttributes(JOB_TYPE_ATTRIBUTE, getJobType(jobDescriptor).name());
        containerInfoBuilder.putPassthroughAttributes(JOB_ID_ATTRIBUTE, job.getId());
        containerInfoBuilder.putPassthroughAttributes(APPLICATION_NAME_ATTRIBUTE, jobDescriptor.getApplicationName());
        Evaluators.acceptNotNull(jobAttributes.get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC),
                v -> containerInfoBuilder.putPassthroughAttributes(RUNTIME_PREDICTION_ATTRIBUTE, v)
        );
        Evaluators.acceptNotNull(jobAttributes.get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE),
                v -> containerInfoBuilder.putPassthroughAttributes(RUNTIME_PREDICTIONS_AVAILABLE_ATTRIBUTE, v)
        );

        if (includeEnv) {
            ContainerInfoUtil.setContainerInfoEnvVariables(containerInfoBuilder, container, task);
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

    @VisibleForTesting
    void appendS3WriterRole(TitanProtos.ContainerInfo.Builder containerInfoBuilder, Job<?> job, Task task) {
        if (!configuration.isDefaultS3WriterRoleEnabled()) {
            return;
        }

        if (LogStorageInfos.findCustomS3Bucket(job).isPresent()) {
            containerInfoBuilder.putPassthroughAttributes(
                    S3_WRITER_ROLE,
                    job.getJobDescriptor().getContainer().getSecurityProfile().getIamRole()
            );
        } else {
            Evaluators.applyNotNull(
                    configuration.getDefaultS3WriterRole(),
                    role -> containerInfoBuilder.putPassthroughAttributes(S3_WRITER_ROLE, role)
            );
        }

        logStorageInfo.getS3LogLocation(task, false).ifPresent(s3LogLocation ->
                Evaluators.applyNotNull(
                        s3LogLocation.getBucket(),
                        bucket -> containerInfoBuilder.putPassthroughAttributes(S3_BUCKET_NAME, bucket)
                )
        );
    }
}
