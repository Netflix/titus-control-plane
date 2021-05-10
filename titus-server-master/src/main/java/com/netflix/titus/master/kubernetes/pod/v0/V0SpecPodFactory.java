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

package com.netflix.titus.master.kubernetes.pod.v0;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.JobConstraints;
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
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolumeUtils;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.KubePodUtil;
import com.netflix.titus.master.kubernetes.pod.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.PodFactory;
import com.netflix.titus.master.kubernetes.pod.env.ContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.env.ContainerEnvs;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.mesos.ContainerInfoUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.KubeModelConverters;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1TopologySpreadConstraint;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.titanframework.messages.TitanProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobDesiredSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobType;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.isServiceJob;
import static com.netflix.titus.common.util.Evaluators.applyNotNull;

@Singleton
public class V0SpecPodFactory implements PodFactory {

    private static final Logger logger = LoggerFactory.getLogger(V0SpecPodFactory.class);

    private static final String FENZO_SCHEDULER = "fenzo-scheduler";
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

    private static final String NEVER_RESTART_POLICY = "Never";
    private static final String DEFAULT_DNS_POLICY = "Default";


    private static final String ARN_PREFIX = "arn:aws:iam::";
    private static final String ARN_SUFFIX = ":role/";
    private static final Pattern IAM_PROFILE_RE = Pattern.compile(ARN_PREFIX + "(\\d+)" + ARN_SUFFIX + "\\S+");

    private final KubePodConfiguration configuration;
    private final MasterConfiguration jobCoordinatorConfiguration;
    private final SchedulerConfiguration schedulerConfiguration;
    private final ApplicationSlaManagementService capacityGroupManagement;
    private final PodAffinityFactory podAffinityFactory;
    private final TaintTolerationFactory taintTolerationFactory;
    private final ContainerEnvFactory containerEnvFactory;
    private final LogStorageInfo<Task> logStorageInfo;
    private final String iamArnPrefix;
    private final Function<String, Matcher> jobsWithNoSpreadingMatcher;

    @Inject
    public V0SpecPodFactory(KubePodConfiguration configuration,
                            MasterConfiguration jobCoordinatorConfiguration,
                            ApplicationSlaManagementService capacityGroupManagement,
                            PodAffinityFactory podAffinityFactory,
                            TaintTolerationFactory taintTolerationFactory,
                            ContainerEnvFactory ContainerEnvFactory,
                            LogStorageInfo<Task> logStorageInfo,
                            SchedulerConfiguration schedulerConfiguration) {
        this.configuration = configuration;
        this.jobCoordinatorConfiguration = jobCoordinatorConfiguration;
        this.capacityGroupManagement = capacityGroupManagement;
        this.podAffinityFactory = podAffinityFactory;
        this.taintTolerationFactory = taintTolerationFactory;
        containerEnvFactory = ContainerEnvFactory;
        this.logStorageInfo = logStorageInfo;
        this.schedulerConfiguration = schedulerConfiguration;

        // Get the AWS account ID to use for building IAM ARNs.
        String accountId = Evaluators.getOrDefault(System.getenv("EC2_OWNER_ID"), "default");
        this.iamArnPrefix = ARN_PREFIX + accountId + ARN_SUFFIX;

        this.jobsWithNoSpreadingMatcher = RegExpExt.dynamicMatcher(configuration::getDisabledJobSpreadingPattern,
                "disabledJobSpreadingPattern", Pattern.DOTALL, logger);
    }

    @Override
    public V1Pod buildV1Pod(Job<?> job, Task task, boolean useKubeScheduler, boolean useKubePv) {
        String taskId = task.getId();
        TitanProtos.ContainerInfo containerInfo = buildContainerInfo(job, task);
        Map<String, String> annotations = KubePodUtil.createPodAnnotations(job, task, containerInfo.toByteArray(),
                containerInfo.getPassthroughAttributesMap(), configuration.isJobDescriptorAnnotationEnabled());

        Pair<V1Affinity, Map<String, String>> affinityWithMetadata = podAffinityFactory.buildV1Affinity(job, task);
        annotations.putAll(affinityWithMetadata.getRight());

        Map<String, String> labels = new HashMap<>();
        labels.put(KubeConstants.POD_LABEL_JOB_ID, job.getId());
        labels.put(KubeConstants.POD_LABEL_TASK_ID, taskId);

        String capacityGroup = JobManagerUtil.getCapacityGroupDescriptorName(job.getJobDescriptor(), capacityGroupManagement).toLowerCase();
        labels.put(KubeConstants.LABEL_CAPACITY_GROUP, capacityGroup);

        if (configuration.isBytePodResourceEnabled()) {
            labels.put(KubeConstants.POD_LABEL_BYTE_UNITS, "true");
        }

        V1ObjectMeta metadata = new V1ObjectMeta()
                .name(taskId)
                .annotations(annotations)
                .labels(labels);

        V1Container container = new V1Container()
                .name(taskId)
                .image("imageIsInContainerInfo")
                .env(ContainerEnvs.toV1EnvVar(containerEnvFactory.buildContainerEnv(job, task)))
                .resources(buildV1ResourceRequirements(job.getJobDescriptor().getContainer().getContainerResources()));

        String schedulerName = FENZO_SCHEDULER;
        if (useKubeScheduler) {
            ApplicationSLA capacityGroupDescriptor = JobManagerUtil.getCapacityGroupDescriptor(job.getJobDescriptor(), capacityGroupManagement);
            if (capacityGroupDescriptor != null && capacityGroupDescriptor.getTier() == Tier.Critical) {
                if (schedulerConfiguration.isCriticalServiceJobSpreadingEnabled()) {
                    schedulerName = configuration.getReservedCapacityKubeSchedulerName();
                } else {
                    schedulerName = configuration.getReservedCapacityKubeSchedulerNameForBinPacking();
                }
            } else {
                schedulerName = configuration.getKubeSchedulerName();
            }
        }

        V1PodSpec spec = new V1PodSpec()
                .schedulerName(schedulerName)
                .containers(Collections.singletonList(container))
                .terminationGracePeriodSeconds(configuration.getPodTerminationGracePeriodSeconds())
                .restartPolicy(NEVER_RESTART_POLICY)
                .dnsPolicy(DEFAULT_DNS_POLICY)
                .affinity(affinityWithMetadata.getLeft())
                .tolerations(taintTolerationFactory.buildV1Toleration(job, task, useKubeScheduler))
                .topologySpreadConstraints(buildTopologySpreadConstraints(job));

        //  If kube scheduler is not enabled then the node name needs to be explicitly set
        if (!useKubeScheduler) {
            spec.setNodeName(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID));
        }
        Optional<Pair<V1Volume, V1VolumeMount>> optionalEbsVolumeInfo = buildV1VolumeInfo(job, task);
        if (useKubePv && optionalEbsVolumeInfo.isPresent()) {
            spec.addVolumesItem(optionalEbsVolumeInfo.get().getLeft());
            container.addVolumeMountsItem(optionalEbsVolumeInfo.get().getRight());
        }

        return new V1Pod().metadata(metadata).spec(spec);
    }

    @VisibleForTesting
    V1ResourceRequirements buildV1ResourceRequirements(ContainerResources containerResources) {
        Map<String, Quantity> requests = new HashMap<>();
        Map<String, Quantity> limits = new HashMap<>();

        requests.put("cpu", new Quantity(String.valueOf(containerResources.getCpu())));
        limits.put("cpu", new Quantity(String.valueOf(containerResources.getCpu())));

        requests.put("nvidia.com/gpu", new Quantity(String.valueOf(containerResources.getGpu())));
        limits.put("nvidia.com/gpu", new Quantity(String.valueOf(containerResources.getGpu())));

        Quantity memory;
        Quantity disk;
        Quantity network;
        if (configuration.isBytePodResourceEnabled()) {
            memory = new Quantity(containerResources.getMemoryMB() + "Mi");
            disk = new Quantity(containerResources.getDiskMB() + "Mi");
            network = new Quantity(containerResources.getNetworkMbps() + "M");
        } else {
            memory = new Quantity(String.valueOf(containerResources.getMemoryMB()));
            disk = new Quantity(String.valueOf(containerResources.getDiskMB()));
            network = new Quantity(String.valueOf(containerResources.getNetworkMbps()));
        }

        requests.put("memory", memory);
        limits.put("memory", memory);

        requests.put("ephemeral-storage", disk);
        limits.put("ephemeral-storage", disk);

        requests.put("titus/network", network);
        limits.put("titus/network", network);

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

        if (jobCoordinatorConfiguration.isContainerInfoEnvEnabled()) {
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
    List<V1TopologySpreadConstraint> buildTopologySpreadConstraints(Job<?> job) {
        List<V1TopologySpreadConstraint> constraints = new ArrayList<>();
        buildZoneTopologySpreadConstraints(job).ifPresent(constraints::add);
        buildJobTopologySpreadConstraints(job).ifPresent(constraints::add);
        return constraints;
    }

    private Optional<V1TopologySpreadConstraint> buildZoneTopologySpreadConstraints(Job<?> job) {
        boolean zoneHard = Boolean.parseBoolean(JobFunctions.findHardConstraint(job, JobConstraints.ZONE_BALANCE).orElse("false"));
        boolean zoneSoft = Boolean.parseBoolean(JobFunctions.findSoftConstraint(job, JobConstraints.ZONE_BALANCE).orElse("false"));
        if (!zoneHard && !zoneSoft) {
            return Optional.empty();
        }

        V1TopologySpreadConstraint zoneConstraint = new V1TopologySpreadConstraint()
                .topologyKey(KubeConstants.NODE_LABEL_ZONE)
                .labelSelector(new V1LabelSelector().matchLabels(Collections.singletonMap(KubeConstants.POD_LABEL_JOB_ID, job.getId())))
                .maxSkew(1);

        if (zoneHard) {
            zoneConstraint.whenUnsatisfiable("DoNotSchedule");
        } else {
            zoneConstraint.whenUnsatisfiable("ScheduleAnyway");
        }
        return Optional.of(zoneConstraint);
    }

    private Optional<V1TopologySpreadConstraint> buildJobTopologySpreadConstraints(Job<?> job) {
        if (!isJobSpreadingEnabled(job)) {
            return Optional.empty();
        }
        int maxSkew = getJobMaxSkew(job);

        V1TopologySpreadConstraint nodeConstraint = new V1TopologySpreadConstraint()
                .topologyKey(KubeConstants.NODE_LABEL_MACHINE_ID)
                .labelSelector(new V1LabelSelector().matchLabels(Collections.singletonMap(KubeConstants.POD_LABEL_JOB_ID, job.getId())))
                .maxSkew(maxSkew)
                .whenUnsatisfiable("ScheduleAnyway");
        return Optional.of(nodeConstraint);
    }

    /**
     * Spreading is by default enabled for service jobs and disabled for batch jobs.
     */
    private boolean isJobSpreadingEnabled(Job<?> job) {
        if (jobsWithNoSpreadingMatcher.apply(job.getJobDescriptor().getApplicationName()).matches()) {
            return false;
        }
        if (jobsWithNoSpreadingMatcher.apply(job.getJobDescriptor().getCapacityGroup()).matches()) {
            return false;
        }
        String spreadingEnabledAttr = job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_SPREADING_ENABLED);
        if (spreadingEnabledAttr == null) {
            return isServiceJob(job);
        }
        return Boolean.parseBoolean(spreadingEnabledAttr);
    }

    /**
     * Get max skew from a job descriptor or compute a value based on the job type and its configured disruption budget.
     *
     * @return -1 if max skew not set or is invalid
     */
    private static int getJobMaxSkew(Job<?> job) {
        String maxSkewAttr = job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_SPREADING_MAX_SKEW);
        try {
            int maxSkew = Integer.parseInt(maxSkewAttr);
            if (maxSkew > 0) {
                return maxSkew;
            }
        } catch (Exception ignore) {
        }

        DisruptionBudgetPolicy policy = job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();
        if (!(policy instanceof AvailabilityPercentageLimitDisruptionBudgetPolicy)) {
            return 1;
        }
        int jobSize = getJobDesiredSize(job);
        if (jobSize <= 1) {
            return 1;
        }
        AvailabilityPercentageLimitDisruptionBudgetPolicy availabilityPolicy = (AvailabilityPercentageLimitDisruptionBudgetPolicy) policy;
        int maxSkew = (int) (jobSize * (100 - availabilityPolicy.getPercentageOfHealthyContainers()) / 100);
        return Math.max(maxSkew, 1);
    }

    /**
     * Builds the various objects needed to for PersistentVolume and Pod objects to use an volume.
     */
    @VisibleForTesting
    Optional<Pair<V1Volume, V1VolumeMount>> buildV1VolumeInfo(Job<?> job, Task task) {
        return EbsVolumeUtils.getEbsVolumeForTask(job, task)
                .map(ebsVolume -> {
                    boolean readOnly = ebsVolume.getMountPermissions().equals(EbsVolume.MountPerm.RO);
                    V1Volume v1Volume = new V1Volume()
                            // The resource name matches the volume ID so that the resource is independent of the job.
                            .name(ebsVolume.getVolumeId())
                            .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                                    .claimName(KubeModelConverters.toPvcName(ebsVolume.getVolumeId(), task.getId())));

                    V1VolumeMount v1VolumeMount = new V1VolumeMount()
                            // The mount refers to the V1Volume being mounted
                            .name(ebsVolume.getVolumeId())
                            .mountPath(ebsVolume.getMountPath())
                            .readOnly(readOnly);

                    return Pair.of(v1Volume, v1VolumeMount);
                });
    }
}
