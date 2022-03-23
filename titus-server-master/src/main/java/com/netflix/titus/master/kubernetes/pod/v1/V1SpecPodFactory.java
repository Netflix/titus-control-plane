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

package com.netflix.titus.master.kubernetes.pod.v1;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BasicContainer;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfos;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.kubernetes.PerformanceToolUtil;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.KubePodUtil;
import com.netflix.titus.master.kubernetes.pod.PodFactory;
import com.netflix.titus.master.kubernetes.pod.affinity.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.ContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.kubernetes.pod.topology.TopologyFactory;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EmptyDirVolumeSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1NFSVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_IMDS_REQUIRE_TOKEN;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_SECCOMP_AGENT_NET_ENABLED;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_SECCOMP_AGENT_PERF_ENABLED;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_SUBNETS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_JUMBO;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ASSIGN_IPV6_ADDRESS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_FUSE_ENABLED;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_HOSTNAME_STYLE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_KEEP_LOCAL_FILE_AFTER_UPLOAD;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_STDIO_CHECK_INTERVAL;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_CHECK_INTERVAL;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_REGEXP;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_THRESHOLD_TIME;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTE_EIPS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTE_EIP_POOL;
import static com.netflix.titus.api.jobmanager.JobAttributes.TITUS_PARAMETER_AGENT_PREFIX;
import static com.netflix.titus.api.jobmanager.model.job.Container.ATTRIBUTE_NETFLIX_APP_METADATA;
import static com.netflix.titus.api.jobmanager.model.job.Container.ATTRIBUTE_NETFLIX_APP_METADATA_SIG;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobType;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_DNS_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_IMAGE_PULL_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_NAMESPACE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.EGRESS_BANDWIDTH;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.ENTRYPOINT_SHELL_SPLITTING_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.IAM_ROLE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.INGRESS_BANDWIDTH;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.JOB_ACCEPTED_TIMESTAMP_MS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.JOB_DISRUPTION_BUDGET_POLICY_NAME;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.JOB_ID;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.JOB_TYPE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_KEEP_LOCAL_FILE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_S3_BUCKET_NAME;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_S3_PATH_PREFIX;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_S3_WRITER_IAM_ROLE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_STDIO_CHECK_INTERVAL;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_UPLOAD_CHECK_INTERVAL;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_UPLOAD_REGEXP;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.LOG_UPLOAD_THRESHOLD_TIME;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_ACCOUNT_ID;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_ASSIGN_IVP6_ADDRESS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_BURSTING_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_ELASTIC_IPS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_ELASTIC_IP_POOL;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_IMDS_REQUIRE_TOKEN;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_JUMBO_FRAMES_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_SECURITY_GROUPS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_SUBNET_IDS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NEVER_RESTART_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_CPU_BURSTING_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_FUSE_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_HOSTNAME_STYLE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_IMAGE_TAG_PREFIX;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SCHED_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SCHEMA_VERSION;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SECCOMP_AGENT_NET_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SECCOMP_AGENT_PERF_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SYSTEM_ENV_VAR_NAMES;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_CPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_EPHERMERAL_STORAGE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_GPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_MEMORY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_NETWORK;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.SECURITY_APP_METADATA;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.SECURITY_APP_METADATA_SIG;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.WORKLOAD_DETAIL;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.WORKLOAD_NAME;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.WORKLOAD_OWNER_EMAIL;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.WORKLOAD_SEQUENCE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.WORKLOAD_STACK;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.buildV1EBSObjects;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.buildV1Volumes;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.createEbsPodAnnotations;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.createPlatformSidecarAnnotations;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.sanitizeVolumeName;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.selectScheduler;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.toV1EnvVar;


@Singleton
public class V1SpecPodFactory implements PodFactory {

    public static final String DEV_SHM = "dev-shm";
    public static final String DEV_SHM_MOUNT_PATH = "/dev/shm";
    private final KubePodConfiguration configuration;
    private final ApplicationSlaManagementService capacityGroupManagement;
    private final PodAffinityFactory podAffinityFactory;
    private final TaintTolerationFactory taintTolerationFactory;
    private final TopologyFactory topologyFactory;
    private final ContainerEnvFactory containerEnvFactory;
    private final LogStorageInfo<Task> logStorageInfo;
    private final SchedulerConfiguration schedulerConfiguration;

    @Inject
    public V1SpecPodFactory(KubePodConfiguration configuration,
                            ApplicationSlaManagementService capacityGroupManagement,
                            PodAffinityFactory podAffinityFactory,
                            TaintTolerationFactory taintTolerationFactory,
                            TopologyFactory topologyFactory,
                            ContainerEnvFactory containerEnvFactory,
                            LogStorageInfo<Task> logStorageInfo,
                            SchedulerConfiguration schedulerConfiguration) {
        this.configuration = configuration;
        this.capacityGroupManagement = capacityGroupManagement;
        this.podAffinityFactory = podAffinityFactory;
        this.taintTolerationFactory = taintTolerationFactory;
        this.topologyFactory = topologyFactory;
        this.containerEnvFactory = containerEnvFactory;
        this.logStorageInfo = logStorageInfo;
        this.schedulerConfiguration = schedulerConfiguration;

    }

    @Override
    public V1Pod buildV1Pod(Job<?> job, Task task) {

        String taskId = task.getId();
        Map<String, String> annotations = createV1SchemaPodAnnotations(job, task);

        Pair<V1Affinity, Map<String, String>> affinityWithMetadata = podAffinityFactory.buildV1Affinity(job, task);
        annotations.putAll(affinityWithMetadata.getRight());

        Pair<List<String>, Map<String, String>> envVarsWithIndex = containerEnvFactory.buildContainerEnv(job, task);
        List<V1EnvVar> envVarsList = toV1EnvVar(envVarsWithIndex.getRight());
        annotations.put(POD_SYSTEM_ENV_VAR_NAMES, String.join(",", envVarsWithIndex.getLeft()));

        Map<String, String> labels = new HashMap<>();
        labels.put(KubeConstants.POD_LABEL_JOB_ID, job.getId());
        labels.put(KubeConstants.POD_LABEL_TASK_ID, taskId);

        JobManagerUtil.getRelocationBinpackMode(job).ifPresent(mode -> labels.put(KubeConstants.POD_LABEL_RELOCATION_BINPACK, mode));

        // A V1Container has no room to store the original tag that the Image came from, so we store it as an
        // annotation. Only saving the 'main' one for now.
        annotations.put(POD_IMAGE_TAG_PREFIX + "main", job.getJobDescriptor().getContainer().getImage().getTag());

        JobDescriptor<?> jobDescriptor = job.getJobDescriptor();
        String capacityGroup = JobManagerUtil.getCapacityGroupDescriptorName(job.getJobDescriptor(), capacityGroupManagement).toLowerCase();
        labels.put(KubeConstants.LABEL_CAPACITY_GROUP, capacityGroup);

        V1ObjectMeta metadata = new V1ObjectMeta()
                .name(taskId)
                .namespace(DEFAULT_NAMESPACE)
                .annotations(annotations)
                .labels(labels);

        V1Container container = new V1Container()
                .name("main")
                .image(KubePodUtil.buildImageString(configuration.getRegistryUrl(), jobDescriptor.getContainer().getImage()))
                .env(envVarsList)
                .resources(buildV1ResourceRequirements(job.getJobDescriptor().getContainer().getContainerResources()))
                .imagePullPolicy(DEFAULT_IMAGE_PULL_POLICY)
                .volumeMounts(KubePodUtil.buildV1VolumeMounts(job.getJobDescriptor().getContainer().getVolumeMounts()));

        Container jobContainer = jobDescriptor.getContainer();
        if (CollectionsExt.isNullOrEmpty(jobContainer.getCommand()) && !shouldSkipEntryPointJoin(jobDescriptor.getAttributes())) {
            // use the old behavior where the agent needs to do shell splitting
            String entrypointStr = StringExt.concatenate(jobContainer.getEntryPoint(), " ");
            container.setCommand(Collections.singletonList(entrypointStr));
            annotations.put(ENTRYPOINT_SHELL_SPLITTING_ENABLED, "true");
        } else {
            container.setCommand(jobContainer.getEntryPoint());
            container.setArgs(jobContainer.getCommand());
        }

        List<V1Container> extraContainers = buildV1ExtraContainers(job.getJobDescriptor().getExtraContainers());
        List<V1Container> allContainers = Stream.concat(Stream.of(container), extraContainers.stream()).collect(Collectors.toList());
        List<V1Volume> volumes = buildV1Volumes(job.getJobDescriptor().getVolumes());

        ApplicationSLA capacityGroupDescriptor = JobManagerUtil.getCapacityGroupDescriptor(job.getJobDescriptor(), capacityGroupManagement);
        String schedulerName = selectScheduler(schedulerConfiguration, capacityGroupDescriptor, configuration);

        V1PodSpec spec = new V1PodSpec()
                .schedulerName(schedulerName)
                .containers(allContainers)
                .volumes(volumes)
                .terminationGracePeriodSeconds(configuration.getPodTerminationGracePeriodSeconds())
                .restartPolicy(NEVER_RESTART_POLICY)
                .dnsPolicy(DEFAULT_DNS_POLICY)
                .affinity(affinityWithMetadata.getLeft())
                .tolerations(taintTolerationFactory.buildV1Toleration(job, task))
                .topologySpreadConstraints(topologyFactory.buildTopologySpreadConstraints(job));

        // volumes need to be correctly added to pod spec
        Optional<Pair<V1Volume, V1VolumeMount>> optionalEbsVolumeInfo = buildV1EBSObjects(job, task);
        if (optionalEbsVolumeInfo.isPresent()) {
            spec.addVolumesItem(optionalEbsVolumeInfo.get().getLeft());
            container.addVolumeMountsItem(optionalEbsVolumeInfo.get().getRight());
        }

        appendEfsMounts(spec, container, job);
        appendShmMount(spec, container, job);

        return new V1Pod().metadata(metadata).spec(spec);
    }

    private List<V1Container> buildV1ExtraContainers(List<BasicContainer> extraContainers) {
        if (extraContainers == null) {
            return Collections.emptyList();
        }
        return extraContainers.stream().map(this::buildV1ExtraContainers).collect(Collectors.toList());
    }

    private V1Container buildV1ExtraContainers(BasicContainer extraContainer) {
        return new V1Container()
                .name(extraContainer.getName())
                .command(extraContainer.getEntryPoint())
                .args(extraContainer.getCommand())
                .image(KubePodUtil.buildImageString(configuration.getRegistryUrl(), extraContainer.getImage()))
                .env(toV1EnvVar(extraContainer.getEnv()))
                .imagePullPolicy(DEFAULT_IMAGE_PULL_POLICY)
                .volumeMounts(KubePodUtil.buildV1VolumeMounts(extraContainer.getVolumeMounts()));
    }


    @VisibleForTesting
    V1ResourceRequirements buildV1ResourceRequirements(ContainerResources containerResources) {
        Map<String, Quantity> requests = new HashMap<>();
        Map<String, Quantity> limits = new HashMap<>();

        Quantity cpu = new Quantity(String.valueOf(containerResources.getCpu()));
        Quantity memory = new Quantity(containerResources.getMemoryMB() + "Mi");
        Quantity disk = new Quantity(containerResources.getDiskMB() + "Mi");
        Quantity network = new Quantity(containerResources.getNetworkMbps() + "M");
        Quantity gpu = new Quantity(String.valueOf(containerResources.getGpu()));

        requests.put(RESOURCE_CPU, cpu);
        limits.put(RESOURCE_CPU, cpu);

        requests.put(RESOURCE_MEMORY, memory);
        limits.put(RESOURCE_MEMORY, memory);

        requests.put(RESOURCE_EPHERMERAL_STORAGE, disk);
        limits.put(RESOURCE_EPHERMERAL_STORAGE, disk);

        requests.put(RESOURCE_NETWORK, network);
        limits.put(RESOURCE_NETWORK, network);

        requests.put(RESOURCE_GPU, gpu);
        limits.put(RESOURCE_GPU, gpu);

        return new V1ResourceRequirements().requests(requests).limits(limits);
    }

    Map<String, String> createV1SchemaPodAnnotations(
            Job<?> job,
            Task task
    ) {
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor = job.getJobDescriptor();
        Container container = jobDescriptor.getContainer();

        Map<String, String> annotations = new HashMap<>();
        annotations.put(POD_SCHEMA_VERSION, "1");

        annotations.put(JOB_ID, job.getId());
        annotations.put(JOB_TYPE, getJobType(job).name());
        if (jobDescriptor.getDisruptionBudget().getDisruptionBudgetPolicy() != null) {
            annotations.put(JOB_DISRUPTION_BUDGET_POLICY_NAME,
                    jobDescriptor.getDisruptionBudget().getDisruptionBudgetPolicy().getClass().getSimpleName());
        }

        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        annotations.put(WORKLOAD_NAME, jobDescriptor.getApplicationName());
        annotations.put(WORKLOAD_STACK, jobGroupInfo.getStack());
        annotations.put(WORKLOAD_DETAIL, jobGroupInfo.getDetail());
        annotations.put(WORKLOAD_SEQUENCE, jobGroupInfo.getSequence());
        annotations.put(WORKLOAD_OWNER_EMAIL, jobDescriptor.getOwner().getTeamEmail());

        Optional<JobStatus> jobStatus = JobFunctions.findJobStatus(job, JobState.Accepted);
        if (jobStatus.isPresent()) {
            String jobAcceptedTimestamp = String.valueOf(jobStatus.get().getTimestamp());
            annotations.put(JOB_ACCEPTED_TIMESTAMP_MS, jobAcceptedTimestamp);
        }

        ContainerResources containerResources = container.getContainerResources();

        String networkBandwidth = containerResources.getNetworkMbps() + "M";
        annotations.put(EGRESS_BANDWIDTH, networkBandwidth);
        annotations.put(INGRESS_BANDWIDTH, networkBandwidth);

        SecurityProfile securityProfile = container.getSecurityProfile();
        String securityGroups = StringExt.concatenate(securityProfile.getSecurityGroups(), ",");
        annotations.put(NETWORK_SECURITY_GROUPS, securityGroups);
        annotations.put(IAM_ROLE, securityProfile.getIamRole());

        Evaluators.acceptNotNull(
                securityProfile.getAttributes().get(ATTRIBUTE_NETFLIX_APP_METADATA),
                appMetadata -> annotations.put(SECURITY_APP_METADATA, appMetadata)
        );
        Evaluators.acceptNotNull(
                securityProfile.getAttributes().get(ATTRIBUTE_NETFLIX_APP_METADATA_SIG),
                appMetadataSignature -> annotations.put(SECURITY_APP_METADATA_SIG, appMetadataSignature)
        );

        Evaluators.acceptNotNull(
                job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC),
                runtimeInSec -> annotations.put(KubeConstants.JOB_RUNTIME_PREDICTION, runtimeInSec + "s")
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID),
                id -> annotations.put(KubeConstants.STATIC_IP_ALLOCATION_ID, id)
        );
        Evaluators.acceptNotNull(
                job.getJobDescriptor().getNetworkConfiguration().getNetworkModeName(),
                modeName -> annotations.put(KubeConstants.NETWORK_MODE, modeName)
        );

        // convert container attributes into annotations
        container.getAttributes().forEach((k, v) -> {
            if (StringExt.isEmpty(k) || StringExt.isEmpty(v) || !k.startsWith(TITUS_PARAMETER_AGENT_PREFIX)) {
                return;
            }

            switch (k) {
                case JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING:
                    annotations.put(POD_CPU_BURSTING_ENABLED, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING:
                    annotations.put(NETWORK_BURSTING_ENABLED, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTE_EIP_POOL:
                    annotations.put(NETWORK_ELASTIC_IP_POOL, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTE_EIPS:
                    annotations.put(NETWORK_ELASTIC_IPS, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH:
                    annotations.put(POD_SCHED_POLICY, "batch");
                    break;
                case JOB_CONTAINER_ATTRIBUTE_SUBNETS:
                    annotations.put(NETWORK_SUBNET_IDS, v);
                    break;
                case JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID:
                    annotations.put(NETWORK_ACCOUNT_ID, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_HOSTNAME_STYLE:
                    annotations.put(POD_HOSTNAME_STYLE, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_JUMBO:
                    annotations.put(NETWORK_JUMBO_FRAMES_ENABLED, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_FUSE_ENABLED:
                    annotations.put(POD_FUSE_ENABLED, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_ASSIGN_IPV6_ADDRESS:
                    annotations.put(NETWORK_ASSIGN_IVP6_ADDRESS, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_CHECK_INTERVAL:
                    annotations.put(LOG_UPLOAD_CHECK_INTERVAL, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_LOG_STDIO_CHECK_INTERVAL:
                    annotations.put(LOG_STDIO_CHECK_INTERVAL, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_THRESHOLD_TIME:
                    annotations.put(LOG_UPLOAD_THRESHOLD_TIME, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_LOG_KEEP_LOCAL_FILE_AFTER_UPLOAD:
                    annotations.put(LOG_KEEP_LOCAL_FILE, v);
                    break;
                case JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_REGEXP:
                    annotations.put(LOG_UPLOAD_REGEXP, v);
                    break;
                case JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX:
                    annotations.put(LOG_S3_PATH_PREFIX, v);
                    break;
                case JOB_CONTAINER_ATTRIBUTE_SECCOMP_AGENT_PERF_ENABLED:
                    annotations.put(POD_SECCOMP_AGENT_PERF_ENABLED, v);
                    break;
                case JOB_CONTAINER_ATTRIBUTE_SECCOMP_AGENT_NET_ENABLED:
                    annotations.put(POD_SECCOMP_AGENT_NET_ENABLED, v);
                    break;
                case JOB_CONTAINER_ATTRIBUTE_IMDS_REQUIRE_TOKEN:
                    annotations.put(NETWORK_IMDS_REQUIRE_TOKEN, v);
                    break;
                default:
                    annotations.put(k, v);
                    break;
            }
        });

        appendS3WriterRole(annotations, job, task);
        annotations.putAll(createEbsPodAnnotations(job, task));
        annotations.putAll(PerformanceToolUtil.toAnnotations(job));
        annotations.putAll(createPlatformSidecarAnnotations(job));

        return annotations;
    }

    private boolean shouldSkipEntryPointJoin(Map<String, String> jobAttributes) {
        return Boolean.parseBoolean(jobAttributes.getOrDefault(JobAttributes.JOB_PARAMETER_ATTRIBUTES_ENTRY_POINT_SKIP_SHELL_PARSING,
                "false").trim());

    }

    @VisibleForTesting
    void appendS3WriterRole(Map<String, String> annotations, Job<?> job, Task task) {
        if (LogStorageInfos.findCustomS3Bucket(job).isPresent()) {
            annotations.put(
                    LOG_S3_WRITER_IAM_ROLE,
                    job.getJobDescriptor().getContainer().getSecurityProfile().getIamRole()
            );
        } else {
            Evaluators.applyNotNull(
                    configuration.getDefaultS3WriterRole(),
                    role -> annotations.put(LOG_S3_WRITER_IAM_ROLE, role)
            );
        }

        logStorageInfo.getS3LogLocation(task, false).ifPresent(s3LogLocation ->
                Evaluators.applyNotNull(
                        s3LogLocation.getBucket(),
                        bucket -> annotations.put(LOG_S3_BUCKET_NAME, bucket)
                )
        );
    }

    void appendEfsMounts(V1PodSpec spec, V1Container container, Job<?> job) {
        List<EfsMount> efsMounts = job.getJobDescriptor().getContainer().getContainerResources().getEfsMounts();
        if (efsMounts.isEmpty()) {
            return;
        }
        for (EfsMount efsMount : efsMounts) {
            boolean readOnly = efsMount.getMountPerm() == EfsMount.MountPerm.RO;
            String efsId = efsMount.getEfsId();
            String efsMountPoint = efsMount.getMountPoint();
            String efsRelativeMountPoint = StringExt.isEmpty(efsMount.getEfsRelativeMountPoint()) ? "/" : efsMount.getEfsRelativeMountPoint();
            String name = sanitizeVolumeName(efsId + efsRelativeMountPoint);

            V1VolumeMount volumeMount = new V1VolumeMount()
                    .name(name)
                    .mountPath(efsMountPoint)
                    .readOnly(readOnly);
            container.addVolumeMountsItem(volumeMount);

            // We can't have duplicate volumes in here. In theory there should be a many:one mapping between
            // EFS mounts and the volumes that back them. For example, there could be two mounts to the same
            // nfs server, but with different *mount points*, but there should only be 1 volumes behind them
            List<String> allNames = KubePodUtil.getVolumeNames(spec.getVolumes());
            if (!allNames.contains(name)) {
                V1NFSVolumeSource nfsVolumeSource = new V1NFSVolumeSource()
                        .server(efsIdToNFSServer(efsId))
                        .readOnly(false);
                // "path" here represents the server-side relative mount path, sometimes called
                // the "exported directory", and goes into the v1 Volume
                nfsVolumeSource.setPath(efsRelativeMountPoint);
                V1Volume volume = new V1Volume()
                        .name(name)
                        .nfs(nfsVolumeSource);
                spec.addVolumesItem(volume);
            }
        }
    }

    /**
     * efsIdToNFSHostname will "resolve" an EFS ID into a real
     * hostname for the pod spec to use if necessary.
     * <p>
     * We have to do this because the pod spec doesn't know about EFS,
     * it just has a field for NFS hostname.
     * <p>
     * Titus has EFSID as a real entry. This function bridges the
     * gap and converts an EFS ID into a hostname.
     */
    private String efsIdToNFSServer(String efsId) {
        // Most of the time, the EFS ID passed into the control plane
        // is a real EFS
        if (isEFSID(efsId)) {
            return efsId + ".efs." + this.configuration.getTargetRegion() + ".amazonaws.com";
        }
        // But sometimes it is not, and in that case we can just let it go through
        // as-is
        return efsId;
    }

    private boolean isEFSID(String s) {
        return s.matches("^fs-[0-9a-f]+$");
    }


    void appendShmMount(V1PodSpec spec, V1Container container, Job<?> job) {
        int shmMB = job.getJobDescriptor().getContainer().getContainerResources().getShmMB();

        V1VolumeMount v1VolumeMount = new V1VolumeMount()
                .name(DEV_SHM)
                .mountPath(DEV_SHM_MOUNT_PATH);

        container.addVolumeMountsItem(v1VolumeMount);

        V1EmptyDirVolumeSource emptyDirVolumeSource = new V1EmptyDirVolumeSource()
                .medium("Memory")
                .sizeLimit(Quantity.fromString(shmMB + "Mi"));

        V1Volume volume = new V1Volume()
                .name(V1SpecPodFactory.DEV_SHM)
                .emptyDir(emptyDirVolumeSource);

        spec.addVolumesItem(volume);
    }
}
