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

import java.util.ArrayList;
import java.util.Base64;
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
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.volume.SharedContainerVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.volume.Volume;
import com.netflix.titus.api.jobmanager.model.job.volume.VolumeSource;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.KubePodUtil;
import com.netflix.titus.master.kubernetes.pod.PodFactory;
import com.netflix.titus.master.kubernetes.pod.affinity.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.env.PodEnvFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.PodContainerInfoFactory;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.kubernetes.pod.topology.TopologyFactory;
import com.netflix.titus.master.mesos.kubeapiserver.PerformanceToolUtil;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1FlexVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.titanframework.messages.TitanProtos;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_WRITER_ROLE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_SUBNETS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_JUMBO;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_ASSIGN_IPV6_ADDRESS;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_HOSTNAME_STYLE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_KEEP_LOCAL_FILE_AFTER_UPLOAD;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_STDIO_CHECK_INTERVAL;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_CHECK_INTERVAL;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_REGEXP;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_THRESHOLD_TIME;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH;
import static com.netflix.titus.api.jobmanager.JobAttributes.TITUS_PARAMETER_AGENT_PREFIX;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobType;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.CONTAINER_INFO;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_DNS_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_IMAGE_PULL_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_NAMESPACE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.EGRESS_BANDWIDTH;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.ENTRYPOINT_SHELL_SPLITTING_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.FENZO_SCHEDULER;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.IAM_ROLE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.INGRESS_BANDWIDTH;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.JOB_ACCEPTED_TIMESTAMP_MS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.JOB_DESCRIPTOR;
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
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_JUMBO_FRAMES_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_SECURITY_GROUPS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NETWORK_SUBNET_IDS;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NEVER_RESTART_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_CPU_BURSTING_ENABLED;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_HOSTNAME_STYLE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SCHED_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SCHEMA_VERSION;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_USER_ENV_VARS_START_INDEX;
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
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.buildV1VolumeInfo;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.createEbsPodAnnotations;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.toV1EnvVar;

@Singleton
public class V1SpecPodFactory implements PodFactory {

    private final KubePodConfiguration configuration;
    private final ApplicationSlaManagementService capacityGroupManagement;
    private final PodAffinityFactory podAffinityFactory;
    private final TaintTolerationFactory taintTolerationFactory;
    private final TopologyFactory topologyFactory;
    private final PodEnvFactory podEnvFactory;
    private final PodContainerInfoFactory podContainerInfoFactory;

    @Inject
    public V1SpecPodFactory(KubePodConfiguration configuration,
                            ApplicationSlaManagementService capacityGroupManagement,
                            PodAffinityFactory podAffinityFactory,
                            TaintTolerationFactory taintTolerationFactory,
                            TopologyFactory topologyFactory,
                            PodEnvFactory podEnvFactory,
                            PodContainerInfoFactory podContainerInfoFactory) {
        this.configuration = configuration;
        this.capacityGroupManagement = capacityGroupManagement;
        this.podAffinityFactory = podAffinityFactory;
        this.taintTolerationFactory = taintTolerationFactory;
        this.topologyFactory = topologyFactory;
        this.podEnvFactory = podEnvFactory;
        this.podContainerInfoFactory = podContainerInfoFactory;
    }

    @Override
    public V1Pod buildV1Pod(Job<?> job, Task task, boolean useKubeScheduler, boolean useKubePv) {

        String taskId = task.getId();
        TitanProtos.ContainerInfo containerInfo = podContainerInfoFactory.buildContainerInfo(job, task, false);
        Map<String, String> annotations = createPodAnnotations(job, task, containerInfo.toByteArray());

        Pair<V1Affinity, Map<String, String>> affinityWithMetadata = podAffinityFactory.buildV1Affinity(job, task);
        annotations.putAll(affinityWithMetadata.getRight());

        Pair<Integer, List<V1EnvVar>> envVarsWithIndex = podEnvFactory.buildEnv(job, task);
        int userEnvBeginIndex = envVarsWithIndex.getLeft();
        List<V1EnvVar> envVarsList = envVarsWithIndex.getRight();
        annotations.put(POD_USER_ENV_VARS_START_INDEX, String.valueOf(userEnvBeginIndex));

        Map<String, String> labels = new HashMap<>();
        labels.put(KubeConstants.POD_LABEL_JOB_ID, job.getId());
        labels.put(KubeConstants.POD_LABEL_TASK_ID, taskId);
        JobManagerUtil.getRelocationBinpackMode(job).ifPresent(mode -> labels.put(KubeConstants.POD_LABEL_RELOCATION_BINPACK, mode));

        JobDescriptor<?> jobDescriptor = job.getJobDescriptor();
        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        labels.put(WORKLOAD_NAME, jobDescriptor.getApplicationName());
        labels.put(WORKLOAD_STACK, jobGroupInfo.getStack());
        labels.put(WORKLOAD_DETAIL, jobGroupInfo.getDetail());
        labels.put(WORKLOAD_SEQUENCE, jobGroupInfo.getSequence());

        String capacityGroup = JobManagerUtil.getCapacityGroupDescriptorName(job.getJobDescriptor(), capacityGroupManagement).toLowerCase();
        labels.put(KubeConstants.LABEL_CAPACITY_GROUP, capacityGroup);

        V1ObjectMeta metadata = new V1ObjectMeta()
                .name(taskId)
                .namespace(DEFAULT_NAMESPACE)
                .annotations(annotations)
                .labels(labels);

        V1Container container = new V1Container()
                .name(taskId)
                .image(KubePodUtil.buildImageString(configuration.getRegistryUrl(), jobDescriptor.getContainer().getImage()))
                .env(envVarsList)
                .resources(buildV1ResourceRequirements(job.getJobDescriptor().getContainer().getContainerResources()))
                .imagePullPolicy(DEFAULT_IMAGE_PULL_POLICY);

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

        String schedulerName = FENZO_SCHEDULER;
        if (useKubeScheduler) {
            ApplicationSLA capacityGroupDescriptor = JobManagerUtil.getCapacityGroupDescriptor(job.getJobDescriptor(), capacityGroupManagement);
            if (capacityGroupDescriptor != null && capacityGroupDescriptor.getTier() == Tier.Critical) {
                schedulerName = configuration.getReservedCapacityKubeSchedulerName();
            } else {
                schedulerName = configuration.getKubeSchedulerName();
            }
        }

        V1PodSpec spec = new V1PodSpec()
                .schedulerName(schedulerName)
                .containers(allContainers)
                .volumes(volumes)
                .terminationGracePeriodSeconds(configuration.getPodTerminationGracePeriodSeconds())
                .restartPolicy(NEVER_RESTART_POLICY)
                .dnsPolicy(DEFAULT_DNS_POLICY)
                .affinity(affinityWithMetadata.getLeft())
                .tolerations(taintTolerationFactory.buildV1Toleration(job, task, useKubeScheduler))
                .topologySpreadConstraints(topologyFactory.buildTopologySpreadConstraints(job));

        //  If kube scheduler is not enabled then the node name needs to be explicitly set
        if (!useKubeScheduler) {
            spec.setNodeName(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID));
        }
        // volumes need to be correctly added to pod spec
        Optional<Pair<V1Volume, V1VolumeMount>> optionalEbsVolumeInfo = buildV1VolumeInfo(job, task);
        if (useKubePv && optionalEbsVolumeInfo.isPresent()) {
            spec.addVolumesItem(optionalEbsVolumeInfo.get().getLeft());
            container.addVolumeMountsItem(optionalEbsVolumeInfo.get().getRight());
        }

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
                .imagePullPolicy(DEFAULT_IMAGE_PULL_POLICY);
    }


    private List<V1Volume> buildV1Volumes(List<Volume> volumes) {
        if (volumes == null) {
            return Collections.emptyList();
        }
        List<V1Volume> v1Volumes = new ArrayList<>();
        for (Volume v : volumes) {
            buildV1Volume(v).ifPresent(realVolume -> v1Volumes.add(realVolume));
        }
        return v1Volumes;
    }

    private Optional<V1Volume> buildV1Volume(Volume volume) {
        if (volume.getVolumeSource() instanceof SharedContainerVolumeSource) {
            V1FlexVolumeSource flexVolume = getV1FlexVolumeForSharedContainerVolumeSource(volume);
            return Optional.ofNullable(new V1Volume()
                    .name(volume.getName())
                    .flexVolume(flexVolume));
        } else {
            // SharedVolumeSource is currently the only supported volume type
            return Optional.empty();
        }
    }

    private V1FlexVolumeSource getV1FlexVolumeForSharedContainerVolumeSource(Volume volume) {
        SharedContainerVolumeSource sharedContainerVolumeSource = (SharedContainerVolumeSource) volume.getVolumeSource();
        Map<String, String> options = new HashMap<>();
        options.put("sourceContainer", sharedContainerVolumeSource.getSourceContainer());
        options.put("sourcePath", sharedContainerVolumeSource.getSourcePath());
        return new V1FlexVolumeSource()
                .driver("SharedContainerVolumeSource")
                .options(options);
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

    Map<String, String> createPodAnnotations(
            Job<?> job,
            Task task,
            byte[] containerInfoData
    ) {
        String encodedContainerInfo = Base64.getEncoder().encodeToString(containerInfoData);
        String encodedJobDescriptor = KubePodUtil.createEncodedJobDescriptor(job);

        com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor = job.getJobDescriptor();
        Container container = jobDescriptor.getContainer();

        Map<String, String> annotations = new HashMap<>();
        annotations.put(POD_SCHEMA_VERSION, "1");

        annotations.put(JOB_ID, job.getId());
        annotations.put(JOB_TYPE, getJobType(job).name());
        annotations.put(CONTAINER_INFO, encodedContainerInfo);
        annotations.put(JOB_DESCRIPTOR, encodedJobDescriptor);

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
        //TODO check if this is always fully qualified or we need to continue checking here
        annotations.put(IAM_ROLE, securityProfile.getIamRole());

        String appMetadata = securityProfile.getAttributes().getOrDefault("NETFLIX_APP_METADATA", "");
        String appMetadataSignature = securityProfile.getAttributes().getOrDefault("NETFLIX_APP_METADATA_SIG", "");
        annotations.put(SECURITY_APP_METADATA, appMetadata);
        annotations.put(SECURITY_APP_METADATA_SIG, appMetadataSignature);

        Evaluators.acceptNotNull(
                job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC),
                runtimeInSec -> annotations.put(KubeConstants.JOB_RUNTIME_PREDICTION, runtimeInSec + "s")
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                count -> annotations.put(KubeConstants.OPPORTUNISTIC_CPU_COUNT, count)
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION),
                id -> annotations.put(KubeConstants.OPPORTUNISTIC_ID, id)
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID),
                id -> annotations.put(KubeConstants.STATIC_IP_ALLOCATION_ID, id)
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
                case JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH:
                    annotations.put(POD_SCHED_POLICY, v);
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
                case JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME:
                    annotations.put(LOG_S3_BUCKET_NAME, v);
                    break;
                case JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX:
                    annotations.put(LOG_S3_PATH_PREFIX, v);
                    break;
                case JOB_CONTAINER_ATTRIBUTE_S3_WRITER_ROLE:
                    annotations.put(LOG_S3_WRITER_IAM_ROLE, v);
                    break;
                default:
                    annotations.put(k, v);
                    break;
            }
        });

        annotations.putAll(createEbsPodAnnotations(job, task));
        annotations.putAll(PerformanceToolUtil.toAnnotations(job));

        return annotations;
    }

    private boolean shouldSkipEntryPointJoin(Map<String, String> jobAttributes) {
        return Boolean.parseBoolean(jobAttributes.getOrDefault(JobAttributes.JOB_PARAMETER_ATTRIBUTES_ENTRY_POINT_SKIP_SHELL_PARSING,
                "false").trim());

    }
}
