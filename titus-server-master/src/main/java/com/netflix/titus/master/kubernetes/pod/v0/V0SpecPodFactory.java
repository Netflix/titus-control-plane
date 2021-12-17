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
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BasicContainer;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.KubePodUtil;
import com.netflix.titus.master.kubernetes.pod.PodFactory;
import com.netflix.titus.master.kubernetes.pod.affinity.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.ContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.PodContainerInfoFactory;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.kubernetes.pod.topology.TopologyFactory;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Volume;
import io.titanframework.messages.TitanProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_DNS_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.DEFAULT_IMAGE_PULL_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.NEVER_RESTART_POLICY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_CPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_EPHERMERAL_STORAGE;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_GPU;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_MEMORY;
import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.RESOURCE_NETWORK;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.createEbsPodAnnotations;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.createPlatformSidecarAnnotations;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.selectScheduler;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.toV1EnvVar;
import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.buildV1Volumes;

@Singleton
public class V0SpecPodFactory implements PodFactory {
    private static final Logger logger = LoggerFactory.getLogger(V0SpecPodFactory.class);

    private final KubePodConfiguration configuration;
    private final ApplicationSlaManagementService capacityGroupManagement;
    private final PodAffinityFactory podAffinityFactory;
    private final TaintTolerationFactory taintTolerationFactory;
    private final TopologyFactory topologyFactory;
    private final ContainerEnvFactory containerEnvFactory;
    private final PodContainerInfoFactory podContainerInfoFactory;
    private final SchedulerConfiguration schedulerConfiguration;

    @Inject
    public V0SpecPodFactory(KubePodConfiguration configuration,
                            ApplicationSlaManagementService capacityGroupManagement,
                            PodAffinityFactory podAffinityFactory,
                            TaintTolerationFactory taintTolerationFactory,
                            TopologyFactory topologyFactory,
                            ContainerEnvFactory containerEnvFactory,
                            PodContainerInfoFactory podContainerInfoFactory,
                            SchedulerConfiguration schedulerConfiguration) {
        this.configuration = configuration;
        this.capacityGroupManagement = capacityGroupManagement;
        this.podAffinityFactory = podAffinityFactory;
        this.taintTolerationFactory = taintTolerationFactory;
        this.topologyFactory = topologyFactory;
        this.containerEnvFactory = containerEnvFactory;
        this.podContainerInfoFactory = podContainerInfoFactory;
        this.schedulerConfiguration = schedulerConfiguration;
    }

    @Override
    public V1Pod buildV1Pod(Job<?> job, Task task, boolean useKubeScheduler) {
        String taskId = task.getId();
        TitanProtos.ContainerInfo containerInfo = podContainerInfoFactory.buildContainerInfo(job, task, true);
        Map<String, String> annotations = KubePodUtil.createPodAnnotations(job, task, containerInfo.toByteArray(),
                containerInfo.getPassthroughAttributesMap(), configuration.isJobDescriptorAnnotationEnabled());

        Pair<V1Affinity, Map<String, String>> affinityWithMetadata = podAffinityFactory.buildV1Affinity(job, task);
        annotations.putAll(affinityWithMetadata.getRight());
        annotations.putAll(createPlatformSidecarAnnotations(job));

        Map<String, String> labels = new HashMap<>();
        labels.put(KubeConstants.POD_LABEL_JOB_ID, job.getId());
        labels.put(KubeConstants.POD_LABEL_TASK_ID, taskId);
        JobManagerUtil.getRelocationBinpackMode(job).ifPresent(mode -> labels.put(KubeConstants.POD_LABEL_RELOCATION_BINPACK, mode));

        String capacityGroup = JobManagerUtil.getCapacityGroupDescriptorName(job.getJobDescriptor(), capacityGroupManagement).toLowerCase();
        labels.put(KubeConstants.LABEL_CAPACITY_GROUP, capacityGroup);

        V1ObjectMeta metadata = new V1ObjectMeta()
                .name(taskId)
                .annotations(annotations)
                .labels(labels);

        V1Container container = new V1Container()
                .name(taskId)
                .image("imageIsInContainerInfo")
                .env(toV1EnvVar(containerEnvFactory.buildContainerEnv(job, task)))
                .resources(buildV1ResourceRequirements(job.getJobDescriptor().getContainer().getContainerResources()))
                .volumeMounts(KubePodUtil.buildV1VolumeMounts(job.getJobDescriptor().getContainer().getVolumeMounts()));

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
                .tolerations(taintTolerationFactory.buildV1Toleration(job, task, useKubeScheduler))
                .topologySpreadConstraints(topologyFactory.buildTopologySpreadConstraints(job));

        //  If kube scheduler is not enabled then the node name needs to be explicitly set
        if (!useKubeScheduler) {
            spec.setNodeName(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID));
        }

        // V0 Pods use annotations to set EBS stuff. V1 pods use real volume/volumeMounts
        annotations.putAll(createEbsPodAnnotations(job, task));

        return new V1Pod().metadata(metadata).spec(spec);
    }

    @VisibleForTesting
    V1ResourceRequirements buildV1ResourceRequirements(ContainerResources containerResources) {
        Map<String, Quantity> requests = new HashMap<>();
        Map<String, Quantity> limits = new HashMap<>();

        requests.put(RESOURCE_CPU, new Quantity(String.valueOf(containerResources.getCpu())));
        limits.put(RESOURCE_CPU, new Quantity(String.valueOf(containerResources.getCpu())));

        requests.put(RESOURCE_GPU, new Quantity(String.valueOf(containerResources.getGpu())));
        limits.put(RESOURCE_GPU, new Quantity(String.valueOf(containerResources.getGpu())));

        Quantity memory;
        Quantity disk;
        Quantity network;

        memory = new Quantity(containerResources.getMemoryMB() + "Mi");
        disk = new Quantity(containerResources.getDiskMB() + "Mi");
        network = new Quantity(containerResources.getNetworkMbps() + "M");

        requests.put(RESOURCE_MEMORY, memory);
        limits.put(RESOURCE_MEMORY, memory);

        requests.put(RESOURCE_EPHERMERAL_STORAGE, disk);
        limits.put(RESOURCE_EPHERMERAL_STORAGE, disk);

        requests.put(RESOURCE_NETWORK, network);
        limits.put(RESOURCE_NETWORK, network);

        return new V1ResourceRequirements().requests(requests).limits(limits);
    }

    private List<V1Container> buildV1ExtraContainers(List<BasicContainer> extraContainers) {
        if (extraContainers == null) {
            return Collections.emptyList();
        }
        return extraContainers.stream().map(this::buildV1ExtraContainer).collect(Collectors.toList());
    }

    private V1Container buildV1ExtraContainer(BasicContainer extraContainer) {
        return new V1Container()
                .name(extraContainer.getName())
                .command(extraContainer.getEntryPoint())
                .args(extraContainer.getCommand())
                .image(KubePodUtil.buildImageString(configuration.getRegistryUrl(), extraContainer.getImage()))
                .env(toV1EnvVar(extraContainer.getEnv()))
                .imagePullPolicy(DEFAULT_IMAGE_PULL_POLICY)
                .volumeMounts(KubePodUtil.buildV1VolumeMounts(extraContainer.getVolumeMounts()));
    }
}
