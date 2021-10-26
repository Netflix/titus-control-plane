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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.netflix.titus.api.jobmanager.model.job.BasicContainer;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.NetworkConfiguration;
import com.netflix.titus.api.jobmanager.model.job.PlatformSidecar;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.VolumeMount;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.volume.SharedContainerVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.volume.Volume;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.affinity.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.env.DefaultPodEnvFactory;
import com.netflix.titus.master.kubernetes.pod.env.PodEnvFactory;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.kubernetes.pod.topology.TopologyFactory;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1FlexVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V1SpecPodFactoryTest {

    String NONE_MOUNT_PROPAGATION = com.netflix.titus.grpc.protogen.VolumeMount.MountPropagation.MountPropagationNone.toString();

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);

    private final ApplicationSlaManagementService capacityGroupManagement = mock(ApplicationSlaManagementService.class);

    private final PodAffinityFactory podAffinityFactory = mock(PodAffinityFactory.class);

    private final TaintTolerationFactory taintTolerationFactory = mock(TaintTolerationFactory.class);

    private final PodEnvFactory podEnvFactory = new DefaultPodEnvFactory();

    private final TopologyFactory topologyFactory = mock(TopologyFactory.class);

    private final LogStorageInfo<Task> logStorageInfo = mock(LogStorageInfo.class);

    private V1SpecPodFactory podFactory;

    @Before
    public void setUp() throws Exception {
        podFactory = new V1SpecPodFactory(
                configuration,
                capacityGroupManagement,
                podAffinityFactory,
                taintTolerationFactory,
                topologyFactory,
                podEnvFactory,
                logStorageInfo,
                schedulerConfiguration
        );
    }

    @Test
    public void multipleContainers() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        Image testImage = Image.newBuilder().withName("testImage").withDigest("123").build();
        List<BasicContainer> extraContainers = Arrays.asList(
                new BasicContainer("extraContainer1", testImage, Collections.emptyList(), Collections.emptyList(), new HashMap<>(), Collections.emptyList()),
                new BasicContainer("extraContainer2", testImage, Collections.emptyList(), Collections.emptyList(), new HashMap<>(), Collections.emptyList())
        );
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withExtraContainers(extraContainers).build()).build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        List<V1Container> containers = Objects.requireNonNull(pod.getSpec()).getContainers();
        // 3 containers here, 1 from the main container, 2 from the extras
        assertThat(containers.size()).isEqualTo(1 + extraContainers.size());
    }

    @Test
    public void podMainContainerHasVolumeMounts() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        List<VolumeMount> volumeMounts = Arrays.asList(
                new VolumeMount("volume1", "", NONE_MOUNT_PROPAGATION, false, ""),
                new VolumeMount("volume2", "", NONE_MOUNT_PROPAGATION, false, "")
        );
        Container container = job.getJobDescriptor().getContainer().toBuilder().withVolumeMounts(volumeMounts).build();
        job = job.toBuilder().withJobDescriptor(
                job.getJobDescriptor().toBuilder().withContainer(container).build()
        ).build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        V1Container mainContainer = pod.getSpec().getContainers().get(0);
        List<V1VolumeMount> mounts = mainContainer.getVolumeMounts();

        // dev-shm is third volume
        assertThat(mounts.size()).isEqualTo(3);
        assertThat(mounts.get(0).getName()).isEqualTo("volume1");
        assertThat(mounts.get(1).getName()).isEqualTo("volume2");
    }

    @Test
    public void podGetsSharedFlexVolumes() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        List<Volume> volumes = Arrays.asList(
                new Volume("volume1", new SharedContainerVolumeSource("main", "/main-root")),
                new Volume("volume2", new SharedContainerVolumeSource("main", "/main-root"))
        );
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withVolumes(volumes).build()).build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        List<V1Volume> podVolumes = Objects.requireNonNull(pod.getSpec()).getVolumes();
        // dev-shm is third volume
        assertThat(podVolumes.size()).isEqualTo(3);
        V1Volume mainSharedVolume = podVolumes.get(0);
        assertThat(mainSharedVolume.getName()).isEqualTo("volume1");
        V1FlexVolumeSource flexVolume = mainSharedVolume.getFlexVolume();
        assertThat(flexVolume.getDriver()).isEqualTo("SharedContainerVolumeSource");
        Map<String, String> flexVolumeOptions = flexVolume.getOptions();
        assertThat(flexVolumeOptions.get("sourceContainer")).isEqualTo("main");
        assertThat(flexVolumeOptions.get("sourcePath")).isEqualTo("/main-root");
    }

    @Test
    public void podHasSidecarAnnotations() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        String json_args = "{\"foo\":true,\"bar\":3.0}";
        List<PlatformSidecar> platformSidecars = Arrays.asList(
                new PlatformSidecar("mysidecar", "stable", json_args)
        );
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withPlatformSidecars(platformSidecars).build()).build();

        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        Map<String, String> annotations = pod.getMetadata().getAnnotations();
        String expectedSidecarAnnotation = "mysidecar" + KubeConstants.PLATFORM_SIDECAR_SUFFIX;
        assertThat(annotations.get(expectedSidecarAnnotation)).isEqualTo("true");

        String expectedChannelAnnotation = "mysidecar" + KubeConstants.PLATFORM_SIDECAR_CHANNEL_SUFFIX;
        assertThat(annotations.get(expectedChannelAnnotation)).isEqualTo("stable");

        String expectedArgsAnnotation = "mysidecar" + KubeConstants.PLATFORM_SIDECAR_ARGS_SUFFIX;
        assertThat(annotations.get(expectedArgsAnnotation)).isEqualTo(json_args);
    }

    @Test
    public void testNetworkConfigurationRespectsBeingSet() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        NetworkConfiguration networkConfiguration = new NetworkConfiguration(3);
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withNetworkConfiguration(networkConfiguration).build()).build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));

        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);
        String networkModeAnnotationValue = pod.getMetadata().getAnnotations().get("network.netflix.com/network-mode");
        assertThat(networkModeAnnotationValue).isEqualTo("Ipv6AndIpv4Fallback");
    }

    @Test
    public void testNetworkConfigurationIsNotNullAndSetToUnknownByDefault() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().build()).build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));

        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);
        String networkModeAnnotationValue = pod.getMetadata().getAnnotations().get("network.netflix.com/network-mode");
        assertThat(networkModeAnnotationValue).isEqualTo("UnknownNetworkMode");
    }

    @Test
    public void testEFSMountsGetTransformedSafely() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();

        EfsMount newEfsMount = new EfsMount("1.2.3.4", "/mountpoint", EfsMount.MountPerm.RO, "/relative");
        Container newContainer = job.getJobDescriptor().getContainer();
        ContainerResources newContainerResources = newContainer.getContainerResources();
        Container newContainerWithEFS = newContainer.toBuilder().withContainerResources(newContainerResources.newBuilder()
                .withEfsMounts(Collections.singletonList(newEfsMount))
                .build()).build();

        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder()
                .withContainer(newContainerWithEFS).build())
                .build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        // Part 1: the volume section needs to be well-formed
        List<V1Volume> volumes = pod.getSpec().getVolumes();
        assertThat(volumes.size()).isEqualTo(2); // one for nfs, one for shm
        V1Volume v1NFSVolume = volumes.get(0);
        assertThat(v1NFSVolume.getName()).isEqualTo("1-2-3-4-relative");
        assertThat(v1NFSVolume.getNfs().getServer()).isEqualTo("1.2.3.4");
        assertThat(v1NFSVolume.getNfs().getPath()).isEqualTo("/relative");
        assertThat(v1NFSVolume.getNfs().getReadOnly()).isEqualTo(true);

        // Part 2: the volume mount section needs to applied to the first container in the podspec
        List<V1VolumeMount> vms = pod.getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(vms.size()).isEqualTo(2); // one for nfs, one for shm
        V1VolumeMount v1NFSvm = vms.get(0);
        assertThat(v1NFSvm.getName()).isEqualTo("1-2-3-4-relative");
        assertThat(v1NFSvm.getMountPath()).isEqualTo("/mountpoint");
    }

    @Test
    public void testEFSMountsHandlesDuplicateVolumes() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();

        EfsMount newEfsMount = new EfsMount("1.2.3.4", "/mountpoint", EfsMount.MountPerm.RO, "/relative");
        EfsMount newEfsMount2 = new EfsMount("1.2.3.4", "/mountpoint2", EfsMount.MountPerm.RO, "/relative");

        Container newContainer = job.getJobDescriptor().getContainer();
        ContainerResources newContainerResources = newContainer.getContainerResources();
        Container newContainerWithEFS = newContainer.toBuilder().withContainerResources(newContainerResources.newBuilder()
                .withEfsMounts(Arrays.asList(newEfsMount, newEfsMount2))
                .build()).build();

        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder()
                .withContainer(newContainerWithEFS).build())
                .build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        // Part 1: There should only be *one* EFS volume to share
        List<V1Volume> volumes = pod.getSpec().getVolumes();
        assertThat(volumes.size()).isEqualTo(2); // one for nfs, one for shm
        V1Volume v1NFSVolume = volumes.get(0);
        assertThat(v1NFSVolume.getName()).isEqualTo("1-2-3-4-relative");
        assertThat(v1NFSVolume.getNfs().getServer()).isEqualTo("1.2.3.4");
        assertThat(v1NFSVolume.getNfs().getPath()).isEqualTo("/relative");
        assertThat(v1NFSVolume.getNfs().getReadOnly()).isEqualTo(true);

        // Part 2: there should be *two* volume mounts, both sharing the volume
        List<V1VolumeMount> vms = pod.getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(vms.size()).isEqualTo(3); // 2 for nfs, one for shm
        V1VolumeMount v1NFSvm = vms.get(0);
        assertThat(v1NFSvm.getName()).isEqualTo("1-2-3-4-relative");
        assertThat(v1NFSvm.getMountPath()).isEqualTo("/mountpoint");
        V1VolumeMount v1NFSvm2 = vms.get(1);
        assertThat(v1NFSvm2.getName()).isEqualTo("1-2-3-4-relative");
        assertThat(v1NFSvm2.getMountPath()).isEqualTo("/mountpoint2");
    }

}