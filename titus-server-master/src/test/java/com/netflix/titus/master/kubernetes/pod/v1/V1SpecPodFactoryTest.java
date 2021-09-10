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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.netflix.titus.api.jobmanager.model.job.BasicContainer;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.SharedContainerVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.Volume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.affinity.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.env.PodEnvFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.PodContainerInfoFactory;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.kubernetes.pod.topology.TopologyFactory;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.kubernetes.pod.env.DefaultPodEnvFactory;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1FlexVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Volume;
import io.titanframework.messages.TitanProtos.ContainerInfo;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V1SpecPodFactoryTest {

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);

    private final ApplicationSlaManagementService capacityGroupManagement = mock(ApplicationSlaManagementService.class);

    private final PodAffinityFactory podAffinityFactory = mock(PodAffinityFactory.class);

    private final TaintTolerationFactory taintTolerationFactory = mock(TaintTolerationFactory.class);

    private final  PodEnvFactory podEnvFactory = new DefaultPodEnvFactory();

    private final TopologyFactory topologyFactory = mock(TopologyFactory.class);

    private final PodContainerInfoFactory podContainerInfoFactory = mock(PodContainerInfoFactory.class);

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
                podContainerInfoFactory
        );
        when(podContainerInfoFactory.buildContainerInfo(any(), any(), anyBoolean())).thenReturn(ContainerInfo.newBuilder().build());
    }

    @Test
    public void multipleContainers() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        Image testImage = Image.newBuilder().withName("testImage").withDigest("123").build();
        List<BasicContainer> extraContainers = Arrays.asList(
                new BasicContainer("extraContainer1", testImage, null, null, new HashMap<>()),
                new BasicContainer("extraContainer2", testImage, null, null, new HashMap<>())
        );
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withExtraContainers(extraContainers).build()).build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        List<V1Container> containers = Objects.requireNonNull(pod.getSpec()).getContainers();
        // 3 containers here, 1 from the main container, 2 from the extras
        assertThat(containers.size()).isEqualTo(1 + extraContainers.size());
    }

    @Test
    public void podGetsSharedFlexVolumes() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();
        List<Volume> volumes = Arrays.asList(
                new Volume("volume1", new SharedContainerVolumeSource("main", "/main-root")),
                new Volume("volume2", null)
        );
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withVolumes(volumes).build()).build();
        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        List<V1Volume> podVolumes = Objects.requireNonNull(pod.getSpec()).getVolumes();
        assertThat(podVolumes.size()).isEqualTo(2);
        V1Volume mainSharedVolume = podVolumes.get(0);
        assertThat(mainSharedVolume.getName()).isEqualTo("volume1");
        V1FlexVolumeSource flexVolume = mainSharedVolume.getFlexVolume();
        assertThat(flexVolume.getDriver()).isEqualTo("SharedContainerVolumeSource");
        Map<String, String> flexVolumeOptions = flexVolume.getOptions();
        assertThat(flexVolumeOptions.get("sourceContainer")).isEqualTo("main");
        assertThat(flexVolumeOptions.get("sourcePath")).isEqualTo("/main-root");
    }

}