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

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BasicContainer;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.NetworkConfiguration;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.SharedContainerVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.Volume;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.kubernetes.client.KubeModelConverters;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.KubePodUtil;
import com.netflix.titus.master.kubernetes.pod.affinity.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.DefaultAggregatingContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.PodContainerInfoFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.TitusProvidedContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.legacy.UserProvidedContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.kubernetes.pod.topology.TopologyFactory;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1FlexVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.titanframework.messages.TitanProtos.ContainerInfo;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V0SpecPodFactoryTest {

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);

    private final MasterConfiguration jobCoordinatorConfiguration = mock(MasterConfiguration.class);

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);

    private final ApplicationSlaManagementService capacityGroupManagement = mock(ApplicationSlaManagementService.class);

    private final PodAffinityFactory podAffinityFactory = mock(PodAffinityFactory.class);

    private final TaintTolerationFactory taintTolerationFactory = mock(TaintTolerationFactory.class);

    private final TopologyFactory topologyFactory = mock(TopologyFactory.class);

    private final PodContainerInfoFactory podContainerInfoFactory = mock(PodContainerInfoFactory.class);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final DefaultAggregatingContainerEnvFactory defaultAggregatingContainerEnvFactory =
            new DefaultAggregatingContainerEnvFactory(titusRuntime,
                    UserProvidedContainerEnvFactory.getInstance(),
                    TitusProvidedContainerEnvFactory.getInstance());

    private V0SpecPodFactory podFactory;

    @Before
    public void setUp() throws Exception {
        podFactory = new V0SpecPodFactory(
                configuration,
                capacityGroupManagement,
                podAffinityFactory,
                taintTolerationFactory,
                topologyFactory,
                defaultAggregatingContainerEnvFactory,
                podContainerInfoFactory,
                schedulerConfiguration
        );
        when(podContainerInfoFactory.buildContainerInfo(any(), any(), anyBoolean())).thenReturn(ContainerInfo.newBuilder().build());
    }

    @Test
    public void testEbsVolumeInfo() {
        String volName1 = "vol-1";
        String volName2 = "vol-2";
        String fsType = "xfs";
        String mountPath = "/mnt";
        EbsVolume.MountPerm mountPerm = EbsVolume.MountPerm.RW;
        EbsVolume vol1 = EbsVolume.newBuilder()
                .withVolumeId(volName1)
                .withMountPath(mountPath)
                .withMountPermissions(mountPerm)
                .withFsType(fsType)
                .withVolumeAvailabilityZone("us-east-1c")
                .withVolumeCapacityGB(10)
                .build();
        EbsVolume vol2 = EbsVolume.newBuilder()
                .withVolumeId(volName2)
                .withMountPath(mountPath)
                .withMountPermissions(mountPerm)
                .withFsType(fsType)
                .withVolumeAvailabilityZone("us-east-1d")
                .withVolumeCapacityGB(20)
                .build();

        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = Job.<BatchJobExt>newBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder()
                        .withContainer(job.getJobDescriptor().getContainer().toBuilder()
                                .withContainerResources(job.getJobDescriptor().getContainer().getContainerResources().toBuilder()
                                        .withEbsVolumes(Arrays.asList(vol1, vol2))
                                        .build())
                                .build())
                        .build())
                .build();
        Task task = JobGenerator.batchTasks(job).getValue();
        task = task.toBuilder()
                .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, volName2)
                .build();

        String pvcName = KubeModelConverters.toPvcName(volName2, task.getId());
        assertThat(KubePodUtil.buildV1VolumeInfo(job, task))
                .isPresent()
                .hasValueSatisfying(pair -> {
                    V1Volume v1Volume = pair.getLeft();
                    V1VolumeMount v1VolumeMount = pair.getRight();

                    assertThat(v1Volume.getName()).isEqualTo(volName2);
                    assertThat(v1Volume.getPersistentVolumeClaim().getClaimName()).isEqualTo(pvcName);

                    assertThat(v1VolumeMount.getName()).isEqualTo(volName2);
                    assertThat(v1VolumeMount.getMountPath()).isEqualTo(mountPath);
                    assertThat(v1VolumeMount.getReadOnly()).isFalse();
                });
    }

    @Test
    public void testContainerInfoEnvVar() throws Exception {
        String testEnvVarName = "TEST_ENV_VAR_NAME";
        String testEnvVarValue = "TEST_ENV_VAR_VALUE";
        String testConflictingEnvVarName = KubeConstants.POD_ENV_NETFLIX_EXECUTOR;
        String testConflictingEnvVarValue = "titus";

        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withContainer(
                job.getJobDescriptor().getContainer().toBuilder()
                        .withEnv(ImmutableMap.of(testEnvVarName, testEnvVarValue,
                                testConflictingEnvVarName, testConflictingEnvVarValue))
                        .build()
        ).build()).build();
        BatchJobTask batchJobTask = JobGenerator.batchTasks(job).getValue();
        when(jobCoordinatorConfiguration.isContainerInfoEnvEnabled()).thenReturn(false);
        when(podAffinityFactory.buildV1Affinity(job, batchJobTask)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));

        V1Pod v1Pod = podFactory.buildV1Pod(job, batchJobTask, true, false);
        String encodedContainerInfo = v1Pod.getMetadata().getAnnotations().get("containerInfo");
        ContainerInfo containerInfo = ContainerInfo.parseFrom(Base64.getDecoder().decode(encodedContainerInfo.getBytes()));
        assertThat(containerInfo.getUserProvidedEnvMap()).isEmpty();
        assertThat(containerInfo.getTitusProvidedEnvMap()).isEmpty();

        verifyEnvVar(v1Pod, testEnvVarName, testEnvVarValue);
        verifyEnvVar(v1Pod, testConflictingEnvVarName, testConflictingEnvVarValue);

        assertThat(titusRuntime.getRegistry().counter("titus.aggregatingContainerEnv.conflict", "var_name", KubeConstants.POD_ENV_NETFLIX_EXECUTOR).count()).isOne();
    }

    @Test
    public void testCapacityGroupAssignment() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        BatchJobTask task = JobGenerator.oneBatchTask();

        job = job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withCapacityGroup("myGroup").build()).build();
        when(capacityGroupManagement.getApplicationSLA("myGroup")).thenReturn(ApplicationSLA.newBuilder()
                .withAppName("myGroup")
                .build()
        );

        when(podAffinityFactory.buildV1Affinity(job, task)).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));
        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);

        assertThat(pod.getMetadata().getLabels()).containsEntry(
                KubeConstants.LABEL_CAPACITY_GROUP, "mygroup"
        );
    }

    private void verifyEnvVar(V1Pod v1Pod, String name, String value) {
        List<V1EnvVar> v1EnvVars = Objects.requireNonNull(v1Pod.getSpec()).getContainers().get(0).getEnv();
        assert v1EnvVars != null;
        Optional<V1EnvVar> envVarOptional = v1EnvVars.stream().filter(v1EnvVar -> v1EnvVar.getName().equals(name)).findFirst();
        assertThat(envVarOptional.isPresent()).isTrue();
        assertThat(envVarOptional.get().getValue()).isEqualTo(value);
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
    public void relocationLabel() {
        Job<ServiceJobExt> job = JobGenerator.oneServiceJob();
        Job<ServiceJobExt> selfManagedJob = job.toBuilder().withJobDescriptor(job.getJobDescriptor().but(
                jd -> jd.getDisruptionBudget().toBuilder()
                        .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder().build())
        )).build();
        ServiceJobTask task = JobGenerator.oneServiceTask();
        when(podAffinityFactory.buildV1Affinity(any(), eq(task))).thenReturn(Pair.of(new V1Affinity(), new HashMap<>()));

        V1Pod pod = podFactory.buildV1Pod(job, task, true, false);
        assertThat(pod.getMetadata().getLabels()).doesNotContainKey(KubeConstants.POD_LABEL_RELOCATION_BINPACK);
        V1Pod selfManagedPod = podFactory.buildV1Pod(selfManagedJob, task, true, false);
        assertThat(selfManagedPod.getMetadata().getLabels()).containsEntry(KubeConstants.POD_LABEL_RELOCATION_BINPACK, "SelfManaged");
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

        List<V1Container> containers = pod.getSpec().getContainers();
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

