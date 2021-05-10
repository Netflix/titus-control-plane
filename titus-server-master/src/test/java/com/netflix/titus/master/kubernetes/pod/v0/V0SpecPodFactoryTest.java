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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo.S3LogLocation;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.PodAffinityFactory;
import com.netflix.titus.master.kubernetes.pod.env.DefaultAggregatingContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.env.TitusProvidedContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.env.UserProvidedContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.taint.TaintTolerationFactory;
import com.netflix.titus.master.mesos.kubeapiserver.direct.KubeModelConverters;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1TopologySpreadConstraint;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.titanframework.messages.TitanProtos.ContainerInfo;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.kubernetes.pod.v0.V0SpecPodFactory.S3_BUCKET_NAME;
import static com.netflix.titus.master.kubernetes.pod.v0.V0SpecPodFactory.S3_WRITER_ROLE;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.officeHourTimeWindow;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V0SpecPodFactoryTest {

    private static final S3LogLocation DEFAULT_S3_LOG_LOCATION = new S3LogLocation(
            "myAccount", "myAccountId", "myRegion", "defaultBucket", "key"
    );

    private static final DisruptionBudget PERCENTAGE_OF_HEALTH_POLICY = budget(
            percentageOfHealthyPolicy(95), unlimitedRate(), Collections.singletonList(officeHourTimeWindow())
    );

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);

    private final MasterConfiguration jobCoordinatorConfiguration = mock(MasterConfiguration.class);

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);

    private final ApplicationSlaManagementService capacityGroupManagement = mock(ApplicationSlaManagementService.class);

    private final PodAffinityFactory podAffinityFactory = mock(PodAffinityFactory.class);

    private final TaintTolerationFactory taintTolerationFactory = mock(TaintTolerationFactory.class);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final DefaultAggregatingContainerEnvFactory defaultAggregatingContainerEnvFactory =
            new DefaultAggregatingContainerEnvFactory(titusRuntime,
                    UserProvidedContainerEnvFactory.getInstance(),
                    TitusProvidedContainerEnvFactory.getInstance());

    private final LogStorageInfo<Task> logStorageInfo = mock(LogStorageInfo.class);

    private V0SpecPodFactory podFactory;

    @Before
    public void setUp() throws Exception {
        podFactory = new V0SpecPodFactory(
                configuration,
                jobCoordinatorConfiguration,
                capacityGroupManagement,
                podAffinityFactory,
                taintTolerationFactory,
                defaultAggregatingContainerEnvFactory,
                logStorageInfo,
                schedulerConfiguration
        );
        when(configuration.getDisabledJobSpreadingPattern()).thenReturn("NONE");
    }

    @Test
    public void testDefaultWriterRoleAssignment() {
        BatchJobTask task = JobGenerator.oneBatchTask();
        when(logStorageInfo.getS3LogLocation(task, false)).thenReturn(Optional.of(DEFAULT_S3_LOG_LOCATION));
        when(configuration.getDefaultS3WriterRole()).thenReturn("defaultWriter");
        when(configuration.isDefaultS3WriterRoleEnabled()).thenReturn(true);

        ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
        podFactory.appendS3WriterRole(containerInfoBuilder, JobGenerator.oneBatchJob(), task);

        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(S3_WRITER_ROLE, "defaultWriter");
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(S3_BUCKET_NAME, "defaultBucket");
    }

    @Test
    public void testNullDefaultWriterRoleAssignment() {
        ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
        podFactory.appendS3WriterRole(containerInfoBuilder, JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).doesNotContainKey(S3_WRITER_ROLE);
    }

    @Test
    public void testCustomBucketWriterRoleAssignment() {
        when(configuration.isDefaultS3WriterRoleEnabled()).thenReturn(true);

        ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = job.toBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder()
                        .withContainer(job.getJobDescriptor().getContainer().toBuilder()
                                .withAttributes(Collections.singletonMap(
                                        JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME, "myOwnBucket"
                                )).build()
                        )
                        .build()
                )
                .build();
        podFactory.appendS3WriterRole(containerInfoBuilder, job, JobGenerator.oneBatchTask());
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(
                S3_WRITER_ROLE,
                job.getJobDescriptor().getContainer().getSecurityProfile().getIamRole()
        );
    }

    @Test
    public void testByteResourceUnits() {
        ContainerResources containerResources = ContainerResources.newBuilder()
                .withMemoryMB(2)
                .withDiskMB(4)
                .withNetworkMbps(128)
                .build();

        // Legacy
        when(configuration.isBytePodResourceEnabled()).thenReturn(false);
        V1ResourceRequirements podResources = podFactory.buildV1ResourceRequirements(containerResources);
        assertThat(podResources.getLimits().get("memory").toSuffixedString()).isEqualTo("2");
        assertThat(podResources.getLimits().get("ephemeral-storage").toSuffixedString()).isEqualTo("4");
        assertThat(podResources.getLimits().get("titus/network").toSuffixedString()).isEqualTo("128");

        // Bytes
        when(configuration.isBytePodResourceEnabled()).thenReturn(true);
        podResources = podFactory.buildV1ResourceRequirements(containerResources);
        assertThat(podResources.getLimits().get("memory").toSuffixedString()).isEqualTo("2Mi");
        assertThat(podResources.getLimits().get("ephemeral-storage").toSuffixedString()).isEqualTo("4Mi");
        assertThat(podResources.getLimits().get("titus/network").toSuffixedString()).isEqualTo("128M");
    }

    @Test
    public void testHardConstraintNameIsCaseInsensitive() {
        testConstraintNameIsCaseInsensitive(JobFunctions.appendHardConstraint(JobGenerator.oneBatchJob(), "ZoneBalance", "true"));
    }

    @Test
    public void testSoftConstraintNameIsCaseInsensitive() {
        testConstraintNameIsCaseInsensitive(JobFunctions.appendSoftConstraint(JobGenerator.oneBatchJob(), "ZoneBalance", "true"));
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
        assertThat(podFactory.buildV1VolumeInfo(job, task))
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

    private void testConstraintNameIsCaseInsensitive(Job<BatchJobExt> job) {
        List<V1TopologySpreadConstraint> constraints = podFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getTopologyKey()).isEqualTo(KubeConstants.NODE_LABEL_ZONE);
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

    @Test
    public void testBatchJobSpreading() {
        // By default no job spreading
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        List<V1TopologySpreadConstraint> constraints = podFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).isEmpty();

        // Enable via job attribute
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_ENABLED, "true");
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_MAX_SKEW, "10");
        constraints = podFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getMaxSkew()).isEqualTo(10);

        // And now add zone constraint
        job = JobFunctions.appendSoftConstraint(job, JobConstraints.ZONE_BALANCE, "true");
        constraints = podFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(2);
    }

    @Test
    public void testServiceJobSpreadingWithAvailabilityPercentageDisruptionBudget() {
        // By default no job spreading
        Job<ServiceJobExt> job = JobGenerator.serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
        job = JobFunctions.changeServiceJobCapacity(job, Capacity.newBuilder().withDesired(100).withMax(100).build());
        job = JobFunctions.changeDisruptionBudget(job, PERCENTAGE_OF_HEALTH_POLICY);
        List<V1TopologySpreadConstraint> constraints = podFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getMaxSkew()).isEqualTo(5);

        // And now add zone constraint
        job = JobFunctions.appendSoftConstraint(job, JobConstraints.ZONE_BALANCE, "true");
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_MAX_SKEW, "10");
        constraints = podFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(2);
        assertThat(constraints.get(1).getMaxSkew()).isEqualTo(10);

        // Disable via job attribute
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_ENABLED, "false");
        constraints = podFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
    }

    @Test
    public void testJobSpreadingDisabledConfiguration() {
        Job<ServiceJobExt> job = JobGenerator.serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
        assertThat(podFactory.buildTopologySpreadConstraints(job)).hasSize(1);

        when(configuration.getDisabledJobSpreadingPattern()).thenReturn(".*");
        assertThat(podFactory.buildTopologySpreadConstraints(job)).isEmpty();
    }

    private void verifyEnvVar(V1Pod v1Pod, String name, String value) {
        List<V1EnvVar> v1EnvVars = Objects.requireNonNull(v1Pod.getSpec()).getContainers().get(0).getEnv();
        assert v1EnvVars != null;
        Optional<V1EnvVar> envVarOptional = v1EnvVars.stream().filter(v1EnvVar -> v1EnvVar.getName().equals(name)).findFirst();
        assertThat(envVarOptional.isPresent()).isTrue();
        assertThat(envVarOptional.get().getValue()).isEqualTo(value);
    }
}
