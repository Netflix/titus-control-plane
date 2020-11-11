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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo.S3LogLocation;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.master.mesos.kubeapiserver.direct.env.ContainerEnvs;
import com.netflix.titus.master.mesos.kubeapiserver.direct.taint.TaintTolerationFactory;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1TopologySpreadConstraint;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.titanframework.messages.TitanProtos.ContainerInfo;
import org.junit.Test;

import static com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultTaskToPodConverter.S3_BUCKET_NAME;
import static com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultTaskToPodConverter.S3_WRITER_ROLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTaskToPodConverterTest {

    private static final S3LogLocation DEFAULT_S3_LOG_LOCATION = new S3LogLocation(
            "myAccount", "myAccountId", "myRegion", "defaultBucket", "key"
    );

    private final DirectKubeConfiguration configuration = mock(DirectKubeConfiguration.class);

    private final PodAffinityFactory podAffinityFactory = mock(PodAffinityFactory.class);

    private final TaintTolerationFactory taintTolerationFactory = mock(TaintTolerationFactory.class);

    private final LogStorageInfo<Task> logStorageInfo = mock(LogStorageInfo.class);

    private final DefaultTaskToPodConverter converter = new DefaultTaskToPodConverter(
            configuration,
            podAffinityFactory,
            taintTolerationFactory,
            ContainerEnvs.getDefaultFactory(),
            logStorageInfo
    );

    @Test
    public void testDefaultWriterRoleAssignment() {
        BatchJobTask task = JobGenerator.oneBatchTask();
        when(logStorageInfo.getS3LogLocation(task, false)).thenReturn(Optional.of(DEFAULT_S3_LOG_LOCATION));
        when(configuration.getDefaultS3WriterRole()).thenReturn("defaultWriter");
        when(configuration.isDefaultS3WriterRoleEnabled()).thenReturn(true);

        ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
        converter.appendS3WriterRole(containerInfoBuilder, JobGenerator.oneBatchJob(), task);

        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(S3_WRITER_ROLE, "defaultWriter");
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(S3_BUCKET_NAME, "defaultBucket");
    }

    @Test
    public void testNullDefaultWriterRoleAssignment() {
        ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
        converter.appendS3WriterRole(containerInfoBuilder, JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
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
        converter.appendS3WriterRole(containerInfoBuilder, job, JobGenerator.oneBatchTask());
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
        V1ResourceRequirements podResources = converter.buildV1ResourceRequirements(containerResources);
        assertThat(podResources.getLimits().get("memory").toSuffixedString()).isEqualTo("2");
        assertThat(podResources.getLimits().get("ephemeral-storage").toSuffixedString()).isEqualTo("4");
        assertThat(podResources.getLimits().get("titus/network").toSuffixedString()).isEqualTo("128");

        // Bytes
        when(configuration.isBytePodResourceEnabled()).thenReturn(true);
        podResources = converter.buildV1ResourceRequirements(containerResources);
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

        assertThat(converter.buildV1VolumeInfo(job, task))
                .isPresent()
                .hasValueSatisfying(pair -> {
                    V1Volume v1Volume = pair.getLeft();
                    V1VolumeMount v1VolumeMount = pair.getRight();

                    assertThat(v1Volume.getName()).isEqualTo(volName2);
                    assertThat(v1Volume.getAwsElasticBlockStore().getVolumeID()).isEqualTo(volName2);
                    assertThat(v1Volume.getAwsElasticBlockStore().getFsType()).isEqualTo(fsType);

                    assertThat(v1VolumeMount.getName()).isEqualTo(volName2);
                    assertThat(v1VolumeMount.getMountPath()).isEqualTo(mountPath);
                    assertThat(v1VolumeMount.getReadOnly()).isFalse();
                });
    }

    private void testConstraintNameIsCaseInsensitive(Job<BatchJobExt> job) {
        List<V1TopologySpreadConstraint> constraints = converter.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getTopologyKey()).isEqualTo(KubeConstants.NODE_LABEL_ZONE);
    }
}