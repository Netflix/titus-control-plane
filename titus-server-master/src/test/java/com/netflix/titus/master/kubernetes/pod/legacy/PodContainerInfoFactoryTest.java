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
import java.util.Optional;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.titanframework.messages.TitanProtos;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.kubernetes.pod.legacy.PodContainerInfoFactory.S3_BUCKET_NAME;
import static com.netflix.titus.master.kubernetes.pod.legacy.PodContainerInfoFactory.S3_WRITER_ROLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodContainerInfoFactoryTest {

    private static final LogStorageInfo.S3LogLocation DEFAULT_S3_LOG_LOCATION = new LogStorageInfo.S3LogLocation(
            "myAccount", "myAccountId", "myRegion", "defaultBucket", "key"
    );

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);
    private final LogStorageInfo<Task> logStorageInfo = mock(LogStorageInfo.class);

    private PodContainerInfoFactory podContainerInfoFactory;

    @Before
    public void setUp() throws Exception {
        podContainerInfoFactory = new PodContainerInfoFactory(configuration, logStorageInfo);
    }

    @Test
    public void testDefaultWriterRoleAssignment() {
        BatchJobTask task = JobGenerator.oneBatchTask();
        when(logStorageInfo.getS3LogLocation(task, false)).thenReturn(Optional.of(DEFAULT_S3_LOG_LOCATION));
        when(configuration.getDefaultS3WriterRole()).thenReturn("defaultWriter");
        when(configuration.isDefaultS3WriterRoleEnabled()).thenReturn(true);

        TitanProtos.ContainerInfo.Builder containerInfoBuilder = TitanProtos.ContainerInfo.newBuilder();
        podContainerInfoFactory.appendS3WriterRole(containerInfoBuilder, JobGenerator.oneBatchJob(), task);

        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(S3_WRITER_ROLE, "defaultWriter");
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(S3_BUCKET_NAME, "defaultBucket");
    }

    @Test
    public void testNullDefaultWriterRoleAssignment() {
        TitanProtos.ContainerInfo.Builder containerInfoBuilder = TitanProtos.ContainerInfo.newBuilder();
        podContainerInfoFactory.appendS3WriterRole(containerInfoBuilder, JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).doesNotContainKey(S3_WRITER_ROLE);
    }

    @Test
    public void testCustomBucketWriterRoleAssignment() {
        when(configuration.isDefaultS3WriterRoleEnabled()).thenReturn(true);

        TitanProtos.ContainerInfo.Builder containerInfoBuilder = TitanProtos.ContainerInfo.newBuilder();
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
        podContainerInfoFactory.appendS3WriterRole(containerInfoBuilder, job, JobGenerator.oneBatchTask());
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(
                S3_WRITER_ROLE,
                job.getJobDescriptor().getContainer().getSecurityProfile().getIamRole()
        );
    }
}