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

import java.util.Collections;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.master.mesos.kubeapiserver.direct.taint.TaintTolerationFactory;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.titanframework.messages.TitanProtos.ContainerInfo;
import org.junit.Test;

import static com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultTaskToPodConverter.S3_WRITER_ROLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class DefaultTaskToPodConverterTest {

    private final DirectKubeConfiguration configuration = Archaius2Ext.newConfiguration(DirectKubeConfiguration.class,
            "titusMaster.directKube.defaultS3WriterRole", "defaultWriter"
    );

    private final PodAffinityFactory podAffinityFactory = mock(PodAffinityFactory.class);

    private final TaintTolerationFactory taintTolerationFactory = mock(TaintTolerationFactory.class);

    private final DefaultTaskToPodConverter converter = new DefaultTaskToPodConverter(
            configuration,
            podAffinityFactory,
            taintTolerationFactory
    );

    @Test
    public void testDefaultWriterRoleAssignment() {
        ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
        converter.appendS3WriterRole(containerInfoBuilder, JobGenerator.oneBatchJob());
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(S3_WRITER_ROLE, "defaultWriter");
    }

    @Test
    public void testCustomBucketWriterRoleAssignment() {
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
        converter.appendS3WriterRole(containerInfoBuilder, job);
        assertThat(containerInfoBuilder.getPassthroughAttributesMap()).containsEntry(
                S3_WRITER_ROLE,
                job.getJobDescriptor().getContainer().getSecurityProfile().getIamRole()
        );
    }
}