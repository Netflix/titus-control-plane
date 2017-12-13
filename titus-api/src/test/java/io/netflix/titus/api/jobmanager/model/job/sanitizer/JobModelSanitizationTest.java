/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.util.Collections;
import java.util.Set;
import javax.validation.ConstraintViolation;

import com.google.common.collect.ImmutableMap;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.DefaultDecoder;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.MapConfig;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class JobModelSanitizationTest {

    private static final ResourceDimension MAX_CONTAINER_SIZE = new ResourceDimension(64, 16, 256_000_000, 256_000_000, 10_000);

    private static final MapConfig CONFIG = MapConfig.from(ImmutableMap.of(
            "titusMaster.job.configuration.defaultSecurityGroups", "sg-12345,sg-34567",
            "titusMaster.job.configuration.defaultIamRole", "iam-12345"
    ));

    private final JobConfiguration constraints = new ConfigProxyFactory(CONFIG, new DefaultDecoder(), new DefaultPropertyFactory(CONFIG))
            .newProxy(JobConfiguration.class);

    private EntitySanitizer entitySanitizer;

    @Before
    public void setUp() throws Exception {
        entitySanitizer = new JobSanitizerBuilder()
                .withJobConstrainstConfiguration(constraints)
                .withMaxContainerSizeResolver(capacityGroup -> MAX_CONTAINER_SIZE)
                .build();
    }

    @Test
    public void testValidBatchJob() throws Exception {
        Job<BatchJobExt> job = JobGenerator.batchJobs(oneTaskBatchJobDescriptor()).getValue();

        assertThat(entitySanitizer.validate(job)).isEmpty();
        assertThat(entitySanitizer.sanitize(job)).isEmpty();
    }

    @Test
    public void testBatchJobWithInvalidSecurityGroups() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = oneTaskBatchJobDescriptor();
        JobDescriptor<BatchJobExt> noSecurityProfileDescriptor = JobModel.newJobDescriptor(jobDescriptor)
                .withContainer(JobModel.newContainer(jobDescriptor.getContainer())
                        .withSecurityProfile(
                                JobModel.newSecurityProfile(jobDescriptor.getContainer().getSecurityProfile())
                                        .withSecurityGroups(Collections.singletonList("abcd"))
                                        .build())
                        .build()
                ).build();
        Job<BatchJobExt> job = JobGenerator.batchJobs(noSecurityProfileDescriptor).getValue();

        // Security group violation expected
        assertThat(entitySanitizer.validate(job)).hasSize(1);
    }

    @Test
    public void testBatchJobWithMissingImageTagAndDigest() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = oneTaskBatchJobDescriptor();
        JobDescriptor<BatchJobExt> noImageTagAndDigestDescriptor = JobModel.newJobDescriptor(jobDescriptor)
                .withContainer(JobModel.newContainer(jobDescriptor.getContainer())
                        .withImage(Image.newBuilder()
                                .withName("imageName")
                                .build()
                        )
                        .build()
                ).build();
        Job<BatchJobExt> job = JobGenerator.batchJobs(noImageTagAndDigestDescriptor).getValue();

        // Image digest and tag violation expected
        Set<ConstraintViolation<Job<BatchJobExt>>> violations = entitySanitizer.validate(job);
        assertThat(violations).hasSize(1);
        assertThat(violations.iterator().next().getMessage().contains("must specify a valid digest or tag"));
    }

    @Test
    public void testBatchJobWithIncompleteEfsDefinition() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = oneTaskBatchJobDescriptor();
        JobDescriptor<BatchJobExt> incompleteEfsDefinition = JobModel.newJobDescriptor(jobDescriptor)
                .withContainer(JobModel.newContainer(jobDescriptor.getContainer())
                        .withContainerResources(
                                JobModel.newContainerResources(jobDescriptor.getContainer().getContainerResources())
                                        .withEfsMounts(Collections.singletonList(
                                                new EfsMount("efsId#1", "/data", null, null)
                                        ))
                                        .build())
                        .build()
                ).build();
        Job<BatchJobExt> job = JobGenerator.batchJobs(incompleteEfsDefinition).getValue();

        // EFS violation expected
        assertThat(entitySanitizer.validate(job)).hasSize(1);

        // Now do cleanup
        Job<BatchJobExt> sanitized = entitySanitizer.sanitize(job).get();
        assertThat(entitySanitizer.validate(sanitized)).isEmpty();
    }
}
