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
import java.util.Optional;
import java.util.Set;
import javax.validation.ConstraintViolation;

import com.google.common.collect.ImmutableMap;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.DefaultDecoder;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.MapConfig;
import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static java.util.Arrays.asList;
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
    public void setUp() {
        entitySanitizer = new JobSanitizerBuilder()
                .withJobConstrainstConfiguration(constraints)
                .withMaxContainerSizeResolver(capacityGroup -> MAX_CONTAINER_SIZE)
                .build();
    }

    @Test
    public void testValidBatchJob() {
        // In test descriptor, make sure we do not use default network throughput.
        Job<BatchJobExt> job = JobGenerator.batchJobs(
                oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().but(c -> c.getContainerResources().toBuilder().withNetworkMbps(256).build()))
        ).getValue();

        assertThat(entitySanitizer.validate(job)).isEmpty();
        assertThat(entitySanitizer.sanitize(job)).isEmpty();
    }

    @Test
    public void testNetworkAllocationAdjustment() {
        Job<BatchJobExt> job = JobGenerator.batchJobs(
                oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().but(c -> c.getContainerResources().toBuilder().withNetworkMbps(10).build()))
        ).getValue();

        assertThat(entitySanitizer.validate(job)).isEmpty();

        Optional<Job<BatchJobExt>> updated = entitySanitizer.sanitize(job);
        assertThat(updated).isPresent();
        assertThat(updated.get().getJobDescriptor().getContainer().getContainerResources().getNetworkMbps()).isEqualTo(128);
    }

    @Test
    public void testJobWithNullFieldValues() {
        Job<BatchJobExt> job = JobGenerator.batchJobs(
                oneTaskBatchJobDescriptor().but(jd -> jd.toBuilder().withContainer(null).build()
                )
        ).getValue();

        assertThat(entitySanitizer.sanitize(job)).isEmpty();
        assertThat(entitySanitizer.validate(job)).isNotEmpty();
    }

    @Test
    public void testBatchTaskWithNullFieldValues() {
        BatchJobTask task = JobGenerator.batchTasks(JobGenerator.batchJobs(oneTaskBatchJobDescriptor()).getValue()).getValue().toBuilder()
                .withStatus(null)
                .build();

        assertThat(entitySanitizer.sanitize(task)).isEmpty();
        assertThat(entitySanitizer.validate(task)).isNotEmpty();
    }

    @Test
    public void testServiceTaskWithNullFieldValues() {
        ServiceJobTask task = JobGenerator.serviceTasks(JobGenerator.serviceJobs(oneTaskServiceJobDescriptor()).getValue()).getValue().toBuilder()
                .withStatus(null)
                .build();

        assertThat(entitySanitizer.sanitize(task)).isEmpty();
        assertThat(entitySanitizer.validate(task)).isNotEmpty();
    }

    @Test
    public void testBatchJobWithInvalidSecurityGroups() {
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
    public void testBatchWithNoSecurityGroup() {
        JobDescriptor<BatchJobExt> badJobDescriptor = oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().but(c ->
                c.getSecurityProfile().toBuilder().withSecurityGroups(Collections.emptyList()).build()
        ));
        Set<ConstraintViolation<JobDescriptor<BatchJobExt>>> violations = entitySanitizer.validate(badJobDescriptor);
        assertThat(violations).hasSize(1);
        assertThat(CollectionsExt.first(violations).getPropertyPath().toString()).contains("securityGroups");
    }

    @Test
    public void testBatchWithTooManySecurityGroups() {
        JobDescriptor<BatchJobExt> badJobDescriptor = oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().but(c ->
                c.getSecurityProfile().toBuilder().withSecurityGroups(
                        asList("sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6", "sg-7")
                ).build()
        ));
        Set<ConstraintViolation<JobDescriptor<BatchJobExt>>> violations = entitySanitizer.validate(badJobDescriptor);
        assertThat(violations).hasSize(1);
        assertThat(CollectionsExt.first(violations).getPropertyPath().toString()).contains("securityGroups");
    }

    @Test
    public void testBatchWithNoIamRole() {
        JobDescriptor<BatchJobExt> badJobDescriptor = oneTaskBatchJobDescriptor().but(jd -> jd.getContainer().but(c ->
                c.getSecurityProfile().toBuilder().withIamRole("").build()
        ));
        Set<ConstraintViolation<JobDescriptor<BatchJobExt>>> violations = entitySanitizer.validate(badJobDescriptor);
        assertThat(violations).hasSize(1);
        assertThat(CollectionsExt.first(violations).getPropertyPath().toString()).contains("iamRole");
    }

    @Test
    public void testBatchJobWithMissingImageTagAndDigest() {
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
    public void testBatchJobWithIncompleteEfsDefinition() {
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
