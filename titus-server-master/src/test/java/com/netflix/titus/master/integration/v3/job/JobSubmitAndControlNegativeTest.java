/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.job;

import java.util.Iterator;
import java.util.regex.Pattern;

import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.grpc.protogen.BatchJobSpec;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Constraints;
import com.netflix.titus.grpc.protogen.ContainerResources;
import com.netflix.titus.grpc.protogen.Image;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceBlockingStub;
import com.netflix.titus.grpc.protogen.Owner;
import com.netflix.titus.grpc.protogen.RetryPolicy;
import com.netflix.titus.grpc.protogen.SecurityProfile;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.grpc.StatusRuntimeException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.netflix.titus.master.integration.v3.job.JobTestUtils.submitBadJob;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicKubeCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.serviceJobDescriptors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 *
 */
@Category(IntegrationTest.class)
public class JobSubmitAndControlNegativeTest extends BaseIntegrationTest {

    private final JobDescriptor.Builder BATCH_JOB_DESCR_BUILDER = toGrpcJobDescriptor(batchJobDescriptors().getValue()).toBuilder();

    private final BatchJobSpec.Builder BATCH_JOB_SPEC_BUILDER = BATCH_JOB_DESCR_BUILDER.getBatch().toBuilder();

    private final JobDescriptor.Builder SERVICE_JOB_DESCR_BUILDER = toGrpcJobDescriptor(serviceJobDescriptors().getValue()).toBuilder();

    private final ServiceJobSpec.Builder SERVICE_JOB_SPEC_BUILDER = SERVICE_JOB_DESCR_BUILDER.getService().toBuilder();

    @ClassRule
    public static final TitusStackResource titusStackResource = new TitusStackResource(basicKubeCell(2));

    private static JobManagementServiceBlockingStub client;

    @BeforeClass
    public static void setUp() throws Exception {
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithNoOwner() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setOwner(Owner.getDefaultInstance()).build(),
                "owner.teamEmail"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithNoApplicationName() {
        submitBadJob(client, BATCH_JOB_DESCR_BUILDER.setApplicationName("").build(), "applicationName");
        submitBadJob(client, BATCH_JOB_DESCR_BUILDER.setApplicationName("   ").build(), "applicationName");
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithInvalidComputeResources() {
        ContainerResources badContainer = ContainerResources.newBuilder()
                .setGpu(-1)
                .build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setResources(badContainer)).build(),
                "container.containerResources.gpu"
        );
    }

    /**
     * TODO GPU is not limited today. We should add GPU to {@link ResourceDimension} model.
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithExcessiveComputeResources() {
        ContainerResources badContainer = ContainerResources.newBuilder()
                .setCpu(100)
                .setGpu(100)
                .setMemoryMB(1000_000_000)
                .setDiskMB(1000_000_000)
                .setNetworkMbps(10_000_000)
                .build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setResources(badContainer)).build(),
                "container.containerResources.cpu",
                "container.containerResources.gpu",
                "container.containerResources.memoryMB",
                "container.containerResources.networkMbps",
                "container.containerResources.diskMB"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithInvalidEfsMounts() {
        ContainerResources badEfs = ContainerResources.newBuilder()
                .addEfsMounts(ContainerResources.EfsMount.getDefaultInstance())
                .build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setResources(badEfs)).build(),
                "container.containerResources.efsMounts[0].efsId",
                "container.containerResources.efsMounts[0].mountPoint"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithBadSecurityProfile() {
        SecurityProfile securityProfile = SecurityProfile.newBuilder()
                .addSecurityGroups("not-good-security-group")
                .setIamRole("A   B")
                .build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setSecurityProfile(securityProfile)).build(),
                "container.securityProfile.securityGroups", "container.securityProfile.iamRole"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithoutImage() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setImage(Image.getDefaultInstance())).build(),
                "container.image.name",
                "container.image.noValidImageDigestOrTag"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithInvalidNameAndTag() {
        Image badImage = Image.newBuilder().setName("????????").setTag("############").build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setImage(badImage)).build(),
                "container.image.name",
                "container.image.noValidImageDigestOrTag"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testInvalidSoftAndHardConstraints() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder()
                        .setSoftConstraints(Constraints.newBuilder().putConstraints("badSoftConstraint", "").build())
                        .setHardConstraints(Constraints.newBuilder().putConstraints("badHardConstraint", "").build())
                ).build(),
                "container.hardConstraints",
                "container.softConstraints"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testOverlappingSoftAndHardConstraints() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder()
                        .setSoftConstraints(Constraints.newBuilder().putConstraints("UniqueHost", "true").build())
                        .setHardConstraints(Constraints.newBuilder().putConstraints("UniqueHost", "true").build())
                ).build(),
                "container"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testBatchJobWithInvalidSize() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setSize(-5)).build(),
                "extensions.size"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testTooLargeBatchJob() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setSize(5000)).build(),
                "extensions.size"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testBatchJobWithTooLowRuntimeLimit() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRuntimeLimitSec(4)).build(),
                "extensions.runtimeLimitMs"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testTooLargeBatchJobRuntimeLimit() {
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRuntimeLimitSec(2 * JobConfiguration.MAX_RUNTIME_LIMIT_SEC)).build(),
                "extensions.runtimeLimitMs"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testServiceJobInvalidCapacity() {
        Capacity badCapacity = Capacity.newBuilder().setMin(-2).setDesired(-3).setMax(-4).build();
        submitBadJob(
                client,
                SERVICE_JOB_DESCR_BUILDER.setService(SERVICE_JOB_SPEC_BUILDER.setCapacity(badCapacity).build()).build(),
                "extensions.capacity",
                "extensions.capacity.desired",
                "extensions.capacity.max",
                "extensions.capacity.min"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testTooLargeServiceJob() {
        Capacity badCapacity = Capacity.newBuilder().setMin(1).setDesired(100).setMax(10_001).build();
        submitBadJob(
                client,
                SERVICE_JOB_DESCR_BUILDER.setService(SERVICE_JOB_SPEC_BUILDER.setCapacity(badCapacity)).build(),
                "extensions.capacity"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithInvalidImmediateRetryPolicy() {
        RetryPolicy badRetryPolicy = RetryPolicy.newBuilder().setImmediate(
                RetryPolicy.Immediate.newBuilder().setRetries(-1)
        ).build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRetryPolicy(badRetryPolicy)).build(),
                "extensions.retryPolicy.retries"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithInvalidDelayedRetryPolicy() {
        RetryPolicy badRetryPolicy = RetryPolicy.newBuilder().setDelayed(
                RetryPolicy.Delayed.newBuilder().setRetries(-1).setDelayMs(-1)
        ).build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRetryPolicy(badRetryPolicy)).build(),
                "extensions.retryPolicy.retries",
                "extensions.retryPolicy.delayMs"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testJobWithInvalidExpBackoffRetryPolicy() {
        RetryPolicy badRetryPolicy = RetryPolicy.newBuilder().setExponentialBackOff(
                RetryPolicy.ExponentialBackOff.newBuilder().setRetries(-1).setInitialDelayMs(-1).setMaxDelayIntervalMs(-1)
        ).build();
        submitBadJob(
                client,
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRetryPolicy(badRetryPolicy)).build(),
                "extensions.retryPolicy.retries",
                "extensions.retryPolicy.initialDelayMs",
                "extensions.retryPolicy.maxDelayMs"
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testSubmitJobsWithIdenticalJobGroupIdentity() {
        JobDescriptor jobDescriptor = toGrpcJobDescriptor(JobDescriptorGenerator.oneTaskServiceJobDescriptor()
                .but(jd -> jd.toBuilder().withApplicationName("v3App").build())
        );
        try {
            String jobId = client.createJob(jobDescriptor).getId();
            Iterator<JobChangeNotification> it = client.observeJob(JobId.newBuilder().setId(jobId).build());
            // Make sure notifications are sent. For that we need to consume snapshot (job + marker), and actual event from reconciler.
            Evaluators.times(3, it::next);

            client.createJob(jobDescriptor).getId();
            fail("Expected test to fail");
        } catch (StatusRuntimeException e) {
            assertThat(e.getMessage()).containsPattern(Pattern.compile("job with group sequence.*exists"));
        }
    }
}
