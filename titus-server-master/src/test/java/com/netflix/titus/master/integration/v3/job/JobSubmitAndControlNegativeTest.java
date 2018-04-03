/*
 * Copyright 2018 Netflix, Inc.
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.rpc.BadRequest;
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
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.testkit.grpc.GrpcClientErrorUtils;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.grpc.StatusRuntimeException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks.basicStack;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.serviceJobDescriptors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 */
@Category(IntegrationTest.class)
public class JobSubmitAndControlNegativeTest extends BaseIntegrationTest {

    private final JobDescriptor.Builder BATCH_JOB_DESCR_BUILDER = toGrpcJobDescriptor(batchJobDescriptors().getValue()).toBuilder();

    private final BatchJobSpec.Builder BATCH_JOB_SPEC_BUILDER = BATCH_JOB_DESCR_BUILDER.getBatch().toBuilder();

    private final JobDescriptor.Builder SERVICE_JOB_DESCR_BUILDER = toGrpcJobDescriptor(serviceJobDescriptors().getValue()).toBuilder();

    private final ServiceJobSpec.Builder SERVICE_JOB_SPEC_BUILDER = SERVICE_JOB_DESCR_BUILDER.getService().toBuilder();

    private static final TitusStackResource titusStackResource = new TitusStackResource(basicStack(2));

    private static final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder);

    private static JobManagementServiceBlockingStub client;

    @BeforeClass
    public static void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicSetupActivation());
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();
    }

    @Test
    public void testJobWithNoOwner() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setOwner(Owner.getDefaultInstance()).build(),
                "owner.teamEmail"
        );
    }

    @Test
    public void testJobWithNoApplicationName() throws Exception {
        submitBadJob(BATCH_JOB_DESCR_BUILDER.setApplicationName("").build(), "applicationName");
        submitBadJob(BATCH_JOB_DESCR_BUILDER.setApplicationName("   ").build(), "applicationName");
    }

    @Test
    public void testJobWithInvalidComputeResources() throws Exception {
        ContainerResources badContainer = ContainerResources.newBuilder()
                .setGpu(-1)
                .build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setResources(badContainer)).build(),
                "container.containerResources.gpu"
        );
    }

    /**
     * TODO GPU is not limited today. We should add GPU to {@link ResourceDimension} model.
     */
    @Test
    public void testJobWithExcessiveComputeResources() throws Exception {
        ContainerResources badContainer = ContainerResources.newBuilder()
                .setCpu(100)
                .setGpu(100)
                .setMemoryMB(1000_000_000)
                .setDiskMB(1000_000_000)
                .setNetworkMbps(10_000_000)
                .build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setResources(badContainer)).build(),
                "container.containerResources.cpu",
                "container.containerResources.gpu",
                "container.containerResources.memoryMB",
                "container.containerResources.networkMbps",
                "container.containerResources.diskMB"
        );
    }

    @Test
    public void testJobWithInvalidEfsMounts() throws Exception {
        ContainerResources badEfs = ContainerResources.newBuilder()
                .addEfsMounts(ContainerResources.EfsMount.getDefaultInstance())
                .build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setResources(badEfs)).build(),
                "container.containerResources.efsMounts[0].efsId",
                "container.containerResources.efsMounts[0].mountPoint"
        );
    }

    @Test
    public void testJobWithBadSecurityProfile() throws Exception {
        SecurityProfile securityProfile = SecurityProfile.newBuilder()
                .addSecurityGroups("not-good-security-group")
                .setIamRole("   ")
                .build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setSecurityProfile(securityProfile)).build(),
                "container.securityProfile.securityGroups", "container.securityProfile.iamRole"
        );
    }

    @Test
    public void testJobWithoutImage() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setImage(Image.getDefaultInstance())).build(),
                "container.image.name",
                "container.image"
        );
    }

    @Test
    @Ignore("Until we support digests")
    public void testJobWithBothTagAndDigest() throws Exception {
    }

    @Test
    public void testJobWithInvalidNameAndTag() throws Exception {
        Image badImage = Image.newBuilder().setName("????????").setTag("############").build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder().setImage(badImage)).build(),
                "container.image.name",
                "container.image"
        );
    }

    @Test
    public void testInvalidSoftAndHardConstraints() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder()
                        .setSoftConstraints(Constraints.newBuilder().putConstraints("badSoftConstraint", "").build())
                        .setHardConstraints(Constraints.newBuilder().putConstraints("badHardConstraint", "").build())
                ).build(),
                "container.hardConstraints",
                "container.softConstraints"
        );
    }

    @Test
    public void testOverlappingSoftAndHardConstraints() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setContainer(BATCH_JOB_DESCR_BUILDER.getContainer().toBuilder()
                        .setSoftConstraints(Constraints.newBuilder().putConstraints("UniqueHost", "true").build())
                        .setHardConstraints(Constraints.newBuilder().putConstraints("UniqueHost", "true").build())
                ).build(),
                "container"
        );
    }

    @Test
    public void testBatchJobWithInvalidSize() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setSize(-5)).build(),
                "extensions.size"
        );
    }

    @Test
    public void testTooLargeBatchJob() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setSize(5000)).build(),
                "extensions.size"
        );
    }

    @Test
    public void testBatchJobWithTooLowRuntimeLimit() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRuntimeLimitSec(5)).build(),
                "extensions.runtimeLimitMs"
        );
    }

    @Test
    public void testTooLargeBatchJobRuntimeLimit() throws Exception {
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRuntimeLimitSec(2 * JobConfiguration.MAX_RUNTIME_LIMIT_SEC)).build(),
                "extensions.runtimeLimitMs"
        );
    }

    @Test
    public void testServiceJobInvalidCapacity() throws Exception {
        Capacity badCapacity = Capacity.newBuilder().setMin(-2).setDesired(-3).setMax(-4).build();
        submitBadJob(
                SERVICE_JOB_DESCR_BUILDER.setService(SERVICE_JOB_SPEC_BUILDER.setCapacity(badCapacity).build()).build(),
                "extensions.capacity",
                "extensions.capacity.desired",
                "extensions.capacity.max",
                "extensions.capacity.min"
        );
    }

    @Test
    public void testTooLargeServiceJob() throws Exception {
        Capacity badCapacity = Capacity.newBuilder().setMin(1).setDesired(100).setMax(5000).build();
        submitBadJob(
                SERVICE_JOB_DESCR_BUILDER.setService(SERVICE_JOB_SPEC_BUILDER.setCapacity(badCapacity)).build(),
                "extensions.capacity"
        );
    }

    @Test
    public void testJobWithInvalidImmediateRetryPolicy() throws Exception {
        RetryPolicy badRetryPolicy = RetryPolicy.newBuilder().setImmediate(
                RetryPolicy.Immediate.newBuilder().setRetries(-1)
        ).build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRetryPolicy(badRetryPolicy)).build(),
                "extensions.retryPolicy.retries"
        );
    }

    @Test
    public void testJobWithInvalidDelayedRetryPolicy() throws Exception {
        RetryPolicy badRetryPolicy = RetryPolicy.newBuilder().setDelayed(
                RetryPolicy.Delayed.newBuilder().setRetries(-1).setDelayMs(-1)
        ).build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRetryPolicy(badRetryPolicy)).build(),
                "extensions.retryPolicy.retries",
                "extensions.retryPolicy.delayMs"
        );
    }

    @Test
    public void testJobWithInvalidExpBackoffRetryPolicy() throws Exception {
        RetryPolicy badRetryPolicy = RetryPolicy.newBuilder().setExponentialBackOff(
                RetryPolicy.ExponentialBackOff.newBuilder().setRetries(-1).setInitialDelayMs(-1).setMaxDelayIntervalMs(-1)
        ).build();
        submitBadJob(
                BATCH_JOB_DESCR_BUILDER.setBatch(BATCH_JOB_SPEC_BUILDER.setRetryPolicy(badRetryPolicy)).build(),
                "extensions.retryPolicy.retries",
                "extensions.retryPolicy.initialDelayMs",
                "extensions.retryPolicy.maxDelayMs"
        );
    }

    @Test
    public void testSubmitJobsWithIdenticalJobGroupIdentityOnV2Engine() throws Exception {
        JobDescriptor jobDescriptor = toGrpcJobDescriptor(JobDescriptorGenerator.oneTaskServiceJobDescriptor());
        try {
            client.createJob(jobDescriptor).getId();
            client.createJob(jobDescriptor).getId();
            fail("Expected test to fail");
        } catch (StatusRuntimeException e) {
            assertThat(e.getMessage()).containsPattern(Pattern.compile("job with group sequence.*exists"));
        }
    }

    @Test
    public void testSubmitJobsWithIdenticalJobGroupIdentityOnV3Engine() throws Exception {
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

    private void submitBadJob(JobDescriptor badJobDescriptor, String... expectedFields) {
        Set<String> expectedFieldSet = new HashSet<>();
        Collections.addAll(expectedFieldSet, expectedFields);

        try {
            client.createJob(badJobDescriptor).getId();
            fail("Expected test to fail");
        } catch (StatusRuntimeException e) {
            System.out.println("Received StatusRuntimeException: " + e.getMessage());

            Optional<BadRequest> badRequestOpt = GrpcClientErrorUtils.getDetail(e, BadRequest.class);

            // Print validation messages for visual inspection
            badRequestOpt.ifPresent(System.out::println);

            Set<String> badFields = badRequestOpt.map(badRequest ->
                    badRequest.getFieldViolationsList().stream().map(BadRequest.FieldViolation::getField).collect(Collectors.toSet())
            ).orElse(Collections.emptySet());

            assertThat(badFields).containsAll(expectedFieldSet);
            assertThat(badFields.size()).isEqualTo(expectedFieldSet.size());
        }
    }
}
