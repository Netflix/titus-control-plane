package com.netflix.titus.master.integration.v3.job;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.validator.FailJobValidator;
import com.netflix.titus.api.jobmanager.model.job.validator.ParallelValidator;
import com.netflix.titus.api.jobmanager.model.job.validator.PassJobValidator;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import io.grpc.StatusRuntimeException;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * This test suite proves that {@link EntityValidator <JobDescriptor>} failures behave as expected.  Outside of this test suite
 * the default EntityValidator is the {@link PassJobValidator}.  All
 * other test suites prove that it does not invalidate jobs inappropriately.
 */
public class JobValidatorNegativeTest extends BaseIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(JobValidatorNegativeTest.class);

    private static final List<EntityValidator<JobDescriptor>> hardValidators = Collections.emptyList();
    private static final List<EntityValidator<JobDescriptor>> softValidators = Arrays.asList(new FailJobValidator());

    private static final TitusStackResource titusStackResource =
            getTitusStackResource(
                    new ParallelValidator(
                            // This is an arbitrary large timeout; the FailJobValidator fails instantaneously, so
                            // timeout never occurs.
                            Duration.ofSeconds(10),
                            hardValidators,
                            softValidators));

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(instanceGroupsScenarioBuilder);

    private JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        this.client = titusStackResource.getGateway().getV3BlockingGrpcClient();
    }

    @Test(timeout = 30_000)
    public void testFailedValidationThrowsException() {
        final com.netflix.titus.grpc.protogen.JobDescriptor jobDescriptor =
                toGrpcJobDescriptor(batchJobDescriptors().getValue());

        try {
            client.createJob(jobDescriptor).getId();
            fail("Expected test to fail");
        } catch (StatusRuntimeException e) {
            logger.info("Received StatusRuntimeException: {}",  e.getMessage());
            assertThat(e.getMessage()).contains(FailJobValidator.ERR_DESCRIPTION);
            assertThat(e.getMessage()).contains(FailJobValidator.ERR_FIELD);
        }
    }

    private static final TitusStackResource getTitusStackResource(EntityValidator<JobDescriptor> validator) {
        SimulatedCloud simulatedCloud = SimulatedClouds.basicCloud(2);

        return new TitusStackResource(EmbeddedTitusCell.aTitusCell()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud).toBuilder()
                        .withCellName("cell-name")
                        .build())
                .withDefaultGateway()
                .withJobValidator(validator)
                .build());
    }
}
