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

package com.netflix.titus.master.integration.v3.job.basic;

import java.util.Collections;
import java.util.List;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.common.model.admission.TitusValidatorConfiguration;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.runtime.endpoint.admission.AggregatingValidator;
import com.netflix.titus.runtime.endpoint.admission.FailJobValidator;
import com.netflix.titus.runtime.endpoint.admission.PassJobValidator;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeClusters;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import io.grpc.StatusRuntimeException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test suite proves that {@link AdmissionValidator <JobDescriptor>} failures behave as expected.  Outside of this test suite
 * the default AdmissionValidator is the {@link PassJobValidator}.  All
 * other test suites prove that it does not invalidate jobs inappropriately.
 */
@Category(IntegrationTest.class)
public class JobValidatorNegativeTest extends BaseIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(JobValidatorNegativeTest.class);

    private static final TitusValidatorConfiguration configuration = mock(TitusValidatorConfiguration.class);
    private static final List<AdmissionValidator<JobDescriptor>> validators = Collections.singletonList(new FailJobValidator());

    public static TitusStackResource titusStackResource;

    private JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    @BeforeClass
    public static void setUpClass() {
        // This is an arbitrary large timeout; the FailJobValidator fails instantaneously, so
        // timeout never occurs.
        when(configuration.getTimeoutMs()).thenReturn(10 * 1000);
        AggregatingValidator validator = new AggregatingValidator(configuration, new DefaultRegistry(), validators);
        titusStackResource = getTitusStackResource(validator);
    }

    @AfterClass
    public static void afterClass() {
        if (titusStackResource != null) {
            titusStackResource.after();
        }
    }

    @Before
    public void setUp() throws Exception {
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
            logger.info("Received StatusRuntimeException: {}", e.getMessage());
            assertThat(e.getMessage()).contains(FailJobValidator.ERR_DESCRIPTION);
            assertThat(e.getMessage()).contains(FailJobValidator.ERR_FIELD);
        }
    }

    private static TitusStackResource getTitusStackResource(AdmissionValidator<JobDescriptor> validator) {
        EmbeddedKubeCluster kubeCluster = EmbeddedKubeClusters.basicCluster(0);
        TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusCell.aTitusCell()
                .withMaster(EmbeddedTitusMasters.basicMasterWithKubeIntegration(kubeCluster).toBuilder()
                        .withCellName("cell-name")
                        .build())
                .withDefaultGateway()
                .withJobValidator(validator)
                .build());
        titusStackResource.before();
        return titusStackResource;
    }
}
