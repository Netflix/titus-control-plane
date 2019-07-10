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

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.runtime.connector.registry.RegistryClient;
import com.netflix.titus.runtime.connector.registry.TitusRegistryException;
import com.netflix.titus.runtime.endpoint.admission.AdmissionSanitizer;
import com.netflix.titus.runtime.endpoint.admission.JobImageSanitizer;
import com.netflix.titus.runtime.endpoint.admission.JobImageValidatorConfiguration;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;

import static com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(IntegrationTest.class)
public class JobSanitizeTest extends BaseIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(JobSanitizeTest.class);

    private static final String repo = "myRepo";
    private static final String tag = "myTag";
    private static final String digest = "sha256:f9f5bb506406b80454a4255b33ed2e4383b9e4a32fb94d6f7e51922704e818fa";

    private static final String missingImageErrorMsg = "does not exist in registry";

    private final JobImageValidatorConfiguration configuration = mock(JobImageValidatorConfiguration.class);
    private final RegistryClient registryClient = mock(RegistryClient.class);

    private final TitusStackResource titusStackResource = getTitusStackResource(
            new JobImageSanitizer(configuration, registryClient, new DefaultRegistry())
    );
    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);
    private JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(instanceGroupsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        when(configuration.isEnabled()).thenReturn(true);
        when(configuration.getJobImageValidationTimeoutMs()).thenReturn(1000L);
        when(configuration.getErrorType()).thenReturn(ValidationError.Type.HARD.name());
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        this.client = titusStackResource.getGateway().getV3BlockingGrpcClient();
    }

    /**
     * Verifies that a digest value is properly added to a job descriptor that is using tag.
     */
    @Test
    public void testJobDigestResolution() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Mono.just(digest));

        com.netflix.titus.grpc.protogen.JobDescriptor jobDescriptor =
                toGrpcJobDescriptor(batchJobDescriptors()
                        .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                                .withImage(Image.newBuilder()
                                        .withName(repo)
                                        .withTag(tag)
                                        .build())
                        ))
                        .getValue());


        String jobId = client.createJob(jobDescriptor).getId();
        Job resultJobDescriptor = client.findJob(JobId.newBuilder().setId(jobId).build());
        assertEquals(digest, resultJobDescriptor.getJobDescriptor().getContainer().getImage().getDigest());
    }

    /**
     * Verifies that a NOT_FOUND image produces an invalid argument exception.
     */
    @Test
    public void testNonexistentTag() {
        when(registryClient.getImageDigest(anyString(), anyString()))
                .thenReturn(Mono.error(TitusRegistryException.imageNotFound(repo, tag)));

        final com.netflix.titus.grpc.protogen.JobDescriptor jobDescriptor =
                toGrpcJobDescriptor(batchJobDescriptors()
                        .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                                .withImage(Image.newBuilder()
                                        .withName(repo)
                                        .withTag(tag)
                                        .build())
                        ))
                        .getValue());

        try {
            client.createJob(jobDescriptor).getId();
            fail("Expect createJob() to fail");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            assertTrue(e.getMessage().contains(missingImageErrorMsg));
        }
    }

    /**
     * Verifies that non-NOT_FOUND errors are suppressed and the original job descriptor is not modified.
     */
    @Test
    public void testSuppressedInternalError() {
        when(registryClient.getImageDigest(anyString(), anyString()))
                .thenReturn(
                        Mono.error(TitusRegistryException.internalError(repo, tag, HttpStatus.INTERNAL_SERVER_ERROR))
                );

        final com.netflix.titus.grpc.protogen.JobDescriptor jobDescriptor =
                toGrpcJobDescriptor(batchJobDescriptors()
                        .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                                .withImage(Image.newBuilder()
                                        .withName(repo)
                                        .withTag(tag)
                                        .build())
                        ))
                        .getValue());

        String jobId = client.createJob(jobDescriptor).getId();

        Job resultJobDescriptor = client.findJob(JobId.newBuilder().setId(jobId).build());
        logger.info("Got back result {}", resultJobDescriptor);
        assertTrue(resultJobDescriptor.getJobDescriptor().getContainer().getImage().getDigest().isEmpty());
    }

    private TitusStackResource getTitusStackResource(AdmissionSanitizer<JobDescriptor> sanitizer) {
        SimulatedCloud simulatedCloud = SimulatedClouds.basicCloud(2);

        return new TitusStackResource(EmbeddedTitusCell.aTitusCell()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud).toBuilder()
                        .withCellName("cell-name")
                        .build())
                .withDefaultGateway()
                .withJobSanitizer(sanitizer)
                .build());
    }
}
