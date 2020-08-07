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

package com.netflix.titus.gateway.service.v3.internal;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.runtime.connector.registry.RegistryClient;
import com.netflix.titus.runtime.connector.registry.TitusRegistryException;
import com.netflix.titus.runtime.endpoint.admission.ServiceMeshImageSanitizer;
import com.netflix.titus.runtime.endpoint.admission.ServiceMeshImageSanitizerConfiguration;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceMeshImageSanitizerTest {

    private static final String repo = "proxydRepo/proxydImage";
    private static final String tag = "proxydTag";
    private static final String digest = "sha256:f9f5bb506406b80454a4255b33ed2e4383b9e4a32fb94d6f7e51922704e818fa";

    private final ServiceMeshImageSanitizerConfiguration configuration = mock(ServiceMeshImageSanitizerConfiguration.class);
    private final RegistryClient registryClient = mock(RegistryClient.class);
    private ServiceMeshImageSanitizer sanitizer;

    private static final String imageNameDigest = String.format("%s@%s", repo, digest);
    private static final Map<String, String> digestAttrs = CollectionsExt.asMap(
            JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_ENABLED, "true",
            JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_CONTAINER, imageNameDigest);

    private static final JobDescriptor<?> jobDescriptorWithDigest = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.toBuilder()
                    .withAttributes(CollectionsExt.copyAndAdd(d.getAttributes(), digestAttrs))
                    .build()))
            .getValue();

    private static final String imageNameTag = String.format("%s:%s", repo, tag);
    private static final Map<String, String> tagAttrs = CollectionsExt.asMap(
            JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_ENABLED, "true",
            JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_CONTAINER, imageNameTag);

    private static final JobDescriptor<?> jobDescriptorWithTag = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.toBuilder()
                    .withAttributes(CollectionsExt.copyAndAdd(d.getAttributes(), tagAttrs))
                    .build()))
            .getValue();

    @Before
    public void setUp() {
        when(configuration.isEnabled()).thenReturn(true);
        when(configuration.getServiceMeshImageValidationTimeoutMs()).thenReturn(1000L);
        when(configuration.getErrorType()).thenReturn(ValidationError.Type.HARD.name());
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Mono.just(digest));
        sanitizer = new ServiceMeshImageSanitizer(configuration, registryClient, new DefaultRegistry());
    }

    @Test
    public void testJobWithTagResolution() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Mono.just(digest));

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptorWithTag))
                .assertNext(jobDescriptor -> assertThat(jobDescriptor
                        .getAttributes()
                        .get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_CONTAINER))
                        .isEqualTo(imageNameDigest))
                .verifyComplete();
    }

    @Test
    public void testJobWithNonExistentTag() {
        when(registryClient.getImageDigest(anyString(), anyString()))
                .thenReturn(Mono.error(TitusRegistryException.imageNotFound(repo, tag)));

        StepVerifier.create(sanitizer.sanitize(jobDescriptorWithTag))
                .expectErrorSatisfies(throwable -> {
                    assertThat(throwable).isInstanceOf(TitusRegistryException.class);
                    assertThat(((TitusRegistryException) throwable).getErrorCode()).isEqualByComparingTo(TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND);
                })
                .verify();
    }

    /**
     * This test verifies that non-NOT_FOUND errors are suppressed and the original job descriptor is returned.
     */
    @Test
    public void testSuppressedInternalError() {
        when(registryClient.getImageDigest(anyString(), anyString()))
                .thenReturn(Mono.error(TitusRegistryException.internalError(repo, tag, HttpStatus.INTERNAL_SERVER_ERROR)));

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptorWithTag))
                .assertNext(jd -> {
                    assertThat(((JobDescriptor<?>) jd).getAttributes())
                            .containsEntry(JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_SERVICEMESH_IMAGE, "true");
                })
                .verifyComplete();
    }

    @Test
    public void testJobWithDigestExists() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Mono.just(digest));

        StepVerifier.create(sanitizer.sanitize(jobDescriptorWithDigest))
                .expectNextCount(0) // nothing to do when digest is valid
                .verifyComplete();
    }
}
