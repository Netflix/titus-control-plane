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
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.runtime.connector.registry.RegistryClient;
import com.netflix.titus.runtime.connector.registry.TitusRegistryException;
import com.netflix.titus.runtime.endpoint.admission.JobImageSanitizer;
import com.netflix.titus.runtime.endpoint.admission.JobImageValidatorConfiguration;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobImageSanitizerTest {

    private static final String repo = "myRepo";
    private static final String tag = "myTag";
    private static final String digest = "sha256:f9f5bb506406b80454a4255b33ed2e4383b9e4a32fb94d6f7e51922704e818fa";
    private static final String errorDescription = "does not exist in registry";

    private final JobImageValidatorConfiguration configuration = mock(JobImageValidatorConfiguration.class);
    private final RegistryClient registryClient = mock(RegistryClient.class);
    private JobImageSanitizer sanitizer;

    private final JobDescriptor<?> jobDescriptorWithDigest = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                    .withImage(Image.newBuilder()
                            .withName(repo)
                            .withDigest(digest)
                            .build())
            ))
            .getValue();

    private final JobDescriptor<?> jobDescriptorWithTag = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                    .withImage(Image.newBuilder()
                            .withName(repo)
                            .withTag(tag)
                            .build())
            ))
            .getValue();

    @Before
    public void setUp() {
        when(configuration.isEnabled()).thenReturn(true);
        when(configuration.getJobImageValidationTimeoutMs()).thenReturn(1000L);
        when(configuration.getErrorType()).thenReturn(ValidationError.Type.HARD.name());
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Mono.just(digest));
        sanitizer = new JobImageSanitizer(configuration, registryClient, new DefaultRegistry());
    }

    @Test
    public void testJobWithTagResolution() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Mono.just(digest));

        StepVerifier.create(sanitizer.sanitize(jobDescriptorWithTag))
                .assertNext(sanitizedImage -> {
                    assertThat(sanitizedImage).isPresent();
                    JobDescriptor jobDescriptor = sanitizer.apply(jobDescriptorWithTag, sanitizedImage);
                    assertThat(jobDescriptor.getContainer().getImage().getDigest().equals(digest)).isTrue();
                })
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

        StepVerifier.create(sanitizer.sanitize(jobDescriptorWithTag))
                .assertNext(sanitizedImage -> {
                    assertThat(sanitizedImage).isNotPresent();

                    // Optional.empty() means sanitization is skipped
                    JobDescriptor jd = sanitizer.apply(jobDescriptorWithTag, sanitizedImage);
                    assertThat(jd.getContainer().getImage().getDigest()).isNullOrEmpty();
                    assertThat(jd.getContainer().getImage()).isEqualTo(jobDescriptorWithTag.getContainer().getImage());
                    assertThat(((JobDescriptor<?>) jd).getAttributes())
                            .containsEntry(JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IMAGE, "true");
                })
                .verifyComplete();
    }

    @Test
    public void testJobWithDigestExists() {
        Image image = jobDescriptorWithDigest.getContainer().getImage();
        when(registryClient.getImageDigest(image.getName(), image.getDigest())).thenReturn(Mono.just(digest));

        StepVerifier.create(sanitizer.sanitize(jobDescriptorWithDigest))
                .expectNextCount(0) // nothing to do when digest is valid
                .verifyComplete();
    }
}
