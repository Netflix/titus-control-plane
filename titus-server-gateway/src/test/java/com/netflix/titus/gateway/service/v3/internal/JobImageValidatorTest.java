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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.runtime.connector.registry.RegistryClient;
import com.netflix.titus.runtime.connector.registry.TitusRegistryException;
import com.netflix.titus.runtime.endpoint.validator.JobImageValidator;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;
import reactor.test.StepVerifier;
import rx.Single;
import rx.observers.AssertableSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobImageValidatorTest {

    private static final String repo = "myRepo";
    private static final String tag = "myTag";
    private static final String digest = "sha256:f9f5bb506406b80454a4255b33ed2e4383b9e4a32fb94d6f7e51922704e818fa";
    private static final String errorDescription = "Image not found";

    private final RegistryClient registryClient = mock(RegistryClient.class);
    private JobImageValidator validator;

    @Before
    public void setUp() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.just(digest));
        validator = new JobImageValidator(registryClient);
    }

    @Test
    public void testJobWithTagResolution() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.just(digest));

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withImage(Image.newBuilder()
                                .withName(repo)
                                .withTag(tag)
                                .build())
                ))
                .getValue();

        final AssertableSubscriber<JobDescriptor> resultSubscriber = ReactorExt.toSingle(validator.sanitize(jobDescriptor)).test();

        resultSubscriber.awaitValueCount(1, 10, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors();

        final List<JobDescriptor> sanitizedJobDescriptors = resultSubscriber.getOnNextEvents();
        assertThat(sanitizedJobDescriptors.size()).isEqualTo(1);
        JobDescriptor sanitizedJobDescriptor = sanitizedJobDescriptors.get(0);
        assertThat(sanitizedJobDescriptor.getContainer().getImage().getDigest().equals(digest)).isTrue();
    }

    @Test
    public void testJobWithNonExistentTag() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.error(new TitusRegistryException(TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND, errorDescription)));

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withImage(Image.newBuilder()
                                .withName(repo)
                                .withTag(tag)
                                .build())
                ))
                .getValue();

        final AssertableSubscriber<JobDescriptor> resultSubscriber = ReactorExt.toSingle(validator.sanitize(jobDescriptor)).test();

        resultSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        resultSubscriber.assertError(TitusRegistryException.class);

        List<Throwable> onErrorEvents = resultSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents).isNotNull();
        assertThat(onErrorEvents).hasSize(1);
        assertThat(((TitusRegistryException)onErrorEvents.get(0)).getErrorCode()).isEqualByComparingTo(TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND);
    }

    /**
     * This test verifies that non-NOT_FOUND errors are suppressed and the original job descriptor is returned.
     */
    @Test
    public void testSuppressedInternalError() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.error(new TitusRegistryException(TitusRegistryException.ErrorCode.INTERNAL, "Oops")));

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withImage(Image.newBuilder()
                                .withName(repo)
                                .withTag(tag)
                                .build())
                ))
                .getValue();

        StepVerifier.create(validator.sanitize(jobDescriptor))
                .assertNext(jd -> {
                    assertThat(jd.getContainer().getImage().getDigest()).isNullOrEmpty();
                    assertThat(jd.getContainer().getImage().equals(jobDescriptor.getContainer().getImage())).isTrue();
                })
                .verifyComplete();
    }

    @Test
    public void testJobWithDigestExists() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.just(digest));

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withImage(Image.newBuilder()
                                .withName(repo)
                                .withDigest(digest)
                                .build())
                ))
                .getValue();

        final AssertableSubscriber<JobDescriptor> resultSubscriber = ReactorExt.toSingle(validator.sanitize(jobDescriptor)).test();

        resultSubscriber.awaitValueCount(1, 10, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors();

        final List<JobDescriptor> sanitizedJobDescriptors = resultSubscriber.getOnNextEvents();
        assertThat(sanitizedJobDescriptors.size()).isEqualTo(1);
        JobDescriptor sanitizedJobDescriptor = sanitizedJobDescriptors.get(0);
        assertThat(sanitizedJobDescriptor.getContainer().getImage().getDigest().equals(digest)).isTrue();
        assertThat(sanitizedJobDescriptor.getContainer().getImage().getTag()).isNull();
    }

    @Test
    public void testValidateImageWithTag() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.just(digest));

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withImage(Image.newBuilder()
                                .withName(repo)
                                .withTag(tag)
                                .build())
                ))
                .getValue();

        StepVerifier.create(validator.validate(jobDescriptor))
                .assertNext(validationErrors -> assertThat(validationErrors.isEmpty()).isTrue())
                .verifyComplete();
    }

    @Test
    public void testValidateImageWithDigest() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.just(digest));

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withImage(Image.newBuilder()
                                .withName(repo)
                                .withDigest(digest)
                                .build())
                ))
                .getValue();

        StepVerifier.create(validator.validate(jobDescriptor))
                .assertNext(validationErrors -> assertThat(validationErrors.isEmpty()).isTrue())
                .verifyComplete();
    }

    @Test
    public void testValidateMissingImage() {
        when(registryClient.getImageDigest(anyString(), anyString())).thenReturn(Single.error(new TitusRegistryException(TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND, errorDescription)));

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withImage(Image.newBuilder()
                                .withName(repo)
                                .withTag(tag)
                                .build())
                ))
                .getValue();

        StepVerifier.create(validator.validate(jobDescriptor))
                .assertNext(validationErrors -> {
                    assertThat(validationErrors.size()).isEqualTo(1);
                    assertThat(validationErrors)
                            .allMatch(validationError ->
                                    validationError.getField().equals(JobImageValidator.class.getSimpleName()))
                            .allMatch(validationError ->
                                    validationError.getDescription().equals(errorDescription))
                            .allMatch(validationError ->
                                    validationError.getType().equals(ValidationError.Type.SOFT));
                })
                .verifyComplete();
    }
}
