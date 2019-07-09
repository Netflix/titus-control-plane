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

package com.netflix.titus.runtime.endpoint.admission;

import java.util.Arrays;
import java.util.Collections;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingSanitizerTest {

    private final TitusValidatorConfiguration configuration = mock(TitusValidatorConfiguration.class);

    @Before
    public void setUp() {
        when(configuration.getTimeoutMs()).thenReturn(500);
    }

    @Test
    public void mergeAllSanitizers() {
        String initialAppName = "initialAppName";
        String initialCapacityGroup = "initialCapacityGroup";
        TestingAppNameSanitizer appNameSanitizer = new TestingAppNameSanitizer();
        TestingCapacityGroupSanitizer capacityGroupSanitizer = new TestingCapacityGroupSanitizer();

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.toBuilder()
                        .withApplicationName(initialAppName)
                        .withCapacityGroup(initialCapacityGroup)
                        .build())
                .getValue();

        AggregatingSanitizer sanitizer = new AggregatingSanitizer(configuration, Arrays.asList(
                appNameSanitizer, capacityGroupSanitizer
        ));
        StepVerifier.create(sanitizer.sanitize(jobDescriptor))
                .assertNext(sanitizedJobDescriptor -> {
                    assertThat(sanitizedJobDescriptor.getApplicationName()).isEqualTo(appNameSanitizer.getDesiredAppName());
                    assertThat(sanitizedJobDescriptor.getCapacityGroup()).isEqualTo(capacityGroupSanitizer.getDesiredCapacityGroup());
                })
                .verifyComplete();
    }

    @Test
    public void sanitizeFail() {
        String initialAppName = "initialAppName";

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.toBuilder()
                        .withApplicationName(initialAppName)
                        .build())
                .getValue();

        AggregatingSanitizer sanitizer = new AggregatingSanitizer(configuration, Arrays.asList(
                new TestingAppNameSanitizer(),
                new FailJobValidator()
        ));
        StepVerifier.create(sanitizer.sanitize(jobDescriptor))
                .expectError(TitusServiceException.class)
                .verify();
    }

    @Test
    public void noSanitizersIsANoop() {
        AggregatingSanitizer sanitizer = new AggregatingSanitizer(configuration, Collections.emptyList());
        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors().getValue();
        StepVerifier.create(sanitizer.sanitize(jobDescriptor))
                .assertNext(sanitizedJobDescriptor -> assertThat(sanitizedJobDescriptor).isSameAs(jobDescriptor))
                .verifyComplete();
    }

    @Test
    public void timeoutForAllSanitizers() {
        String initialAppName = "initialAppName";
        TestingAppNameSanitizer appNameSanitizer = new TestingAppNameSanitizer();
        NeverJobValidator neverSanitizer = new NeverJobValidator();

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.toBuilder()
                        .withApplicationName(initialAppName)
                        .build())
                .getValue();

        AggregatingSanitizer sanitizer = new AggregatingSanitizer(configuration,
                Arrays.asList(appNameSanitizer, neverSanitizer));

        StepVerifier.create(sanitizer.sanitize(jobDescriptor))
                .expectErrorMatches(error -> {
                    if (!(error instanceof TitusServiceException)) {
                        return false;
                    }
                    TitusServiceException titusException = (TitusServiceException) error;
                    return titusException.getErrorCode().equals(TitusServiceException.ErrorCode.INTERNAL) &&
                            error.getMessage().contains("Job sanitization timed out");

                })
                .verify();
    }

    @Test
    public void noopSanitizers() {
        String initialAppName = "initialAppName";
        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.toBuilder()
                        .withApplicationName(initialAppName)
                        .build())
                .getValue();

        TestingAppNameSanitizer appNameSanitizer = new TestingAppNameSanitizer();
        AggregatingSanitizer sanitizer = new AggregatingSanitizer(configuration, Arrays.asList(
                new EmptyValidator(),
                appNameSanitizer,
                new EmptyValidator()
        ));
        StepVerifier.create(sanitizer.sanitize(jobDescriptor))
                .assertNext(sanitizedJobDescriptor -> assertThat(sanitizedJobDescriptor.getApplicationName())
                        .isEqualTo(appNameSanitizer.getDesiredAppName()))
                .verifyComplete();
    }

    @Test
    public void allNoopSanitizers() {
        AggregatingSanitizer sanitizer = new AggregatingSanitizer(configuration, Arrays.asList(
                new EmptyValidator(),
                new EmptyValidator(),
                new EmptyValidator()
        ));
        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors().getValue();
        StepVerifier.create(sanitizer.sanitize(jobDescriptor))
                .assertNext(sanitizedJobDescriptor -> assertThat(sanitizedJobDescriptor).isSameAs(jobDescriptor))
                .verifyComplete();
    }
}
