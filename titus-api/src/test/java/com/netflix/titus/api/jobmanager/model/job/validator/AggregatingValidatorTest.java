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

package com.netflix.titus.api.jobmanager.model.job.validator;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.EntityValidatorConfiguration;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.runtime.endpoint.validator.AggregatingValidator;
import com.netflix.titus.runtime.endpoint.validator.TitusValidatorConfiguration;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class tests the {@link AggregatingValidator} class.
 */
public class AggregatingValidatorTest {
    private static final JobDescriptor MOCK_JOB = mock(JobDescriptor.class);
    private final TitusValidatorConfiguration configuration = mock(TitusValidatorConfiguration.class);
    private final Registry registry = new DefaultRegistry();

    @Before
    public void setUp() {
        when(configuration.getTimeoutMs()).thenReturn(500);
    }

    // Hard validation tests

    @Test
    public void validateHardPassPass() {
        EntityValidator pass0 = new PassJobValidator(ValidationError.Type.HARD);
        EntityValidator pass1 = new PassJobValidator(ValidationError.Type.HARD);
        AggregatingValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(pass0, pass1),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNext(Collections.emptySet())
                .verifyComplete();
    }

    @Test
    public void validateHardFailFail() {
        EntityValidator fail0 = new FailJobValidator(ValidationError.Type.HARD);
        EntityValidator fail1 = new FailJobValidator(ValidationError.Type.HARD);
        AggregatingValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(fail0, fail1),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 2)
                .verifyComplete();

        Set<ValidationError> errors = mono.block();
        validateFailErrors(errors);
        validateErrorType(errors, ValidationError.Type.HARD);
    }

    @Test
    public void validateHardPassFail() {
        EntityValidator pass = new PassJobValidator(ValidationError.Type.HARD);
        EntityValidator fail = new FailJobValidator(ValidationError.Type.HARD);
        AggregatingValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(pass, fail),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 1)
                .verifyComplete();

        Set<ValidationError> errors = mono.block();
        validateFailErrors(errors);
        validateErrorType(errors, ValidationError.Type.HARD);
    }

    @Test
    public void validateHardPassTimeout() {
        EntityValidator pass = new PassJobValidator(ValidationError.Type.HARD);
        EntityValidator never = new NeverJobValidator(ValidationError.Type.HARD);
        EntityValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(pass, never),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 1)
                .verifyComplete();

        Set<ValidationError> errors = mono.block();
        validateTimeoutErrors(errors);
        validateErrorType(errors, ValidationError.Type.HARD);
    }

    @Test
    public void validateHardPassFailTimeout() {
        EntityValidator pass = new PassJobValidator(ValidationError.Type.HARD);
        EntityValidator fail = new FailJobValidator(ValidationError.Type.HARD);
        EntityValidator never = new NeverJobValidator(ValidationError.Type.HARD);
        EntityValidator parallelValidator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(pass, fail, never),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = parallelValidator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 2)
                .verifyComplete();

        Set<ValidationError> errors = mono.block();
        validateErrorType(errors, ValidationError.Type.HARD);

        Collection<ValidationError> failErrors = errors.stream()
                .filter(error -> error.getField().equals(FailJobValidator.ERR_FIELD))
                .collect(Collectors.toList());
        assertThat(failErrors).hasSize(1);
        validateFailErrors(failErrors);

        Collection<ValidationError> timeoutErrors = errors.stream()
                .filter(error -> error.getField().equals(NeverJobValidator.class.getSimpleName()))
                .collect(Collectors.toList());
        assertThat(timeoutErrors).hasSize(1);
        validateTimeoutErrors(timeoutErrors);
    }

    // Soft validation tests

    @Test
    public void validateSoftTimeout() {
        EntityValidator never = new NeverJobValidator(ValidationError.Type.SOFT);
        EntityValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(never),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 1)
                .verifyComplete();

        Collection<ValidationError> errors = mono.block();
        validateTimeoutErrors(errors);
        validateErrorType(errors, ValidationError.Type.SOFT);
    }

    @Test
    public void validateSoftFailure() {
        EntityValidator fail = new FailJobValidator(ValidationError.Type.SOFT);
        EntityValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(fail),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 1)
                .verifyComplete();

        Collection<ValidationError> errors = mono.block();
        validateFailErrors(errors);
        validateErrorType(errors, ValidationError.Type.HARD);
    }

    @Test
    public void validateSoftPass() {
        EntityValidator pass = new PassJobValidator(ValidationError.Type.SOFT);
        EntityValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(pass),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 0)
                .verifyComplete();
    }

    // Hard/Soft validation tests

    @Test
    public void validateHardSoftTimeout() {
        EntityValidator<JobDescriptor> never0 = new NeverJobValidator(ValidationError.Type.HARD);
        EntityValidator never1 = new NeverJobValidator(ValidationError.Type.SOFT);
        EntityValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(never0, never1),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 2)
                .verifyComplete();
        Collection<ValidationError> errors = mono.block();
        validateTimeoutErrors(errors);

        Collection<ValidationError> hardErrors = errors.stream()
                .filter(error -> error.getType().equals(ValidationError.Type.HARD))
                .collect(Collectors.toList());
        assertThat(hardErrors).hasSize(1);

        Collection<ValidationError> softErrors = errors.stream()
                .filter(error -> error.getType().equals(ValidationError.Type.SOFT))
                .collect(Collectors.toList());
        assertThat(softErrors).hasSize(1);
    }

    @Test
    public void validateHardSoftPass() {
        EntityValidator pass0 = new PassJobValidator(ValidationError.Type.HARD);
        EntityValidator pass1 = new PassJobValidator(ValidationError.Type.SOFT);
        EntityValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(pass0, pass1),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 0)
                .verifyComplete();
    }

    @Test
    public void validateHardSoftFail() {
        EntityValidator fail0 = new FailJobValidator(ValidationError.Type.HARD);
        EntityValidator fail1 = new FailJobValidator(ValidationError.Type.SOFT);
        EntityValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(fail0, fail1),
                Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 2)
                .verifyComplete();

        Collection<ValidationError> errors = mono.block();
        validateFailErrors(errors);
        assertThat(errors).allMatch(error -> error.getType().equals(ValidationError.Type.HARD));
    }

    @Test
    public void validateIsProtectedAgainstEmpty() {
        EntityValidator<JobDescriptor> pass = new PassJobValidator(ValidationError.Type.HARD);
        EntityValidator<JobDescriptor> empty = new EmptyValidator();
        AggregatingValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Arrays.asList(empty, pass, empty),
                Collections.emptySet()
        );
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);
        StepVerifier.create(mono)
                .expectNext(Collections.emptySet())
                .verifyComplete();
    }

    // Sanitizer tests

    @Test
    public void sanitizePassPass() {
        final String initialAppName = "initialAppName";
        final String initialCapacityGroup = "initialCapacityGroup";
        EntityValidator appNameSanitizer = new TestingAppNameSanitizer();
        EntityValidator capacityGroupSanitizer = new TestingCapacityGroupSanitizer();

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.toBuilder()
                        .withApplicationName(initialAppName)
                        .withCapacityGroup(initialCapacityGroup)
                        .build())
                .getValue();

        AggregatingValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Collections.emptySet(),
                Arrays.asList(appNameSanitizer, capacityGroupSanitizer));

        StepVerifier.create(validator.sanitize(jobDescriptor))
                .assertNext(sanitizedJobDescriptor -> {
                    assertThat(sanitizedJobDescriptor.getApplicationName().equals(TestingAppNameSanitizer.desiredAppName)).isTrue();
                    assertThat(sanitizedJobDescriptor.getCapacityGroup().equals(TestingCapacityGroupSanitizer.desiredCapacityGroup)).isTrue();
                })
                .verifyComplete();
    }

    @Test
    public void sanitizePassFail() {
        final String initialAppName = "initialAppName";
        final String initialCapacityGroup = "initialCapacityGroup";
        EntityValidator appNameSanitizer = new TestingAppNameSanitizer();
        EntityValidator failSanitizer = new FailJobValidator();

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.toBuilder()
                        .withApplicationName(initialAppName)
                        .withCapacityGroup(initialCapacityGroup)
                        .build())
                .getValue();

        AggregatingValidator validator = new AggregatingValidator(
                configuration,
                registry,
                Collections.emptySet(),
                Arrays.asList(appNameSanitizer, failSanitizer));

        StepVerifier.create(validator.sanitize(jobDescriptor))
                .expectError(TitusServiceException.class)
                .verify();
    }

    private void validateFailErrors(Collection<ValidationError> failErrors) {
        assertThat(failErrors.size() > 0).isTrue();
        assertThat(failErrors)
                .allMatch(error -> error.getField().equals(FailJobValidator.ERR_FIELD))
                .allMatch(error -> error.getDescription().contains(FailJobValidator.ERR_DESCRIPTION));
    }

    private void validateTimeoutErrors(Collection<ValidationError> timeoutErrors) {
        assertThat(timeoutErrors.size() > 0).isTrue();
        assertThat(timeoutErrors)
                .allMatch(error -> error.getField().equals(NeverJobValidator.class.getSimpleName()))
                .allMatch(error -> error.getDescription().equals(AggregatingValidator.getTimeoutMsg(Duration.ofMillis(configuration.getTimeoutMs()))));
    }

    private void validateErrorType(Collection<ValidationError> hardErrors, ValidationError.Type errorType) {
        assertThat(hardErrors).allMatch(error -> error.getType().equals(errorType));
    }

    private static class EmptyValidator implements EntityValidator<JobDescriptor> {
        @Override
        public Mono<Set<ValidationError>> validate(JobDescriptor entity) {
            return Mono.empty();
        }

        @Override
        public Mono<JobDescriptor> sanitize(JobDescriptor entity) {
            return Mono.empty();
        }

        @Override
        public ValidationError.Type getErrorType(EntityValidatorConfiguration configuration) {
            return ValidationError.Type.HARD;
        }
    }

}
