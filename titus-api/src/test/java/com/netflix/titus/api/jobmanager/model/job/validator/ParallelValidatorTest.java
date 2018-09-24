package com.netflix.titus.api.jobmanager.model.job.validator;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * This class tests the {@link ParallelValidator} class.
 */
public class ParallelValidatorTest {
    private static final JobDescriptor MOCK_JOB = mock(JobDescriptor.class);
    private static final Duration TIMEOUT = Duration.ofMillis(10);

    // Hard validation tests

    @Test
    public void validateHardPassPass() {
        EntityValidator pass0 = new PassJobValidator();
        EntityValidator pass1 = new PassJobValidator();
        ParallelValidator validator = new ParallelValidator(TIMEOUT, Arrays.asList(pass0, pass1), Collections.emptySet());
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNext(Collections.emptySet())
                .verifyComplete();
    }

    @Test
    public void validateHardFailFail() {
        EntityValidator fail0 = new FailJobValidator();
        EntityValidator fail1 = new FailJobValidator();
        ParallelValidator validator = new ParallelValidator(TIMEOUT, Arrays.asList(fail0, fail1), Collections.emptySet());
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
        EntityValidator pass = new PassJobValidator();
        EntityValidator fail = new FailJobValidator();
        ParallelValidator validator = new ParallelValidator(TIMEOUT, Arrays.asList(pass, fail), Collections.emptySet());
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
        EntityValidator pass = new PassJobValidator();
        EntityValidator never = new NeverJobValidator();
        EntityValidator validator = new ParallelValidator(TIMEOUT, Arrays.asList(pass, never), Collections.emptySet());
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
        EntityValidator pass = new PassJobValidator();
        EntityValidator fail = new FailJobValidator();
        EntityValidator never = new NeverJobValidator();
        EntityValidator parallelValidator = new ParallelValidator(TIMEOUT, Arrays.asList(pass, fail, never), Collections.emptySet());
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
        EntityValidator never = new NeverJobValidator();
        EntityValidator validator = new ParallelValidator(TIMEOUT, Collections.emptySet(), Arrays.asList(never));
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
        EntityValidator fail = new FailJobValidator();
        EntityValidator validator = new ParallelValidator(TIMEOUT, Collections.emptySet(), Arrays.asList(fail));
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
        EntityValidator pass = new PassJobValidator();
        EntityValidator validator = new ParallelValidator(TIMEOUT, Collections.emptySet(), Arrays.asList(pass));
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 0)
                .verifyComplete();
    }

    // Hard/Soft validation tests

    @Test
    public void validateHardSoftTimeout() {
        EntityValidator never0 = new NeverJobValidator();
        EntityValidator never1 = new NeverJobValidator();
        EntityValidator validator = new ParallelValidator(TIMEOUT, Arrays.asList(never0), Arrays.asList(never1));
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
        EntityValidator pass0 = new PassJobValidator();
        EntityValidator pass1 = new PassJobValidator();
        EntityValidator validator = new ParallelValidator(TIMEOUT, Arrays.asList(pass0), Arrays.asList(pass1));
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 0)
                .verifyComplete();
    }

    @Test
    public void validateHardSoftFail() {
        EntityValidator fail0 = new FailJobValidator();
        EntityValidator fail1 = new FailJobValidator();
        EntityValidator validator = new ParallelValidator(TIMEOUT, Arrays.asList(fail0), Arrays.asList(fail1));
        Mono<Set<ValidationError>> mono = validator.validate(MOCK_JOB);

        StepVerifier.create(mono)
                .expectNextMatches(errors -> errors.size() == 2)
                .verifyComplete();

        Collection<ValidationError> errors = mono.block();
        validateFailErrors(errors);
        assertThat(errors).allMatch(error -> error.getType().equals(ValidationError.Type.HARD));
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
                .allMatch(error -> error.getDescription().equals(ParallelValidator.getTimeoutMsg(TIMEOUT)));
    }

    private void validateErrorType(Collection<ValidationError> hardErrors, ValidationError.Type errorType) {
        assertThat(hardErrors).allMatch(error -> error.getType().equals(errorType));
    }
}
