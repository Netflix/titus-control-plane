package com.netflix.titus.api.jobmanager.model.job.validator;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

/**
 * A ParallelValidator executes multiple {@link EntityValidator}s in parallel.
 */
public class ParallelValidator implements EntityValidator<JobDescriptor> {
    private final Duration timeout;
    private final Collection<EntityValidator<JobDescriptor>> hardValidators;
    private final Collection<EntityValidator<JobDescriptor>> softValidators;

    /**
     * A ParallelValidator ensures that all its constituent validators complete within a specified duration.  The
     * distinction between "hard" and "soft" validators is behavior on timeout.  When a "hard" validator fails to
     * complete within the specified duration it produces a {@link ValidationError} with a {@link ValidationError.Type}
     * of HARD.  Conversely "soft" validators which fail to complete within the specified duration have a
     * {@link ValidationError.Type} of SOFT.
     *
     * @param timeout The duration within which each validator should complete.
     * @param hardValidators Those validators which should generate a HARD {@link ValidationError} on timeout.
     * @param softValidators Those validators which should generate a SOFT {@link ValidationError} on timeout.
     */
    public ParallelValidator(
            Duration timeout,
            Collection<EntityValidator<JobDescriptor>> hardValidators,
            Collection<EntityValidator<JobDescriptor>> softValidators) {
        this.timeout = timeout;
        this.hardValidators = hardValidators;
        this.softValidators = softValidators;
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor jobDescriptor) {
        Collection<Mono<Set<ValidationError>>> monos =
                Stream.concat(
                        getMonos(jobDescriptor, timeout, hardValidators, ValidationError.Type.HARD).stream(),
                        getMonos(jobDescriptor, timeout, softValidators, ValidationError.Type.SOFT).stream())
                        .collect(Collectors.toList());

        return Mono.zip(
                monos,
                objects -> Arrays.stream(objects)
                        .map(objectSet -> (Set<ValidationError>) objectSet)
                        .flatMap(errorSet -> errorSet.stream())
                        .collect(Collectors.toSet()))
                .defaultIfEmpty(Collections.emptySet());
    }

    private static Collection<Mono<Set<ValidationError>>> getMonos(
            JobDescriptor jobDescriptor,
            Duration timeout,
            Collection<EntityValidator<JobDescriptor>> validators,
            ValidationError.Type errorType) {

        return validators.stream()
                .map(v -> new Pair<>(v.getClass().getSimpleName(), v.validate(jobDescriptor))) // (ValidatorClassName, Mono)
                .map(pair -> pair.getRight().timeout(
                        timeout,
                        Mono.just(
                                new HashSet<>(Arrays.asList(
                                // Field: ValidatorClassName, Description: TimeoutMessage, Type: [SOFT|HARD]
                                new ValidationError(pair.getLeft(), getTimeoutMsg(timeout), errorType))))))
                .collect(Collectors.toList());
    }

    static String getTimeoutMsg(Duration timeout) {
        return String.format("Timed out in %s ms", timeout.toMillis());
    }
}
