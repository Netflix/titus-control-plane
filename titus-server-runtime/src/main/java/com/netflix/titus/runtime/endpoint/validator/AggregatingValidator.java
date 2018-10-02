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

package com.netflix.titus.runtime.endpoint.validator;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * An AggregatingValidator executes and aggregates the results of multiple {@link EntityValidator}s.
 */
@Singleton
public class AggregatingValidator implements EntityValidator<JobDescriptor> {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingValidator.class);
    private final TitusValidatorConfiguration configuration;

    private final Duration timeout;
    private final Collection<EntityValidator<JobDescriptor>> hardValidators;
    private final Collection<EntityValidator<JobDescriptor>> softValidators;

    private final Collection<EntityValidator<JobDescriptor>> sanitizers;

    /**
     * A AggregatingValidator ensures that all its constituent validators or sanitizers complete within a specified
     * duration.  The distinction between "hard" and "soft" validators is behavior on timeout.  When a "hard" validator
     * fails to complete within the specified duration it produces a {@link ValidationError} with a
     * {@link ValidationError.Type} of HARD.  Conversely "soft" validators which fail to complete within the specified
     * duration have a {@link ValidationError.Type} of SOFT.
     *
     * @param configuration The configuration of the validator.
     */
    @Inject
    public AggregatingValidator(
            TitusValidatorConfiguration configuration,
            Collection<EntityValidator<JobDescriptor>> hardValidators,
            Collection<EntityValidator<JobDescriptor>> softValidators,
            Collection<EntityValidator<JobDescriptor>> sanitizers) {
        this.configuration = configuration;
        this.timeout = Duration.ofMillis(this.configuration.getTimeoutMs());
        this.hardValidators = hardValidators;
        this.softValidators = softValidators;

        this.sanitizers = sanitizers;
    }

    /**
     * Validate executes all of the hard and soft validators in parallel. Validation errors for both are
     * returned and noted if the specific error was a hard or soft failure.
     */
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

    /**
     * Sanitize executes all of sanitizers in serial, passing the sanitized result of the previous into the next.
     * The iteration order of the sanitizers is not guaranteed. Any sanitization failure results in the Mono
     * emitting an error.
     */
    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor entity) {
        Mono<JobDescriptor> sanitizedJobDescriptorMono = Mono.just(entity);
        for (EntityValidator<JobDescriptor> sanitizer : sanitizers) {
            sanitizedJobDescriptorMono = sanitizedJobDescriptorMono
                    .flatMap(sanitizer::sanitize);
        }
        return sanitizedJobDescriptorMono
                .timeout(timeout);
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

    public static String getTimeoutMsg(Duration timeout) {
        return String.format("Timed out in %s ms", timeout.toMillis());
    }
}
