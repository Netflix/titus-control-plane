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

package com.netflix.titus.runtime.endpoint.admission;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.common.model.admission.TitusValidatorConfiguration;
import com.netflix.titus.common.model.admission.ValidatorMetrics;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * An AggregatingValidator executes and aggregates the results of multiple {@link AdmissionValidator}s.
 */
@Singleton
public class AggregatingValidator implements AdmissionValidator<JobDescriptor> {
    private final TitusValidatorConfiguration configuration;
    private final Duration timeout;
    private final Collection<? extends AdmissionValidator<JobDescriptor>> validators;
    private final ValidatorMetrics validatorMetrics;

    /**
     * A AggregatingValidator ensures that all its constituent validators or sanitizers complete within a specified
     * duration. Each validator specifies its error type, either "hard" or "soft". The distinction between "hard" and
     * "soft" validators is behavior on timeout.  When a "hard" validator
     * fails to complete within the specified duration it produces a {@link ValidationError} with a
     * {@link ValidationError.Type} of HARD.  Conversely "soft" validators which fail to complete within the specified
     * duration have a {@link ValidationError.Type} of SOFT.
     *
     * @param configuration The configuration of the validator.
     */
    @Inject
    public AggregatingValidator(
            TitusValidatorConfiguration configuration,
            Registry registry,
            Collection<? extends AdmissionValidator<JobDescriptor>> validators) {
        this.configuration = configuration;
        this.timeout = Duration.ofMillis(this.configuration.getTimeoutMs());
        this.validators = validators;
        this.validatorMetrics = new ValidatorMetrics(this.getClass().getSimpleName(), registry);
    }

    /**
     * Validate executes all of the hard and soft validators in parallel. Validation errors for both are
     * returned and noted if the specific error was a hard or soft failure.
     */
    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor jobDescriptor) {
        return Mono.zip(
                getMonos(jobDescriptor, timeout, validators),
                objects -> Arrays.stream(objects)
                        .map(objectSet -> (Set<ValidationError>) objectSet)
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet()))
                .defaultIfEmpty(Collections.emptySet());
    }

    @Override
    public ValidationError.Type getErrorType() {
        return configuration.toValidatorErrorType();
    }

    private Collection<Mono<Set<ValidationError>>> getMonos(
            JobDescriptor jobDescriptor,
            Duration timeout,
            Collection<? extends AdmissionValidator<JobDescriptor>> validators) {

        return validators.stream()
                .map(v -> v.validate(jobDescriptor)
                        .subscribeOn(Schedulers.parallel())
                        .timeout(timeout, Mono.just(Collections.singleton(
                                new ValidationError(v.getClass().getSimpleName(), getTimeoutMsg(timeout), v.getErrorType())
                        )))
                        .switchIfEmpty(Mono.just(Collections.emptySet()))
                        .doOnSuccessOrError(this::registerMetrics))
                .collect(Collectors.toList());
    }

    private void registerMetrics(Collection<ValidationError> validationErrors,
                                 Throwable throwable) {
        if (null == throwable) {
            validationErrors.forEach(validationError -> {
                validatorMetrics.incrementValidationError(
                        validationError.getField(),
                        validationError.getDescription(),
                        Collections.singletonMap("type", validationError.getType().name()));
            });
        } else {
            validatorMetrics.incrementValidationError(this.getClass().getSimpleName(), throwable.getClass().getSimpleName());
        }
    }

    public static String getTimeoutMsg(Duration timeout) {
        return String.format("Timed out in %s ms", timeout.toMillis());
    }
}
