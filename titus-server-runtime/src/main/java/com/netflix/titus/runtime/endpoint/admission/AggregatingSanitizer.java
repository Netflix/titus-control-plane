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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.util.CollectionsExt;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Singleton
public class AggregatingSanitizer implements AdmissionSanitizer<JobDescriptor> {
    private final Collection<? extends AdmissionSanitizer<JobDescriptor>> sanitizers;
    private final Duration timeout;

    @Inject
    public AggregatingSanitizer(TitusValidatorConfiguration configuration, Collection<? extends AdmissionSanitizer<JobDescriptor>> sanitizers) {
        this.sanitizers = sanitizers;
        this.timeout = Duration.ofMillis(configuration.getTimeoutMs());
    }

    /**
     * Executes all sanitizers in parallel, collecting partial results in the first phase, and applying them all
     * sequentially through each sanitizer in the second phase.
     * <p>
     * The iteration order of the sanitizers is not guaranteed. Any sanitization failure results in the Mono
     * emitting an error.
     *
     * @return a {@link Function} that applies partial updates through all sanitizers sequentially
     */
    public Mono<UnaryOperator<JobDescriptor>> sanitize(JobDescriptor entity) {
        List<Mono<UnaryOperator<JobDescriptor>>> sanitizationFunctions = sanitizers.stream()
                .map(s -> s.sanitize(entity)
                        .subscribeOn(Schedulers.parallel())
                        // important: Mono.zip will be short circuited if members are empty
                        .switchIfEmpty(Mono.just(UnaryOperator.identity()))
                )
                .collect(Collectors.toList());

        Mono<UnaryOperator<JobDescriptor>> merged = Mono.zip(sanitizationFunctions, partials -> {
            if (CollectionsExt.isNullOrEmpty(partials)) {
                return UnaryOperator.identity();
            }
            //noinspection unchecked
            return applyAll(Arrays.stream(partials).map(partial -> (UnaryOperator<JobDescriptor>) partial));
        });

        return merged.switchIfEmpty(Mono.just(UnaryOperator.identity()))
                .timeout(timeout, Mono.error(() -> TitusServiceException.internal("Job sanitization timed out")));

    }

    private static UnaryOperator<JobDescriptor> applyAll(Stream<UnaryOperator<JobDescriptor>> partialUpdates) {
        return entity -> {
            AtomicReference<JobDescriptor> result = new AtomicReference<>(entity);
            partialUpdates.forEachOrdered(result::updateAndGet);
            return result.get();
        };
    }

}
