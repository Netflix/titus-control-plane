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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.util.CollectionsExt;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Singleton
public class AggregatingSanitizer {
    private final Collection<? extends AdmissionSanitizer<JobDescriptor, ?>> sanitizers;
    private final Duration timeout;

    @Inject
    public AggregatingSanitizer(TitusValidatorConfiguration configuration, Collection<? extends AdmissionSanitizer<JobDescriptor, ?>> sanitizers) {
        this.sanitizers = sanitizers;
        this.timeout = Duration.ofMillis(configuration.getTimeoutMs());
    }

    /**
     * Executes all sanitizers in parallel, collecting partial results in the first phase, and applying them all
     * sequentially through each sanitizer in the second phase.
     * <p>
     * The iteration order of the sanitizers is not guaranteed. Any sanitization failure results in the Mono
     * emitting an error.
     */
    public Mono<JobDescriptor> sanitize(JobDescriptor entity) {
        List<Mono<SanitizerWithResult>> sanitizersWithResults = sanitizers.stream()
                .map(s -> s.sanitize(entity)
                        .subscribeOn(Schedulers.parallel())
                        .timeout(timeout)
                        .map(r -> new SanitizerWithResult(s, Optional.of(r)))
                        // important: Mono.zip will be short circuited if members are empty
                        .switchIfEmpty(Mono.just(new SanitizerWithResult(s, Optional.empty())))
                )
                .collect(Collectors.toList());

        Mono<JobDescriptor> merged = Mono.zip(sanitizersWithResults, partials -> {
            if (CollectionsExt.isNullOrEmpty(partials)) {
                return entity;
            }

            JobDescriptor result = entity;
            for (Object partial : partials) {
                SanitizerWithResult partialWithResult = (SanitizerWithResult) partial;
                Optional<?> optionalResult = partialWithResult.getResult();
                if (!optionalResult.isPresent()) {
                    continue;
                }
                result = partialWithResult.getSanitizer().apply(result, optionalResult.get());
            }
            return result;
        });

        return merged.timeout(timeout, Mono.error(() -> TitusServiceException.internal("Job sanitization timed out")))
                .switchIfEmpty(Mono.just(entity));
    }

    // Not using Pair<U, V> for generics sanity
    private static class SanitizerWithResult {
        private final AdmissionSanitizer<JobDescriptor, ?> sanitizer;
        private final Optional<?> result;

        private SanitizerWithResult(AdmissionSanitizer<JobDescriptor, ?> sanitizer, Optional<?> result) {
            this.sanitizer = sanitizer;
            this.result = result;
        }

        @SuppressWarnings("unchecked")
        public AdmissionSanitizer<JobDescriptor, Object> getSanitizer() {
            return (AdmissionSanitizer<JobDescriptor, Object>) sanitizer;
        }

        public Optional<?> getResult() {
            return result;
        }
    }
}
