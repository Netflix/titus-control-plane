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

import reactor.core.publisher.Mono;

/**
 * Sanitizers have a MapReduce-like interface, and are executed concurrently in two phases:
 * <p>
 * 1. <tt>sanitize(input)</tt> is called on all registered {@link AdmissionSanitizer sanitizers} concurrently. The input
 * parameter may or may not have passed through other sanitizers, no assumptions on what data has been already changed
 * during the sanitization process can be made. It is safer to assume no other sanitizers will touch the same data,
 * and the input is the original value sent to the system.
 * <p>
 * 2. <tt>apply(input, changes)</tt> is chained through all sanitizers sequentially. Each sanitizer adds its partial
 * update to the entity modified by its predecessor in the chain. The final result includes all partial updates from all
 * sanitizers. No assumptions should be made on the order in which sanitizers will be called.
 *
 * @param <T> entity being sanitized
 * @param <S> partial (sanitized) data to be merged back to the entity in the second step
 */
public interface AdmissionSanitizer<T, S> {
    /**
     * Async method that produces clean and missing data elements to an entity.
     *
     * @return Returns a Mono/Single with all necessary changes that need to be applied later. It may be a
     * {@link Mono#empty()} when no changes are to be made, or a {@link Mono#error(Throwable)} when the entity is invalid
     */
    Mono<S> sanitize(T entity);

    /**
     * Apply a partial update into the entity being sanitized.
     *
     * @return the modified entity with updates applied
     */
    T apply(T entity, S update);
}
