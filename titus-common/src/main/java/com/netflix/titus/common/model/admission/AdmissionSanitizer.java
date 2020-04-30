/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.common.model.admission;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import reactor.core.publisher.Mono;

/**
 * Sanitizers have a MapReduce-like interface, and are executed concurrently in two phases:
 * <p>
 * 1. <tt>sanitize(input)</tt> is called on all registered {@link AdmissionSanitizer sanitizers} concurrently. The input
 * parameter may or may not have passed through other sanitizers, no assumptions on what data has been already changed
 * during the sanitization process can be made. It is safer to assume no other sanitizers should touch the same data,
 * and the input is the original value sent to the system. The behavior with multiple sanitizers with overlapping
 * updates is unspecified.
 * <p>
 * 2. The previous call returns a {@link Function} that applies a partial update, and partial updates are chained
 * through all sanitizers sequentially. Each sanitizer adds its partial update to the entity modified by its
 * predecessor in the chain. The final result includes all partial updates from all sanitizers. No assumptions should be
 * made on the order in which sanitizers will be called.
 *
 * @param <T> entity being sanitized
 */
public interface AdmissionSanitizer<T> {

    /**
     * Async method that produces clean and missing data elements to an entity. Its return should be a synchronous
     * function (closure) that applies all necessary updates. All heavy lifting should be done asynchronously, with the
     * returned <tt>apply</tt> function being cheap and only responsible for merging sanitized data back into the
     * entity.
     *
     * @return a closure that applies necessary changes to the entity. It may be a {@link Mono#empty()} when no changes
     * are to be made, or a {@link Mono#error(Throwable)} when the entity is invalid
     */
    Mono<UnaryOperator<T>> sanitize(T entity);

    /**
     * Helper to be used when there are no multiple sanitizers to be executed concurrently.
     */
    default Mono<T> sanitizeAndApply(T entity) {
        return sanitize(entity).map(update -> update.apply(entity));
    }
}
