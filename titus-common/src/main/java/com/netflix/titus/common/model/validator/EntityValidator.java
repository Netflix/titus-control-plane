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

package com.netflix.titus.common.model.validator;

import reactor.core.publisher.Mono;

import java.util.Set;

/**
 * A EntityValidator determines whether an object of the parameterized type is valid.  If it finds an object to be invalid it
 * returns a non-empty set of {@link ValidationError}s.
 *
 * @param <T> The type of object this EntityValidator validates.
 */
public interface EntityValidator<T> {
    Mono<Set<ValidationError>> validate(T entity);

    /**
     * Async method to clean and add missing data elements to an entity
     *
     * @return Returns a Mono/Single that emits an a cleaned up version. If no changes were made,
     * the emitted item will share reference with the source parameter.
     */
    Mono<T> sanitize(T entity);
}
