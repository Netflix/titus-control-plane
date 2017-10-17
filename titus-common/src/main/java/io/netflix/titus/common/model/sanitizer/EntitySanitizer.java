/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.model.sanitizer;

import java.util.Optional;
import java.util.Set;
import javax.validation.ConstraintViolation;

/**
 * Whenever data is exchanged with an external entity (REST, database), there is a risk, that the entity
 * invariants are violated. This may have adverse impact on system availability and correctness of execution.
 * For example, bad one entity data stored in a database, may affect all operations operating on entity collection.
 * <h1>Constraints</h1>
 * Validated/sanitized entities must be JavaBean types with single constructor.
 */
public interface EntitySanitizer {

    /**
     * Validate an entity and report constraint violations.
     *
     * @return failed constraints or empty set if entity is valid
     */
    <T> Set<ConstraintViolation<T>> validate(T entity);

    /**
     * Cleans and adds missing data elements to an entity.
     *
     * @return {@link Optional#empty()} if the entity does not require any updates, or a new, cleaned up version
     */
    <T> Optional<T> sanitize(T entity);
}
