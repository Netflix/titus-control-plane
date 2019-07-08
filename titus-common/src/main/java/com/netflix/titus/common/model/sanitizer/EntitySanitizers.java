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

package com.netflix.titus.common.model.sanitizer;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * A collection of predefined sanitizers.
 */
public class EntitySanitizers {

    private static final EntitySanitizer ALWAYS_VALID = new EntitySanitizer() {
        @Override
        public <T> Set<ValidationError> validate(T entity) {
            return Collections.emptySet();
        }

        @Override
        public <T> Optional<T> sanitize(T entity) {
            return Optional.empty();
        }
    };

    /**
     * Sanitizer that accepts all data as valid, and performs no sanitization.
     */
    public static EntitySanitizer alwaysValid() {
        return ALWAYS_VALID;
    }
}
