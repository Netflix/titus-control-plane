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

package com.netflix.titus.common.model.sanitizer.internal;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.function.Function;

public class StdValueSanitizer extends AbstractFieldSanitizer<Object> {

    private final Function<Class<?>, Boolean> innerEntityPredicate;

    public StdValueSanitizer(Function<Class<?>, Boolean> innerEntityPredicate) {
        this.innerEntityPredicate = innerEntityPredicate;
    }

    @Override
    public Optional<Object> apply(Object entity) {
        return apply(entity, NOTHING);
    }

    @Override
    protected Optional<Object> sanitizeFieldValue(Field field, Object fieldValue, Object context) {
        Class<?> fieldType = field.getType();
        if (fieldType.isPrimitive()) {
            return Optional.empty();
        }
        if (fieldType == String.class) {
            return doStringCleanup((String) fieldValue);
        }
        if (!fieldType.isAssignableFrom(Enum.class) && fieldValue != null && innerEntityPredicate.apply(fieldValue.getClass())) {
            return apply(fieldValue, NOTHING);
        }
        return Optional.empty();
    }

    private Optional<Object> doStringCleanup(String value) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String trimmed = value.trim();
        return value == trimmed ? Optional.empty() : Optional.of(trimmed);
    }
}
