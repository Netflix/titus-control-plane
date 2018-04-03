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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 */
abstract class AbstractFieldSanitizer<CONTEXT> implements Function<Object, Optional<Object>> {

    static final Object NOTHING = new Object();

    protected Optional<Object> apply(Object entity, CONTEXT context) {
        JavaBeanReflection javaBeanRefl = JavaBeanReflection.forType(entity.getClass());

        Map<Field, Object> fixedValues = new HashMap<>();
        javaBeanRefl.getFields().forEach(field -> {
            Object fieldValue = javaBeanRefl.getFieldValue(entity, field);
            sanitizeFieldValue(field, fieldValue, context).ifPresent(newValue -> fixedValues.put(field, newValue));
        });

        if (fixedValues.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(javaBeanRefl.create(entity, fixedValues));
    }

    protected abstract Optional<Object> sanitizeFieldValue(Field field, Object value, CONTEXT context);
}
