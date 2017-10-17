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

package io.netflix.titus.common.model.sanitizer.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import static io.netflix.titus.common.util.ReflectionExt.getAllFields;
import static java.lang.String.format;
import static java.util.Arrays.stream;

class JavaBeanReflection {

    private static final ConcurrentMap<Class<?>, JavaBeanReflection> CACHE = new ConcurrentHashMap<>();

    private final Constructor<?> constructor;
    private final List<Field> fields;

    JavaBeanReflection(Class<?> entityType) {
        Preconditions.checkArgument(entityType.getConstructors().length == 1, "Expected single constructor in class %s", entityType);
        this.constructor = entityType.getConstructors()[0];

        Map<String, Field> fieldsByName = getAllFields(entityType).stream()
                .filter(f -> !Modifier.isStatic(f.getModifiers()) && !f.getName().startsWith("$")) // Static fields and instrumentation (jacoco)
                .collect(Collectors.toMap(Field::getName, Function.identity()));
        this.fields = stream(constructor.getParameters())
                .map(p -> {
                    Field field = fieldsByName.get(p.getName());
                    Preconditions.checkNotNull(field, "Constructor parameter %s does not map to any field in class %s", p.getName(), entityType);
                    return field;
                })
                .collect(Collectors.toList());
    }

    Object create(Object entity, Map<Field, Object> overrides) {
        List<Object> newValues = new ArrayList<>();
        for (Field field : fields) {
            Object newValue = overrides.get(field);
            if (newValue != null) {
                newValues.add(newValue);
            } else {
                newValues.add(getFieldValue(entity, field));
            }
        }
        try {
            return constructor.newInstance(newValues.toArray());
        } catch (Exception e) {
            throw new IllegalArgumentException(format("Cannot instantiate %s with constructor arguments %s", entity.getClass(), newValues), e);
        }
    }

    List<Field> getFields() {
        return fields;
    }

    Object getFieldValue(Object entity, Field field) {
        try {
            return field.get(entity);
        } catch (Exception e) {
            throw new IllegalStateException(format("Cannot access value of field %s on %s", field.getName(), entity.getClass()), e);
        }
    }

    static JavaBeanReflection forType(Class<?> entityType) {
        return CACHE.computeIfAbsent(entityType, JavaBeanReflection::new);
    }
}
