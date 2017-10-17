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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import io.netflix.titus.common.model.sanitizer.Template;
import io.netflix.titus.common.util.ReflectionExt;

/**
 */
public class TemplateSanitizer extends AbstractFieldSanitizer<String> {

    private final Function<String, Optional<Object>> templateResolver;
    private final Function<Class<?>, Boolean> innerEntityPredicate;

    public TemplateSanitizer(Function<String, Optional<Object>> templateResolver,
                             Function<Class<?>, Boolean> innerEntityPredicate) {
        this.templateResolver = templateResolver;
        this.innerEntityPredicate = innerEntityPredicate;
    }

    @Override
    public Optional<Object> apply(Object entity) {
        return entity != null ? apply(entity, "") : Optional.empty();
    }

    @Override
    protected Optional<Object> sanitizeFieldValue(Field field, Object value, String path) {
        String fieldPath = path.isEmpty() ? field.getName() : path + '.' + field.getName();
        if (value == null) {
            return isEnabled(field) ? templateResolver.apply(fieldPath) : Optional.empty();
        }
        Class<?> fieldType = field.getType();

        // Process empty collection/map/optional/string
        if (Collection.class.isAssignableFrom(fieldType)) {
            Collection<?> collectionValue = (Collection<?>) value;
            if (collectionValue.isEmpty() && replaceEmptyValue(field)) {
                return templateResolver.apply(fieldPath);
            }
        } else if (Map.class.isAssignableFrom(fieldType)) {
            Map<?, ?> mapValue = (Map<?, ?>) value;
            if (mapValue.isEmpty() && replaceEmptyValue(field)) {
                return templateResolver.apply(fieldPath);
            }
        } else if (Optional.class == fieldType) {
            Optional optionalValue = (Optional) value;
            if (!optionalValue.isPresent() && replaceEmptyValue(field)) {
                return templateResolver.apply(fieldPath);
            }
        } else if (String.class == fieldType) {
            String stringValue = (String) value;
            if (stringValue.isEmpty() && replaceEmptyValue(field)) {
                return templateResolver.apply(fieldPath);
            }
        }

        // Skip primitive type or collections/maps/optional
        if (ReflectionExt.isStandardDataType(fieldType) || ReflectionExt.isContainerType(field)) {
            return Optional.empty();
        }

        if (!innerEntityPredicate.apply(fieldType)) {
            return Optional.empty();
        }
        return apply(value, fieldPath);
    }

    private static boolean isEnabled(Field field) {
        return field.getAnnotation(Template.class) != null;
    }

    private static boolean replaceEmptyValue(Field field) {
        Template annotation = field.getAnnotation(Template.class);
        return annotation != null && annotation.onEmpty();
    }
}
