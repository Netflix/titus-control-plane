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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.ConstraintValidatorContext;

import com.netflix.titus.common.model.sanitizer.CollectionInvariants;

public class CollectionValidator extends AbstractConstraintValidator<CollectionInvariants, Object> {

    private CollectionInvariants constraintAnnotation;

    @Override
    public void initialize(CollectionInvariants constraintAnnotation) {
        this.constraintAnnotation = constraintAnnotation;
    }

    @Override
    protected boolean isValid(Object value, Function<String, ConstraintValidatorContext.ConstraintViolationBuilder> constraintViolationBuilderFunction) {

        if (value == null) {
            return true;
        }
        if (value instanceof Collection) {
            return isValid((Collection<?>) value, constraintViolationBuilderFunction);
        }
        if (value instanceof Map) {
            return isValid((Map<?, ?>) value, constraintViolationBuilderFunction);
        }
        return false;
    }

    private boolean isValid(Collection<?> value, Function<String, ConstraintValidatorContext.ConstraintViolationBuilder> constraintViolationBuilderFunction) {
        if (value.isEmpty()) {
            return true;
        }

        if (!constraintAnnotation.allowNullValues()) {
            if (value.stream().anyMatch(Objects::isNull)) {
                attachMessage(constraintViolationBuilderFunction, "null values not allowed");
                return false;
            }
        }

        if (!constraintAnnotation.allowDuplicateValues()) {
            if (value.stream().distinct().count() != value.size()) {
                attachMessage(constraintViolationBuilderFunction, "duplicate values not allowed");
                return false;
            }
        }

        return true;
    }

    private boolean isValid(Map<?, ?> value, Function<String, ConstraintValidatorContext.ConstraintViolationBuilder> constraintViolationBuilderFunction) {
        if (value.isEmpty()) {
            return true;
        }

        if (!constraintAnnotation.allowEmptyKeys()) {
            if (value.keySet().stream().anyMatch(key -> key == null || (key instanceof String && ((String) key).isEmpty()))) {
                attachMessage(constraintViolationBuilderFunction, "empty key names not allowed");
                return false;
            }
        }

        if (!constraintAnnotation.allowNullKeys()) {
            if (value.keySet().stream().anyMatch(Objects::isNull)) {
                attachMessage(constraintViolationBuilderFunction, "null key names not allowed");
                return false;
            }
        }

        if (!constraintAnnotation.allowNullValues()) {
            Set<String> badEntryKeys = value.entrySet().stream()
                    .filter(e -> e.getValue() == null)
                    .map(e -> e.getKey() instanceof String ? (String) e.getKey() : "<not_string>")
                    .collect(Collectors.toSet());
            if (!badEntryKeys.isEmpty()) {
                attachMessage(constraintViolationBuilderFunction, "null values found for keys: " + new TreeSet<>(badEntryKeys));
                return false;
            }
        }

        return true;
    }

    private void attachMessage(Function<String, ConstraintValidatorContext.ConstraintViolationBuilder> constraintViolationBuilderFunction, String message) {
        constraintViolationBuilderFunction.apply(message).addConstraintViolation().disableDefaultConstraintViolation();
    }
}
