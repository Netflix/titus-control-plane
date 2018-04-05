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
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.netflix.titus.common.model.sanitizer.CollectionInvariants;

/**
 */
public class CollectionValidator implements ConstraintValidator<CollectionInvariants, Object> {

    private CollectionInvariants constraintAnnotation;

    @Override
    public void initialize(CollectionInvariants constraintAnnotation) {
        this.constraintAnnotation = constraintAnnotation;
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        if (value instanceof Collection) {
            return isValid((Collection<?>) value, context);
        }
        if (value instanceof Map) {
            return isValid((Map<?, ?>) value, context);
        }
        return false;
    }

    private boolean isValid(Collection<?> value, ConstraintValidatorContext context) {
        if (value.isEmpty()) {
            return true;
        }

        if (!constraintAnnotation.allowNullValues()) {
            if (value.stream().anyMatch(Objects::isNull)) {
                return false;
            }
        }

        return true;
    }

    private boolean isValid(Map<?, ?> value, ConstraintValidatorContext context) {
        if (value.isEmpty()) {
            return true;
        }

        if (!constraintAnnotation.allowNullKeys()) {
            if (value.keySet().stream().anyMatch(Objects::isNull)) {
                return false;
            }
        }
        if (!constraintAnnotation.allowNullValues()) {
            if (value.values().stream().anyMatch(Objects::isNull)) {
                return false;
            }
        }

        return true;
    }
}
