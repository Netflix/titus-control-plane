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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import io.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;
import io.netflix.titus.common.util.ReflectionExt;

public class NeverNullValidator implements ConstraintValidator<ClassFieldsNotNull, Object> {

    @Override
    public void initialize(ClassFieldsNotNull constraintAnnotation) {
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        Set<String> nullFields = validate(value);
        if (nullFields.isEmpty()) {
            return true;
        }
        context.buildConstraintViolationWithTemplate(buildMessage(nullFields)).addConstraintViolation();
        context.disableDefaultConstraintViolation();
        return false;
    }

    private String buildMessage(Set<String> nullFields) {
        StringBuilder sb = new StringBuilder("'null' in fields: ");
        for (String field : nullFields) {
            sb.append(field).append(',');
        }
        return sb.substring(0, sb.length() - 1);
    }

    private Set<String> validate(Object value) {
        JavaBeanReflection jbr = JavaBeanReflection.forType(value.getClass());
        Set<String> nullFields = null;
        for (Field field : jbr.getFields()) {
            Object fieldValue = ReflectionExt.getFieldValue(field, value);
            if (fieldValue == null) {
                if (nullFields == null) {
                    nullFields = new HashSet<>();
                }
                nullFields.add(field.getName());
            }
        }
        return nullFields == null ? Collections.emptySet() : nullFields;
    }
}
