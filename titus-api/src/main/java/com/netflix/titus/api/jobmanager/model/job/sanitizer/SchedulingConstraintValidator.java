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

package com.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.Constraint;
import javax.validation.Payload;

import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.common.model.sanitizer.internal.AbstractConstraintValidator;
import com.netflix.titus.common.model.sanitizer.internal.ConstraintValidatorContextWrapper;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;

public class SchedulingConstraintValidator extends AbstractConstraintValidator<SchedulingConstraintValidator.SchedulingConstraint, Map<String, String>> {

    @Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @Constraint(validatedBy = {SchedulingConstraintValidator.class})
    public @interface SchedulingConstraint {

        String message() default "{SchedulingConstraint.message}";

        Class<?>[] groups() default {};

        Class<? extends Payload>[] payload() default {};

        /**
         * Defines several {@link SchedulingConstraint} annotations on the same element.
         *
         * @see SchedulingConstraint
         */
        @Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER})
        @Retention(RetentionPolicy.RUNTIME)
        @Documented
        @interface List {
            SchedulingConstraint value();
        }
    }

    @Override
    public void initialize(SchedulingConstraint constraintAnnotation) {
   }

    @Override
    public boolean isValid(Map<String, String> value, ConstraintValidatorContextWrapper context) {
        HashSet<String> unknown = value.keySet().stream().map(String::toLowerCase).collect(Collectors.toCollection(HashSet::new));
        unknown.removeAll(JobConstraints.CONSTRAINT_NAMES);
        if (unknown.isEmpty()) {
            return true;
        }
        context.buildConstraintViolationWithStaticMessage("Unrecognized constraints " + unknown)
                .addConstraintViolation().disableDefaultConstraintViolation();
        return false;
    }
}
