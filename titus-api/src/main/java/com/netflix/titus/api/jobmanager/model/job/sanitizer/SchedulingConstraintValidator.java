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
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;

public class SchedulingConstraintValidator implements ConstraintValidator<SchedulingConstraintValidator.SchedulingConstraint, Map<String, String>> {

    /**
     * In V3 IDL we define names with first lower case latter, but actual implementation originally assumed names starting with upper case letter.
     * To avoid compatibility issues we will ignore case for constraint names.
     * TODO Convert names to IDL defined format when job is created.
     */
    private static final Set<String> CONSTRAINT_NAMES = asSet("uniquehost", "exclusivehost", "zonebalance",
            "activehost", "availabilityzone", "machineid", "machinetype");

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
    public boolean isValid(Map<String, String> value, ConstraintValidatorContext context) {
        Set<String> namesInLowerCase = value.keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
        HashSet<String> unknown = new HashSet<>(namesInLowerCase);
        unknown.removeAll(CONSTRAINT_NAMES);
        if (unknown.isEmpty()) {
            return true;
        }
        context.buildConstraintViolationWithTemplate("Unrecognized constraints " + unknown)
                .addConstraintViolation().disableDefaultConstraintViolation();
        return false;
    }
}
