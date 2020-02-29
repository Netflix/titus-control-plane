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
import java.util.Set;
import javax.validation.Constraint;
import javax.validation.Payload;

import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.common.model.sanitizer.internal.AbstractConstraintValidator;
import com.netflix.titus.common.model.sanitizer.internal.ConstraintValidatorContextWrapper;

import static java.lang.annotation.ElementType.TYPE;

/**
 *
 */
public class SchedulingConstraintSetValidator extends AbstractConstraintValidator<SchedulingConstraintSetValidator.SchedulingConstraintSet, Container> {

    @Target({TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @Constraint(validatedBy = {SchedulingConstraintSetValidator.class})
    public @interface SchedulingConstraintSet {

        String message() default "{SoftAndHardConstraint.message}";

        Class<?>[] groups() default {};

        Class<? extends Payload>[] payload() default {};
    }

    @Override
    public void initialize(SchedulingConstraintSet constraintAnnotation) {
    }

    @Override
    public boolean isValid(Container container, ConstraintValidatorContextWrapper context) {
        if (container == null) {
            return true;
        }
        Set<String> common = new HashSet<>(container.getSoftConstraints().keySet());
        common.retainAll(container.getHardConstraints().keySet());

        if (common.isEmpty()) {
            return true;
        }

        context.buildConstraintViolationWithStaticMessage(
                "Soft and hard constraints not unique. Shared constraints: " + common
        ).addConstraintViolation().disableDefaultConstraintViolation();
        return false;
    }
}
