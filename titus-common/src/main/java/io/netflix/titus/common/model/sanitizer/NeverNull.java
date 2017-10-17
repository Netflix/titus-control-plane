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

package io.netflix.titus.common.model.sanitizer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

import io.netflix.titus.common.model.sanitizer.internal.NeverNullValidator;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Class level constraint, requiring all JavaBean fields to have non-null values.
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = {NeverNullValidator.class})
public @interface NeverNull {

    String message() default "{NeverNull.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /**
     * Defines several {@link NeverNull} annotations on the same element.
     */
    @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
    @Retention(RUNTIME)
    @Documented
    @interface List {

        NeverNull[] value();
    }
}
