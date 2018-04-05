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

package com.netflix.titus.common.model.sanitizer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

import com.netflix.titus.common.model.sanitizer.internal.SpELClassValidator;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for expressing class level invariants written in Spring SpEL language.
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = {SpELClassValidator.class})
public @interface ClassInvariant {

    /**
     * Condition should evaluate to true or false.
     */
    String condition() default "";

    /**
     * Expression that should return a map of string key/values describing fields for which validation failed.
     * Empty map indicates that the data are valid.
     */
    String expr() default "";

    VerifierMode mode() default VerifierMode.Permissive;

    String message() default "{ClassInvariant.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /**
     * Defines several {@link ClassInvariant} annotations on the same element.
     *
     * @see ClassInvariant
     */
    @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
    @Retention(RUNTIME)
    @Documented
    @interface List {
        ClassInvariant[] value();
    }
}
