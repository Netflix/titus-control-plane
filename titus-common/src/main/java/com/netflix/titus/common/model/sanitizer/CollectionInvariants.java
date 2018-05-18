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

import com.netflix.titus.common.model.sanitizer.internal.CollectionValidator;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for specifying constraint(s) on collection types (lists, sets, maps, etc).
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = {CollectionValidator.class})
public @interface CollectionInvariants {

    boolean allowNullKeys() default false;

    /**
     * Only for keys with {@link String} type. This property is ignored for other key types.
     */
    boolean allowEmptyKeys() default true;

    boolean allowNullValues() default false;

    String message() default "{CollectionInvariants.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /**
     * Defines several {@link FieldInvariant} annotations on the same element.
     *
     * @see ClassInvariant
     */
    @Target({ElementType.FIELD})
    @Retention(RUNTIME)
    @Documented
    @interface List {
        CollectionInvariants[] value();
    }
}
