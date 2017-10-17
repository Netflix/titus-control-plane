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
import java.util.Optional;
import java.util.function.Function;

/**
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FieldSanitizer {

    /**
     * For a field holding an integer/long number, set this value if the field value is smaller.
     */
    long atLeast() default Long.MIN_VALUE;

    /**
     * For a field holding an integer/long number, set this value if the field value is larger.
     */
    long atMost() default Long.MAX_VALUE;

    /**
     * SpEL expression that should output adjusted value of the same type.
     */
    String adjuster() default "";

    /**
     * Instantiate sanitizer of this type, and apply it to the annotated field.
     */
    Class<? extends Function<Object, Optional<Object>>> sanitizer() default EmptySanitizer.class;

    class EmptySanitizer implements Function<Object, Optional<Object>> {
        @Override
        public Optional<Object> apply(Object value) {
            return Optional.empty();
        }
    }
}
