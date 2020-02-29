/*
 * Copyright 2020 Netflix, Inc.
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

import javax.validation.ConstraintValidatorContext;

public class ConstraintValidatorContextWrapper {

    private final ConstraintValidatorContext constraintValidatorContext;

    /**
     * Escape all special characters that participate in EL expressions so the the message string
     * cannot be classified as a template for interpolation.
     *
     * @param message string that needs to be sanitized
     * @return copy of the input string with '{','}','#' and '$' characters escaped
     */
    private static String sanitizeMessage(String message) {
        return message.replaceAll("([}{$#])", "\\\\$1");
    }

    public ConstraintValidatorContextWrapper(ConstraintValidatorContext context) {
        this.constraintValidatorContext = context;
    }

    public ConstraintValidatorContext.ConstraintViolationBuilder buildConstraintViolationWithStaticMessage(String message) {
        String sanitizedMessage = sanitizeMessage(message);
        return this.constraintValidatorContext.buildConstraintViolationWithTemplate(sanitizedMessage);
    }
}
