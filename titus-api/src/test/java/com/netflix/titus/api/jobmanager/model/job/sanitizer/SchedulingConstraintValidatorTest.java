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

package com.netflix.titus.api.jobmanager.model.job.sanitizer;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.JobConstraints;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class SchedulingConstraintValidatorTest {

    private Validator validator;

    private static final String EL_EXPRESSION_MESSAGE = "#{#this.class.name.substring(0,5) == 'com.g' ? 'FOO' : T(java.lang.Runtime).getRuntime().exec(new java.lang.String(T(java.util.Base64).getDecoder().decode('dG91Y2ggL3RtcC9wd25lZA=='))).class.name}";
    private static final String RCE_VIOLATION_MESSAGE = "Unrecognized constraints [\\#\\{\\#this.class.name.substring(0,5) == 'com.g' ? 'foo' : t(java.lang.runtime).getruntime().exec(new java.lang.string(t(java.util.base64).getdecoder().decode('dg91y2ggl3rtcc9wd25lza=='))).class.name\\}]";

    @Before
    public void setUp() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Test
    public void testValidSchedulingConstraint() {
        Map<String, String> validConstraintMap = JobConstraints.CONSTRAINT_NAMES.stream().collect(Collectors.toMap(Function.identity(), containerName -> "random_string"));
        assertThat(validator.validate(new ConstraintMap(validConstraintMap))).isEmpty();
    }

    @Test
    public void testInvalidSchedulingConstraint() {
        String randomKey = UUID.randomUUID().toString();
        String expectedViolationString = "Unrecognized constraints [" + randomKey + "]";
        Map<String, String> invalidConstraintMap = ImmutableMap.of(randomKey, "test");
        Set<ConstraintViolation<ConstraintMap>> violations = validator.validate(new ConstraintMap(invalidConstraintMap));
        ConstraintViolation<ConstraintMap> constraintMapConstraintViolation = violations.stream().findFirst().orElseThrow(() -> new AssertionError("Expected violation not found"));
        assertThat(constraintMapConstraintViolation.getMessage()).isEqualTo(expectedViolationString);
    }

    @Test
    public void testRceAttemptInSchedulingConstraint() {
        Map<String, String> invalidConstraintMap = ImmutableMap.of(EL_EXPRESSION_MESSAGE, "test");
        Set<ConstraintViolation<ConstraintMap>> violations = validator.validate(new ConstraintMap(invalidConstraintMap));
        ConstraintViolation<ConstraintMap> constraintMapConstraintViolation = violations.stream().findFirst().orElseThrow(() -> new AssertionError("Expected violation not found"));
        assertThat(constraintMapConstraintViolation.getMessageTemplate()).isEqualTo(RCE_VIOLATION_MESSAGE);
    }

    static class ConstraintMap {

        @SchedulingConstraintValidator.SchedulingConstraint
        final Map<String, String> constraintMap;

        public ConstraintMap(Map<String, String> map) {
            this.constraintMap = map;
        }
    }
}
