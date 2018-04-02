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

package com.netflix.titus.common.model.sanitizer.internal;

import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;

import com.netflix.titus.common.model.sanitizer.TestModel.Child;
import com.netflix.titus.common.model.sanitizer.TestModel.NullableChild;
import com.netflix.titus.common.model.sanitizer.TestModel.Root;
import com.netflix.titus.common.model.sanitizer.TestValidator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClassFieldsNotNullValidatorTest {

    private Validator validator;

    @Before
    public void setUp() throws Exception {
        validator = TestValidator.testStrictValidator();
    }

    @Test
    public void testNullTopLevelField() throws Exception {
        Root root = new Root(
                null,
                new Child("child1", 0, 2),
                new NullableChild("child2")
        );
        Set<ConstraintViolation<Root>> violations = validator.validate(root);
        System.out.println(violations);
        assertThat(violations).hasSize(1);
    }

    @Test
    public void testNullNestedField() throws Exception {
        Root root = new Root(
                "Root1",
                new Child(null, 0, 2),
                new NullableChild(null)
        );
        Set<ConstraintViolation<Root>> violations = validator.validate(root);
        System.out.println(violations);
        assertThat(violations).hasSize(1);
    }
}