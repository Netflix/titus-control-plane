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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;

import io.netflix.titus.common.model.sanitizer.TestModel.Child;
import io.netflix.titus.common.model.sanitizer.TestModel.NullableChild;
import io.netflix.titus.common.model.sanitizer.TestModel.Root;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpELClassValidatorTest {

    @Test
    public void testSpELCondition() {
        Validator validator = TestValidator.testStrictValidator();

        Root root = new Root(
                "root1",
                new Child("child1", 0, 2),
                new NullableChild("child2")
        );
        Set<ConstraintViolation<Root>> violations = validator.validate(root);

        System.out.println(violations);
        assertThat(violations).hasSize(1);
    }

    @Test
    public void testSpELExpression() {
        Validator validator = TestValidator.testValidator("myUtil", this);

        ExprCheckModel entity = new ExprCheckModel("abc");
        Set<ConstraintViolation<ExprCheckModel>> violations = validator.validate(entity);

        System.out.println(violations);
        assertThat(violations).hasSize(1);
    }

    @Test
    public void testRegisteredFunctions() {
        assertThat(testRegisteredFunctions(TestValidator.testStrictValidator())).hasSize(1);
    }

    @Test
    public void testPermissiveMode() {
        assertThat(testRegisteredFunctions(TestValidator.testPermissiveValidator())).isEmpty();
    }

    private Set<ConstraintViolation<Root>> testRegisteredFunctions(Validator validator) {
        Root root = new Root(
                "Root1",
                new Child("Child1", 0, 2),
                new NullableChild("Child2")
        );

        TestModel.setFit(false);

        Set<ConstraintViolation<Root>> violations = validator.validate(root);

        System.out.println(violations);
        return violations;
    }

    public Map<String, String> check(String name) {
        if (!name.startsWith("my")) {
            return Collections.singletonMap("name", "Should start with prefix 'my'");
        }
        return Collections.emptyMap();
    }

    @ClassInvariant(expr = "@myUtil.check(name)")
    public static class ExprCheckModel {

        private final String name;

        public ExprCheckModel(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}