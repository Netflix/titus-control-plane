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

import java.util.Optional;
import java.util.Set;
import javax.validation.ConstraintViolation;

import com.netflix.titus.common.model.sanitizer.TestModel.Child;
import com.netflix.titus.common.model.sanitizer.TestModel.NullableChild;
import com.netflix.titus.common.model.sanitizer.TestModel.Root;
import com.netflix.titus.common.model.sanitizer.TestModel.StringWithPrefixCheck;
import org.junit.Test;

import static com.netflix.titus.common.util.ReflectionExt.isStandardDataType;
import static org.assertj.core.api.Assertions.assertThat;

public class EntitySanitizerBuilderTest {

    private final EntitySanitizer sanitizer = EntitySanitizerBuilder.stdBuilder()
            .processEntities(type -> !isStandardDataType(type))
            .addTemplateResolver(path -> path.equals("child.childName") ? Optional.of("GuestChild") : Optional.empty())
            .registerFunction("fit", TestModel.getFitMethod())
            .registerBean("myObj", new TestModel.SampleValidationMethods("test"))
            .build();


    @Test
    public void testCompleteSetup() throws Exception {
        TestModel.Root root = new TestModel.Root(
                null,
                new TestModel.Child(null, -2, 1),
                new TestModel.NullableChild(null)
        );

        // Null values violation + min
        Set<ConstraintViolation<TestModel.Root>> violations = sanitizer.validate(root);
        System.out.println(violations);
        assertThat(violations).hasSize(3);

        // Now fix this by sanitizing
        TestModel.Root sanitizedRoot = sanitizer.sanitize(root).get();

        Set<ConstraintViolation<TestModel.Root>> rangeViolations = sanitizer.validate(sanitizedRoot);
        System.out.println(violations);
        assertThat(rangeViolations).hasSize(2);
    }

    @Test
    public void testRegisteredObjects() throws Exception {
        Set<ConstraintViolation<TestModel.StringWithPrefixCheck>> violations = sanitizer.validate(new TestModel.StringWithPrefixCheck("testXXX"));
        assertThat(violations).isEmpty();
    }
}