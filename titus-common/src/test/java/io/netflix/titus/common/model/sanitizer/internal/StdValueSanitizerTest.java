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

package io.netflix.titus.common.model.sanitizer.internal;

import java.util.Optional;

import io.netflix.titus.common.model.sanitizer.TestModel.Child;
import io.netflix.titus.common.model.sanitizer.TestModel.NullableChild;
import io.netflix.titus.common.model.sanitizer.TestModel.Root;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StdValueSanitizerTest {

    @Test
    public void testPrimitiveDataSanitization() throws Exception {
        Root root = new Root(
                "  root1   ",
                new Child("   child1  ", 1, 2),
                new NullableChild("  child2 ")
        );

        Optional<Object> sanitizedOpt = new StdValueSanitizer(type -> true).apply(root);
        assertThat(sanitizedOpt).isPresent();

        Root sanitized = (Root) sanitizedOpt.get();
        assertThat(sanitized.getName()).isEqualTo("root1");
        assertThat(sanitized.getChild().getChildName()).isEqualTo("child1");
        assertThat(sanitized.getChild().getMin()).isEqualTo(1);
        assertThat(sanitized.getChild().getMax()).isEqualTo(2);
        assertThat(sanitized.getNullableChild().getOptionalValue()).isEqualTo("child2");
    }
}