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
import org.springframework.expression.spel.support.StandardEvaluationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class AnnotationBasedSanitizerTest {

    @Test
    public void testSanitizerOption() throws Exception {
        Root root = new Root(
                "root1",
                new Child("child1", 20, 1),
                new NullableChild("child2")
        );

        Root sanitized = apply(root);

        // Check that min/max order has been fixed
        Child child = sanitized.getChild();
        assertThat(child.getChildName()).isEqualTo("child1");
        assertThat(child.getMin()).isEqualTo(20);
        assertThat(child.getMax()).isEqualTo(20);
    }

    @Test
    public void testAtLeastOption() throws Exception {
        Root root = new Root(
                "root1",
                new Child("child1", 2, 5),
                new NullableChild("child2")
        );
        Root sanitized = apply(root);
        assertThat(sanitized.getChild().getMax()).isEqualTo(10);
    }

    @Test
    public void testAtMostOption() throws Exception {
        Root root = new Root(
                "root1",
                new Child("child1", 2, 150),
                new NullableChild("child2")
        );
        Root sanitized = apply(root);
        assertThat(sanitized.getChild().getMax()).isEqualTo(100);
    }

    @Test
    public void testAdjusterOption() throws Exception {
        Root root = new Root(
                "root1",
                new Child("child1", 5, 25),
                new NullableChild("child2")
        );
        Root sanitized = apply(root);
        assertThat(sanitized.getChild().getMax()).isEqualTo(24);
    }

    private Root apply(Root root) {
        Optional<Object> sanitizedOpt = new AnnotationBasedSanitizer(new StandardEvaluationContext(), t -> true).apply(root);
        assertThat(sanitizedOpt).isPresent();
        return (Root) sanitizedOpt.get();
    }
}