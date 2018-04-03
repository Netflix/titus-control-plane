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

import java.util.Collections;
import java.util.Optional;

import com.netflix.titus.common.model.sanitizer.TestModel.Child;
import com.netflix.titus.common.model.sanitizer.TestModel.CollectionHolder;
import com.netflix.titus.common.model.sanitizer.TestModel.Root;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TemplateSanitizerTest {

    @Test
    public void testPrimitiveValueTemplate() throws Exception {
        Root root = new Root(
                null,
                new Child(null, 1, 2),
                null
        );

        Optional<Object> sanitizedOpt = new TemplateSanitizer(
                path -> path.equals("child.childName") ? Optional.of("ChildGuest") : Optional.empty(),
                type -> true
        ).apply(root);
        assertThat(sanitizedOpt).isPresent();

        Root sanitized = (Root) sanitizedOpt.get();
        assertThat(sanitized.getName()).isNull();
        assertThat(sanitized.getChild().getChildName()).isEqualTo("ChildGuest");
        assertThat(sanitized.getChild().getMin()).isEqualTo(1);
        assertThat(sanitized.getChild().getMax()).isEqualTo(2);
        assertThat(sanitized.getNullableChild()).isNull();
    }

    @Test
    public void testCollectionTemplate() throws Exception {
        CollectionHolder root = new CollectionHolder(Collections.emptyList());

        Optional<Object> sanitizedOpt = new TemplateSanitizer(
                path -> path.equals("values") ? Optional.of(asList("a", "b")) : Optional.empty(),
                type -> true
        ).apply(root);
        assertThat(sanitizedOpt).isPresent();
    }
}