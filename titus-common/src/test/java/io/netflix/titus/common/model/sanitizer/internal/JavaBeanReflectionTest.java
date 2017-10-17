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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import io.netflix.titus.common.model.sanitizer.TestModel.Child;
import io.netflix.titus.common.model.sanitizer.TestModel.ChildExt;
import io.netflix.titus.common.model.sanitizer.TestModel.NullableChild;
import io.netflix.titus.common.model.sanitizer.TestModel.Root;
import org.junit.Test;

import static io.netflix.titus.common.util.ReflectionExt.getField;
import static org.assertj.core.api.Assertions.assertThat;

public class JavaBeanReflectionTest {

    @Test
    public void testGetFields() throws Exception {
        JavaBeanReflection jbr = JavaBeanReflection.forType(Root.class);

        Set<String> fieldNames = jbr.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        assertThat(fieldNames).contains("name", "child");
    }

    @Test
    public void testGetFieldsWithInheritanceHierarchy() throws Exception {
        JavaBeanReflection jbr = JavaBeanReflection.forType(ChildExt.class);
        Set<String> fieldNames = jbr.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        assertThat(fieldNames).contains("childName", "min", "desired", "max");
    }

    @Test
    public void testObjectCreate() throws Exception {
        JavaBeanReflection jbr = JavaBeanReflection.forType(Root.class);

        Root root = new Root(
                "root",
                new Child("child1", 1, 2),
                new NullableChild("child2")
        );
        Root updatedRoot = (Root) jbr.create(root, Collections.singletonMap(getField(Root.class, "name"), "Root"));

        assertThat(updatedRoot.getName()).isEqualTo("Root");
    }
}