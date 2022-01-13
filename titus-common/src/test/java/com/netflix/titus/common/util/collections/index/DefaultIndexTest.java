/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.collections.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultIndexTest {

    private static final IndexSpec<String, String, SampleNestedItem, String> SPEC = IndexSpec.<String, String, SampleNestedItem, String>newBuilder()
            .withIndexKeysExtractor(item -> new HashSet<>(item.getChildren().keySet()))
            .withPrimaryKeyExtractor(SampleNestedItem::getRootId)
            .withIndexKeyComparator(String::compareTo)
            .withPrimaryKeyComparator(String::compareTo)
            .withTransformer((key, value) -> value.getChildren().get(key))
            .build();

    private static final DefaultIndex<String, String, SampleNestedItem, String> EMPTY_INDEX = DefaultIndex.newEmpty(SPEC);

    @Test
    public void testAdd() {
        // Initial
        DefaultIndex<String, String, SampleNestedItem, String> index = EMPTY_INDEX.add(Arrays.asList(
                SampleNestedItem.newItem("r1", "r1c1", "v1", "r1c2", "v2"),
                SampleNestedItem.newItem("r2", "r2c1", "v1", "r2c2", "v2")
        ));
        assertThat(index.get()).hasSize(4);
        assertThat(index.get()).containsKeys("r1c1", "r1c2", "r2c1", "r2c2");

        // Change existing child.
        index = index.add(SampleNestedItem.newItemList("r1", "r1c1", "v1b", "r1c2", "v2"));
        assertThat(index.get()).hasSize(4);
        assertThat(index.get().get("r1c1")).isEqualTo("v1b");
        assertThat(index.get().get("r1c2")).isEqualTo("v2");

        // Update by removing one of the existing children.
        index = index.add(SampleNestedItem.newItemList("r1", "r1c1", "v1b"));
        assertThat(index.get()).hasSize(3);
        assertThat(index.get().get("r1c1")).isEqualTo("v1b");
        assertThat(index.get().get("r1c2")).isNull();

        // Update by adding empty children set
        index = index.add(SampleNestedItem.newItemList("r1"));
        assertThat(index.get()).hasSize(2);

        // Add empty
        index = index.add(SampleNestedItem.newItemList("r3"));
        assertThat(index.get()).hasSize(2);

        // Add to empty
        index = index.add(SampleNestedItem.newItemList("r3", "r3v1", "v1"));
        assertThat(index.get()).hasSize(3);
        assertThat(index.get()).containsEntry("r3v1", "v1");

    }

    @Test
    public void testRemove() {
        // Initial
        DefaultIndex<String, String, SampleNestedItem, String> index = EMPTY_INDEX.add(Arrays.asList(
                SampleNestedItem.newItem("r1", "r1c1", "v1", "r1c2", "v2"),
                SampleNestedItem.newItem("r2", "r2c1", "v1", "r2c2", "v2")
        ));

        // Remove
        index = index.remove(Collections.singleton("r1"));
        assertThat(index.get()).hasSize(2);
        assertThat(index.get()).containsKeys("r2c1", "r2c2");
    }
}