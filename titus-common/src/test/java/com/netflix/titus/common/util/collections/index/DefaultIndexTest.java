/*
 * Copyright 2021 Netflix, Inc.
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

import java.util.Collections;

import org.junit.Test;

import static com.netflix.titus.common.util.collections.index.SampleItem.newItems;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultIndexTest {

    private static final IndexSpec<Character, String, SampleItem, String> SPEC = IndexSpec.<Character, String, SampleItem, String>newBuilder()
            .withIndexKeyExtractor(item -> item.getKey().charAt(0))
            .withPrimaryKeyExtractor(SampleItem::getKey)
            .withIndexKeyComparator(Character::compareTo)
            .withPrimaryKeyComparator(String::compareTo)
            .withTransformer(SampleItem::getValue)
            .build();

    private static final DefaultIndex<Character, String, SampleItem, String> EMPTY_INDEX = DefaultIndex.newEmpty(SPEC);

    @Test
    public void testAdd() {
        // Initial
        DefaultIndex<Character, String, SampleItem, String> index = EMPTY_INDEX.add(newItems("a1", "#a1", "a2", "#a2", "b1", "#b1"));
        assertThat(index.orderedList()).contains("#a1", "#a2", "#b1");

        // Add
        index = index.add(newItems("b2", "#b2", "a3", "#a3"));
        assertThat(index.orderedList()).contains("#a1", "#a2", "#a3", "#b1", "#b2");
    }

    @Test
    public void testRemove() {
        DefaultIndex<Character, String, SampleItem, String> index = EMPTY_INDEX.add(
                newItems("a1", "#a1", "a2", "#a2", "b1", "#b1", "b2", "#b2")
        );
        assertThat(index.orderedList()).containsExactly("#a1", "#a2", "#b1", "#b2");

        index = index.remove(Collections.singleton("a1"));
        assertThat(index.orderedList()).containsExactly("#a2", "#b1", "#b2");

        index = index.remove(Collections.singleton("a2"));
        assertThat(index.orderedList()).containsExactly("#b1", "#b2");

        index = index.remove(Collections.singleton("b2"));
        assertThat(index.orderedList()).containsExactly("#b1");

        index = index.remove(Collections.singleton("b1"));
        assertThat(index.orderedList()).isEmpty();
    }
}