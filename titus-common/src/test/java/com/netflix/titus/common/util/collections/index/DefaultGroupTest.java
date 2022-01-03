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

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultGroupTest {

    private static final IndexSpec<Character, String, SampleItem, String> SPEC = IndexSpec.<Character, String, SampleItem, String>newBuilder()
            .withIndexKeyExtractor(item -> item.getKey().charAt(0))
            .withPrimaryKeyExtractor(SampleItem::getKey)
            .withIndexKeyComparator(Character::compareTo)
            .withPrimaryKeyComparator(String::compareTo)
            .withTransformer(SampleItem::getValue)
            .build();

    private static final DefaultGroup<Character, String, SampleItem, String> EMPTY_GROUP = DefaultGroup.newEmpty(SPEC);

    @Test
    public void testAdd() {
        // Initial
        DefaultGroup<Character, String, SampleItem, String> group = EMPTY_GROUP.add(SampleItem.newItems("a1", "#a1", "a2", "#a2", "b1", "#b1"));
        assertThat(group.get()).containsKeys('a', 'b');
        assertThat(group.get().get('a')).containsEntry("a1", "#a1");
        assertThat(group.get().get('a')).containsEntry("a2", "#a2");
        assertThat(group.get().get('b')).containsEntry("b1", "#b1");

        // Add to existing subset.
        group = group.add(SampleItem.newItems("b2", "#b2"));
        assertThat(group.get().get('b')).containsEntry("b1", "#b1");
        assertThat(group.get().get('b')).containsEntry("b2", "#b2");

        // Add new subset
        group = group.add(SampleItem.newItems("c1", "#c1"));
        assertThat(group.get().get('c')).containsEntry("c1", "#c1");
    }

    @Test
    public void testRemove() {
        DefaultGroup<Character, String, SampleItem, String> group = EMPTY_GROUP.add(SampleItem.newItems("a1", "#a1", "a2", "#a2", "b1", "#b1"));

        // Non empty result subset
        group = group.remove(Collections.singleton("a1"));
        assertThat(group.get().get('a')).containsOnlyKeys("a2");

        // Remove non-existent key
        DefaultGroup<Character, String, SampleItem, String> noChange = group.remove(Collections.singleton("a1"));
        assertThat(group == noChange).isTrue();

        // Empty subset
        group = group.remove(Collections.singleton("a2"));
        assertThat(group.get().get('a')).isNull();

    }
}