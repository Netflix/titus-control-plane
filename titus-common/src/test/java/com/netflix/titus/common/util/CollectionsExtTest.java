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

package com.netflix.titus.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.common.util.CollectionsExt.xor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CollectionsExtTest {

    @Test
    public void testSetXor() {
        assertThat(xor(asSet(1, 2, 3), asSet(2, 3, 4), asSet(3, 4, 5))).contains(1, 5);
    }

    @Test
    public void testContainsAnyKeys() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        assertThat(CollectionsExt.containsAnyKeys(map, "a", "c")).isTrue();
        assertThat(CollectionsExt.containsAnyKeys(map, "c", "a")).isTrue();
        assertThat(CollectionsExt.containsAnyKeys(map, "c", "d")).isFalse();
    }

    @Test
    public void testBinarySearchLeftMostCornerCases() {
        testBinarySearchLeftMost(Collections.emptyList(), 1);
        testBinarySearchLeftMost(Collections.singletonList(1), -100);
        testBinarySearchLeftMost(Collections.singletonList(1), 100);
    }

    @Test
    public void testBinarySearchLeftMostNoDuplicates() {
        Random random = new Random(123);
        Set<Integer> numbers = new HashSet<>();
        while (numbers.size() < 100) {
            numbers.add(2 * random.nextInt(1_000));
        }
        List<Integer> orderedNumbers = new ArrayList<>(numbers);
        orderedNumbers.sort(Integer::compare);

        for (int i = 0; i < numbers.size(); i++) {
            for (int delta = -1; delta <= 1; delta++) {
                testBinarySearchLeftMost(orderedNumbers, orderedNumbers.get(i) + delta);
            }
        }
    }

    @Test
    public void testBinarySearchLeftMostWithDuplicates() {
        List<Integer> orderedNumbers = Arrays.asList(1, 2, 3, 3, 3, 5);
        int result = CollectionsExt.binarySearchLeftMost(
                orderedNumbers,
                item -> Integer.compare(3, item)
        );
        assertThat(result).isEqualTo(2);
    }

    private void testBinarySearchLeftMost(List<Integer> orderedNumbers, int target) {
        int result = CollectionsExt.binarySearchLeftMost(
                orderedNumbers,
                item -> Integer.compare(target, item)
        );
        int stdResult = Collections.binarySearch(orderedNumbers, target);
        if (result != stdResult) {
            fail("Result different from returned by JDK binarySearch function: list=%s, our=%s, std=%s",
                    orderedNumbers, result, stdResult
            );
        }
    }

    @Test
    public void mapLowerCaseKeys() {
        Map<String, String> result = CollectionsExt.toLowerCaseKeys(CollectionsExt.asMap("a", "foo", "b", "bar", "mIxEd", "UntouChed"));
        assertThat(result).containsOnlyKeys("a", "b", "mixed");
        assertThat(result).containsValues("foo", "bar", "UntouChed");
    }
}