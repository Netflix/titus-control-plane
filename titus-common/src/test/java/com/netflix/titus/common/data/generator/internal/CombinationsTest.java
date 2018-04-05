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

package com.netflix.titus.common.data.generator.internal;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class CombinationsTest {

    private static final List<int[]> COMBINATIONS_2x2 = asList(new int[]{0, 0}, new int[]{0, 1}, new int[]{1, 0}, new int[]{1, 1});
    private static final List<int[]> COMBINATIONS_3x3 = asList(
            new int[]{0, 0}, new int[]{0, 1}, new int[]{0, 2},
            new int[]{1, 0}, new int[]{1, 1}, new int[]{1, 2},
            new int[]{2, 0}, new int[]{2, 1}, new int[]{2, 2}
    );

    @Test
    public void testInit() throws Exception {
        Combinations combinations = Combinations.newInstance(2).resize(asList(2, 2));

        assertThat(combinations.getSize()).isEqualTo(4);

        List<int[]> tuples = take(combinations, 4);
        assertThat(tuples).containsAll(COMBINATIONS_2x2);
    }

    @Test
    public void testResize() throws Exception {
        Combinations combinations = Combinations.newInstance(2)
                .resize(asList(2, 2))
                .resize(asList(3, 3));

        List<int[]> tuples = take(combinations, 9);
        assertThat(tuples.get(0)).isIn(COMBINATIONS_2x2);
        assertThat(tuples.get(1)).isIn(COMBINATIONS_2x2);
        assertThat(tuples).containsAll(COMBINATIONS_3x3);
    }

    private List<int[]> take(Combinations combinations, int count) {
        List<int[]> tuples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            tuples.add(combinations.combinationAt(i));
        }
        return tuples;
    }
}