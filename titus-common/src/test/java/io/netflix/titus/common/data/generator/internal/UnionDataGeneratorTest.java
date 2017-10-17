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

package io.netflix.titus.common.data.generator.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import io.netflix.titus.common.data.generator.DataGenerator;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class UnionDataGeneratorTest {
    @Test
    public void testUnion() throws Exception {
        List<String> words = asList("a", "b", "c", "d", "e");

        DataGenerator<List<String>> batchGen = DataGenerator.union(
                DataGenerator.items(words),
                DataGenerator.range(0, 5),
                (t, n) -> t + '-' + n
        ).batch(25);

        List<String> data = batchGen.getValue();
        data.sort(Comparator.naturalOrder());

        List<String> expected = words.stream().flatMap(w -> {
            List<String> items = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                items.add(w + '-' + i);
            }
            return items.stream();
        }).collect(Collectors.toList());

        assertThat(data).containsAll(expected);
        assertThat(batchGen.apply().isClosed()).isTrue();
    }
}