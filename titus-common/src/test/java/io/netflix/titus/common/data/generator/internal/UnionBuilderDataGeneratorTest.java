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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netflix.titus.common.data.generator.DataGenerator;
import org.junit.Test;

import static io.netflix.titus.common.data.generator.DataGenerator.items;
import static org.assertj.core.api.Assertions.assertThat;

public class UnionBuilderDataGeneratorTest {

    @Test
    public void testUnionBuilder() throws Exception {
        DataGenerator<String> values = DataGenerator.unionBuilder(SampleBuilder::new)
                .combine(items("a", "b", "a", "b"), SampleBuilder::withA)
                .combine(items("x", "y", "z"), SampleBuilder::withB)
                .combine(items("1", "2", "3", "4"), SampleBuilder::withC)
                .map(SampleBuilder::build);

        List<String> expected = Stream.of("a", "b", "a", "b").
                flatMap(i1 -> Stream.of("x", "y", "z").map(i2 -> i1 + i2))
                .flatMap(i12 -> Stream.of("1", "2", "3", "4").map(i3 -> i12 + i3))
                .collect(Collectors.toList());

        List<String> actual = values.toList();

        assertThat(actual).hasSize(expected.size());
        assertThat(actual).containsAll(expected);
    }

    static class SampleBuilder {
        private String a;
        private String b;
        private String c;

        SampleBuilder withA(String a) {
            this.a = a;
            return this;
        }

        SampleBuilder withB(String b) {
            this.b = b;
            return this;
        }

        SampleBuilder withC(String c) {
            this.c = c;
            return this;
        }

        String build() {
            return a + b + c;
        }
    }
}