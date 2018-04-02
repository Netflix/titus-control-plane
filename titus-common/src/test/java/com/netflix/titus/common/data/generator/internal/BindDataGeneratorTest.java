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

import java.util.HashSet;
import java.util.List;

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static com.netflix.titus.common.data.generator.DataGenerator.bind;
import static com.netflix.titus.common.data.generator.DataGenerator.items;
import static org.assertj.core.api.Assertions.assertThat;

public class BindDataGeneratorTest {

    @Test
    public void testBind() throws Exception {
        assertThat(bind(items("a", "b", "c", "d"), items(1, 2).loop()).toList()).contains(
                Pair.of("a", 1),
                Pair.of("b", 2),
                Pair.of("c", 1),
                Pair.of("d", 2)
        );
    }

    @Test
    public void testBuilder() throws Exception {
        DataGenerator<String> values = DataGenerator.bindBuilder(SampleBuilder::new)
                .bind(items("a", "b", "a", "b"), SampleBuilder::withA)
                .bind(items("x", "y", "z").loop(), SampleBuilder::withB)
                .bind(items("1", "2", "3", "4").loop(), SampleBuilder::withC)
                .map(SampleBuilder::build);

        List<String> all = values.toList();
        assertThat(all).hasSize(4);
        assertThat(new HashSet<>(all)).hasSize(2);
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