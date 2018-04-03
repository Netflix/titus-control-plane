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

import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static com.netflix.titus.common.data.generator.DataGenerator.items;
import static com.netflix.titus.common.data.generator.DataGenerator.zip;
import static org.assertj.core.api.Assertions.assertThat;

public class ZipDataGeneratorTest {
    @Test
    public void testZip() throws Exception {
        assertThat(zip(items("a", "b"), items(1, 2, 3)).toList()).contains(Pair.of("a", 1), Pair.of("b", 2));
    }
}