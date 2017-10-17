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

import org.junit.Test;

import static io.netflix.titus.common.data.generator.DataGenerator.items;
import static org.assertj.core.api.Assertions.assertThat;

public class LoopedDataGeneratorTest {

    @Test
    public void testLoop() throws Exception {
        assertThat(items("a", "b").loop().limit(4).toList()).containsExactly("a", "b", "a", "b");
    }

    @Test
    public void testLoopWithIndex() throws Exception {
        assertThat(items("a", "b").loop((v, idx) -> v + idx).limit(4).toList()).containsExactly("a0", "b0", "a1", "b1");
    }
}