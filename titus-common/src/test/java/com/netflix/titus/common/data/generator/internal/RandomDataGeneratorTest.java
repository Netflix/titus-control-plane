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

import java.util.List;

import org.junit.Test;

import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static org.assertj.core.api.Assertions.assertThat;

public class RandomDataGeneratorTest {

    @Test
    public void testRandom() throws Exception {
        List<Long> allItems = range(0, 20).random(0.5, 10).toList();

        assertThat(allItems.subList(0, 5)).isSubsetOf(range(0, 10).toList());
        assertThat(allItems).containsAll(range(0, 20).toList());
    }
}