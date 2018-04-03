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

import static com.netflix.titus.common.data.generator.DataGenerator.items;
import static com.netflix.titus.common.data.generator.DataGenerator.merge;
import static org.assertj.core.api.Assertions.assertThat;

public class MergeDataGeneratorTest {

    @Test
    public void testMerge() throws Exception {
        List<String> items = merge(items("a", "b"), items("c", "d")).toList();
        assertThat(items).contains("a", "b", "c", "d");
    }
}