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

package io.netflix.titus.common.util;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphExtTest {

    @Test
    public void testOrdering() throws Exception {
        // Already in order
        List<Character> ordered = GraphExt.order(Arrays.asList('A', 'B', 'C'), (s1, s2) -> s1 == 'A' && s2 == 'C');
        assertThat(ordered).containsExactlyInAnyOrder('A', 'B', 'C');

        // Out of order
        List<Character> notOrdered = GraphExt.order(Arrays.asList('A', 'B', 'C'), (s1, s2) -> s1 == 'C' && s2 == 'A');
        assertThat(notOrdered).containsExactlyInAnyOrder('B', 'C', 'A');
    }
}