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

import io.netflix.titus.common.data.generator.DataGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterDataGeneratorTest {

    @Test
    public void testFilter() throws Exception {
        DataGenerator<List<Long>> filteredBatch = DataGenerator.range(0, 9).filter(v -> v % 2 == 0).batch(5);

        assertThat(filteredBatch.getValue()).contains(0L, 2L, 4L, 6L, 8L);
        assertThat(filteredBatch.isClosed()).isFalse();
        assertThat(filteredBatch.apply().isClosed()).isTrue();
    }
}