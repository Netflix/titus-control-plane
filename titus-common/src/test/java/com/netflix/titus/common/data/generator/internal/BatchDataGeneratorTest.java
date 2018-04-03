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

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class BatchDataGeneratorTest {

    @Test
    public void testBatches() throws Exception {
        DataGenerator<List<Long>> batchGen = DataGenerator.range(0, 10).batch(2);
        Pair<DataGenerator<List<Long>>, List<List<Long>>> result = take(batchGen, 5);

        DataGenerator<List<Long>> lastGen = result.getLeft();
        List<List<Long>> values = result.getRight();

        assertThat(lastGen.isClosed()).isTrue();
        assertThat(values).contains(asList(0L, 1L), asList(2L, 3L), asList(4L, 5L), asList(6L, 7L), asList(8L, 9L));
    }

    private <T> Pair<DataGenerator<T>, List<T>> take(DataGenerator<T> generator, int count) {
        List<T> result = new ArrayList<>();
        DataGenerator<T> nextG = generator;
        for (int i = 0; i < count; i++) {
            result.add(nextG.getValue());
            nextG = nextG.apply();
        }
        return Pair.of(nextG, result);
    }
}