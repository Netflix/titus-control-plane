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

import java.util.Arrays;
import java.util.Random;

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;

public class SeqNumberDataGenerators {
    public static DataGenerator<int[]> sequenceOfAscendingIntegers(int size, int lowerBound, int upperBound) {
        final int range = upperBound - lowerBound;
        return DataGenerator.fromContext(new Random(), random -> {
            int[] result = new int[size];
            for (int i = 0; i < size; i++) {
                result[i] = lowerBound + random.nextInt(range);
            }
            Arrays.sort(result);
            return Pair.of(random, result);
        });
    }
}
