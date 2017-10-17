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

import java.util.ArrayList;
import java.util.List;

import io.netflix.titus.common.data.generator.DataGenerator;

public class DataGeneratorOperators {

    private static final int TO_LIST_LIMIT = 10000;

    public static <A> List<A> toList(DataGenerator<A> source) {
        List<A> result = new ArrayList<>();
        DataGenerator<A> nextGen = source;
        while (!nextGen.isClosed()) {
            result.add(nextGen.getValue());
            if (result.size() > TO_LIST_LIMIT) {
                throw new IllegalStateException("Too many items");
            }
            nextGen = nextGen.apply();
        }
        return result;
    }
}
