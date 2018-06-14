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

package com.netflix.titus.common.util.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MemoizedFunctionTest {

    @Test
    public void cachesLastResultForTheSameInput() {
        AtomicInteger counter = new AtomicInteger(0);
        Function<String, Integer> timesCalled = new MemoizedFunction<>((input, lastResult) -> counter.incrementAndGet());
        for (int i=0; i < 5; i++) {
            assertThat(timesCalled.apply("foo")).isEqualTo(1);
        }
        for (int i=0; i < 5; i++) {
            assertThat(timesCalled.apply("bar")).isEqualTo(2);
        }

        // no caching if inputs are different
        assertThat(timesCalled.apply("foo")).isEqualTo(3);
        assertThat(timesCalled.apply("bar")).isEqualTo(4);
    }

}