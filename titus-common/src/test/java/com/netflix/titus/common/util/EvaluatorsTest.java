/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.util;

import java.util.function.Function;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EvaluatorsTest {

    private static final RuntimeException SIMULATED_ERROR = new RuntimeException("simulated error");

    private int counter;

    @Test
    public void testMemoizeLast() {
        Function<String, String> evaluator = Evaluators.memoizeLast(s -> {
            counter++;
            return ("" + s).toUpperCase();
        });

        assertThat(evaluator.apply("a")).isEqualTo("A");
        assertThat(evaluator.apply("a")).isEqualTo("A");
        assertThat(counter).isEqualTo(1);

        assertThat(evaluator.apply(null)).isEqualTo("NULL");
        assertThat(evaluator.apply(null)).isEqualTo("NULL");
        assertThat(counter).isEqualTo(2);

        assertThat(evaluator.apply("b")).isEqualTo("B");
        assertThat(evaluator.apply("b")).isEqualTo("B");
        assertThat(counter).isEqualTo(3);
    }

    @Test
    public void testMemoizeLastError() {
        Function<String, String> evaluator = Evaluators.memoizeLast(s -> {
            counter++;
            if (s.equals("error")) {
                throw SIMULATED_ERROR;
            }
            return s.toUpperCase();
        });

        applyWithErrorResult(evaluator);
        applyWithErrorResult(evaluator);
        assertThat(counter).isEqualTo(1);

        assertThat(evaluator.apply("a")).isEqualTo("A");
        assertThat(counter).isEqualTo(2);
    }

    private void applyWithErrorResult(Function<String, String> evaluator) {
        try {
            evaluator.apply("error");
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("simulated error");
        }
    }
}