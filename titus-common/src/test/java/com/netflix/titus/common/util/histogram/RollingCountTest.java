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

package com.netflix.titus.common.util.histogram;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RollingCountTest {

    private static final long START_TIME = 1_000;
    private static final int STEPS = 4;
    private static final int STEP_TIME = 25;

    private final RollingCount rollingCount = RollingCount.rollingCount(STEP_TIME, STEPS, START_TIME);

    @Test
    public void testBasicProgression() {
        assertThat(rollingCount.getCounts(START_TIME)).isEqualTo(0);

        // Adds within the current window
        assertThat(rollingCount.addOne(START_TIME + 0 * STEP_TIME)).isEqualTo(1);
        assertThat(rollingCount.addOne(START_TIME + 1 * STEP_TIME)).isEqualTo(2);
        assertThat(rollingCount.addOne(START_TIME + 2 * STEP_TIME)).isEqualTo(3);
        assertThat(rollingCount.addOne(START_TIME + 3 * STEP_TIME)).isEqualTo(4);

        // Now passed the window
        assertThat(rollingCount.addOne(START_TIME + 4 * STEP_TIME)).isEqualTo(4);
        assertThat(rollingCount.addOne(START_TIME + 5 * STEP_TIME)).isEqualTo(4);
        assertThat(rollingCount.addOne(START_TIME + 7 * STEP_TIME)).isEqualTo(3);

        // Now get long into the future
        assertThat(rollingCount.getCounts(START_TIME + 1000 * STEP_TIME)).isEqualTo(0);
    }
}