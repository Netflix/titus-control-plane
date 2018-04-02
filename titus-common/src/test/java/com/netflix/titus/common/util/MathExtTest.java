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

package com.netflix.titus.common.util;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class MathExtTest {

    @Test
    public void testScaleBetween0And1() {
        double scaledValue = MathExt.scale(10.0, 0.0, 100.0);
        Assertions.assertThat(scaledValue).isEqualTo(0.1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testScaleMinAndMaxValidation() {
        MathExt.scale(10.0, 10.0, 10.0);
    }

    @Test
    public void testScaleBetween1and10() {
        double scaledValue = MathExt.scale(10.0, 0.0, 100.0, 0.0, 10.0);
        Assertions.assertThat(scaledValue).isEqualTo(1.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testScaleRangeValueValidation() {
        MathExt.scale(10.0, 9.0, 10.0, 10.0, 9.0);
    }

    @Test
    public void testBetween0And1() {
        Assertions.assertThat(MathExt.between(1.1, 0.0, 1.0)).isEqualTo(1.0);
        Assertions.assertThat(MathExt.between(1.0, 2.0, 10.0)).isEqualTo(2.0);
        Assertions.assertThat(MathExt.between(0.5, 0.0, 1.0)).isEqualTo(0.5);
    }

    @Test
    public void testBetween0And1Long() {
        Assertions.assertThat(MathExt.between(0, 5, 10)).isEqualTo(5);
        Assertions.assertThat(MathExt.between(1, 2, 10)).isEqualTo(2);
        Assertions.assertThat(MathExt.between(5, 0, 10)).isEqualTo(5);
    }
}