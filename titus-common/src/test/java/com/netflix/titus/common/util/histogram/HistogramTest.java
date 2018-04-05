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

public class HistogramTest {

    @Test
    public void testCorrectness() throws Exception {
        Histogram.Builder builder = Histogram.newBuilder(HistogramDescriptor.histogramOf(2, 5, 30, 60));
        for (int i = 0; i <= 100; i++) {
            builder.increment(i);
        }
        Histogram histogram = builder.build();
        assertThat(histogram.getCounters()).containsExactlyInAnyOrder(3L, 3L, 25L, 30L, 40L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBounds() throws Exception {
        HistogramDescriptor.histogramOf(1, 2, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoBounds() throws Exception {
        HistogramDescriptor.histogramOf();
    }
}