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

import java.util.List;

import com.google.common.primitives.Longs;

public class Histogram {

    private final List<Long> counters;
    private final HistogramDescriptor histogramDescriptor;

    private Histogram(List<Long> counters, HistogramDescriptor histogramDescriptor) {
        this.counters = counters;
        this.histogramDescriptor = histogramDescriptor;
    }

    public List<Long> getCounters() {
        return counters;
    }

    public HistogramDescriptor getHistogramDescriptor() {
        return histogramDescriptor;
    }

    public static Builder newBuilder(HistogramDescriptor histogramDescriptor) {
        return new Builder(histogramDescriptor);
    }

    public static class Builder {

        private final HistogramDescriptor histogramDescriptor;
        private final long[] counters;

        private Builder(HistogramDescriptor histogramDescriptor) {
            this.histogramDescriptor = histogramDescriptor;
            this.counters = histogramDescriptor.newCounters();
        }

        public Builder increment(long value) {
            return add(value, 1);
        }

        public Builder add(long value, long count) {
            int position = histogramDescriptor.positionOf(value);
            counters[position] = counters[position] + count;
            return this;
        }

        public Histogram build() {
            return new Histogram(Longs.asList(counters), histogramDescriptor);
        }
    }
}
