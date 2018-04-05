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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

public class HistogramDescriptor {

    private final long[] valueBounds;
    private final List<Long> valueBoundList;

    private HistogramDescriptor(long[] valueBounds) {
        Preconditions.checkArgument(valueBounds.length > 0, "Expecting at least one element in the value bounds array");
        Preconditions.checkArgument(isAscending(valueBounds), "Expected increasing sequence of numbers: %s", valueBounds);
        this.valueBounds = valueBounds;
        this.valueBoundList = Longs.asList(valueBounds);
    }

    public List<Long> getValueBounds() {
        return valueBoundList;
    }

    long[] newCounters() {
        return new long[valueBounds.length + 1];
    }

    int positionOf(long value) {
        int insertionPoint = Arrays.binarySearch(valueBounds, value);
        if (insertionPoint >= 0) {
            return insertionPoint;
        }
        int position = -(insertionPoint + 1);
        return position;
    }

    private boolean isAscending(long[] valueBounds) {
        long previous = valueBounds[0];
        for (int i = 1; i < valueBounds.length; i++) {
            if (previous >= valueBounds[i]) {
                return false;
            }
            previous = valueBounds[i];
        }
        return true;
    }

    public static HistogramDescriptor histogramOf(long... valueBounds) {
        return new HistogramDescriptor(valueBounds);
    }
}
