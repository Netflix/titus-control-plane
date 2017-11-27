package io.netflix.titus.common.util.histogram;

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
