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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * Given collection of ranges (0..n1), (0..n2),...,(0..nk) of different sizes, generates a sequence
 * of unique tuples, containing one element from the same range. The ranges can be re-sized, to increase number
 * of available elements.
 */
class Combinations {

    private static final int[][] EMPTY_TUPLES = new int[0][];

    private final List<Integer> seqSizes;
    private final int[][] tuples;

    private Combinations(int tupleSize) {
        this.seqSizes = createListFilledWithZeros(tupleSize);
        this.tuples = EMPTY_TUPLES;
    }

    private Combinations(Combinations previous, List<Integer> newSeqSizes) {
        this.seqSizes = newSeqSizes;
        int total = seqSizes.stream().reduce(1, (acc, s) -> acc * s);

        int[] currentTuple = new int[seqSizes.size()];
        int[][] output = new int[total][];
        copy(previous.tuples, output);
        fill(currentTuple, previous.seqSizes, newSeqSizes, 0, output, previous.tuples.length, false);

        this.tuples = output;
    }

    public int getSize() {
        return tuples.length;
    }

    public int[] combinationAt(int pos) {
        return tuples[pos];
    }

    public Combinations resize(List<Integer> newSeqSizes) {
        Preconditions.checkArgument(newSeqSizes.size() == seqSizes.size());
        for (int i = 0; i < newSeqSizes.size(); i++) {
            Preconditions.checkArgument(newSeqSizes.get(i) >= seqSizes.get(i));
        }
        return new Combinations(this, newSeqSizes);
    }

    public static Combinations newInstance(int tupleSize) {
        return new Combinations(tupleSize);
    }

    private List<Integer> createListFilledWithZeros(int size) {
        List<Integer> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(0);
        }
        return result;
    }

    private static void copyTuple(int[] source, int[][] output, int outputPos) {
        output[outputPos] = Arrays.copyOf(source, source.length);
    }

    private static void copy(int[][] source, int[][] output) {
        System.arraycopy(source, 0, output, 0, source.length);
    }

    private static int fill(int[] currentTuple,
                            List<Integer> oldSeqSizes,
                            List<Integer> newSeqSizes,
                            int seqIdx,
                            int[][] output,
                            int outputPos,
                            boolean hasNewItem) {
        int currentSeqSize = newSeqSizes.get(seqIdx);
        int lastSeqIdx = newSeqSizes.size() - 1;
        int startFrom = oldSeqSizes.get(seqIdx);
        if (seqIdx == lastSeqIdx) {
            for (int i = hasNewItem ? 0 : startFrom; i < currentSeqSize; i++, outputPos++) {
                currentTuple[seqIdx] = i;
                copyTuple(currentTuple, output, outputPos);
            }
        } else {
            if (!hasNewItem) {
                for (int i = 0; i < startFrom; i++) {
                    currentTuple[seqIdx] = i;
                    outputPos = fill(currentTuple, oldSeqSizes, newSeqSizes, seqIdx + 1, output, outputPos, false);
                }
            }
            for (int i = hasNewItem ? 0 : startFrom; i < currentSeqSize; i++) {
                currentTuple[seqIdx] = i;
                outputPos = fill(currentTuple, oldSeqSizes, newSeqSizes, seqIdx + 1, output, outputPos, true);
            }
        }
        return outputPos;
    }
}
