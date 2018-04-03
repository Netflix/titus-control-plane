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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.tuple.Either;

/**
 * Ordered sequence of integers. The sequence is parsed from string which could include individual values, a range or
 * mixture of both. For example:
 * <ul>
 * <li>1, 2, 5</li>
 * <li>1..5</li>
 * <li>1..</li>
 * <li>1, 2, 5..10, 20, 50..</li>
 * </ul>
 */
public abstract class NumberSequence {

    private static final Pattern PART_RE = Pattern.compile("(\\d+)(..(\\d+)?)?");

    private static final NumberSequence EMPTY_SEQUENCE = new EmptySequence();

    public abstract long getCount();

    public abstract Iterator<Long> getIterator();

    public abstract boolean matches(long number);

    public abstract NumberSequence skip(long skipCount);

    public abstract NumberSequence step(long step);

    public static NumberSequence from(long from) {
        return new RangeSequence(from, Long.MAX_VALUE, 1);
    }

    public static Either<NumberSequence, String> parse(String value) {
        List<String> parts = StringExt.splitByComma(value);
        List<NumberSequence> subsequences = new ArrayList<>();
        for (String part : parts) {
            Matcher matcher = PART_RE.matcher(part);
            if (!matcher.matches()) {
                return Either.ofError(String.format("Syntax error in sequence fragment '%s'", part));
            }
            if (matcher.group(2) == null) {
                subsequences.add(new ValuesSequence(Integer.parseInt(part)));
            } else if (matcher.group(3) == null) {
                subsequences.add(new RangeSequence(Long.parseLong(matcher.group(1)), Long.MAX_VALUE, 1));
            } else {
                long from = Long.parseLong(matcher.group(1));
                long to = Long.parseLong(matcher.group(3));
                if (from >= to) {
                    return Either.ofError(String.format("Invalid range: %s (from) >= %s (to)", from, to));
                }
                subsequences.add(new RangeSequence(from, to, 1));
            }
        }
        return Either.ofValue(subsequences.isEmpty() ? EMPTY_SEQUENCE : new CompositeSequence(subsequences));
    }

    private static class EmptySequence extends NumberSequence {

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public Iterator<Long> getIterator() {
            return new Iterator<Long>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Long next() {
                    throw new IllegalStateException("End of sequence");
                }
            };
        }

        @Override
        public boolean matches(long number) {
            return false;
        }

        @Override
        public NumberSequence skip(long skipCount) {
            return this;
        }

        @Override
        public NumberSequence step(long step) {
            return this;
        }

        @Override
        public String toString() {
            return "EmptySequence{}";
        }
    }

    private static class ValuesSequence extends NumberSequence {

        private final List<Long> numbers;

        private ValuesSequence(long number) {
            this.numbers = Collections.singletonList(number);
        }

        @Override
        public long getCount() {
            return 1;
        }

        @Override
        public Iterator<Long> getIterator() {
            return numbers.iterator();
        }

        @Override
        public boolean matches(long number) {
            return numbers.get(0) == number;
        }

        @Override
        public NumberSequence skip(long skipCount) {
            return skipCount == 0 ? this : EMPTY_SEQUENCE;
        }

        @Override
        public NumberSequence step(long step) {
            return this;
        }

        @Override
        public String toString() {
            return "ValuesSequence{numbers=" + numbers.get(0) + "}";
        }
    }

    private static class RangeSequence extends NumberSequence {
        private final long from;
        private final long to;
        private final long step;
        private final long count;

        private RangeSequence(long from, long to, long step) {
            this.from = from;
            this.to = to;
            this.step = step;
            this.count = 1 + (to - from - 1) / step;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public Iterator<Long> getIterator() {
            return new Iterator<Long>() {

                private long next = from;

                @Override
                public boolean hasNext() {
                    return next < to;
                }

                @Override
                public Long next() {
                    Preconditions.checkState(hasNext(), "End of sequence");
                    long result = next;
                    next = next + step;
                    return result;
                }
            };
        }

        @Override
        public boolean matches(long number) {
            if (number < from || number >= to) {
                return false;
            }
            return (number - from) % step == 0;
        }

        @Override
        public NumberSequence skip(long skipCount) {
            if (count <= skipCount) {
                return EMPTY_SEQUENCE;
            }
            return new RangeSequence(from + skipCount * step, to, step);
        }

        @Override
        public NumberSequence step(long step) {
            Preconditions.checkArgument(step > 0, "Step value cannot be <= 0");
            if (step == 1) {
                return this;
            }
            return new RangeSequence(from, to, this.step * step);
        }

        @Override
        public String toString() {
            return "RangeSequence{" +
                    "from=" + from +
                    ", to=" + to +
                    ", step=" + step +
                    "}";
        }
    }

    private static class CompositeSequence extends NumberSequence {

        private final List<NumberSequence> subsequences;
        private final long count;

        private CompositeSequence(List<NumberSequence> subsequences) {
            this.subsequences = subsequences;
            this.count = subsequences.stream().mapToLong(NumberSequence::getCount).sum();
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public Iterator<Long> getIterator() {
            return new Iterator<Long>() {

                private int subsequenceIndex = 0;
                private Iterator<Long> currentIterator = subsequences.get(0).getIterator();

                @Override
                public boolean hasNext() {
                    if (subsequenceIndex >= subsequences.size()) {
                        return false;
                    }
                    if (currentIterator.hasNext()) {
                        return true;
                    }
                    subsequenceIndex++;
                    if (subsequenceIndex >= subsequences.size()) {
                        return false;
                    }
                    currentIterator = subsequences.get(subsequenceIndex).getIterator();
                    return true; // We know it is true, as we hold only non-empty sequences
                }

                @Override
                public Long next() {
                    Preconditions.checkState(hasNext(), "End of sequence");
                    return currentIterator.next();
                }
            };
        }

        @Override
        public boolean matches(long number) {
            return subsequences.stream().anyMatch(s -> s.matches(number));
        }

        @Override
        public NumberSequence skip(long skipCount) {
            List<NumberSequence> newSubsequences = new ArrayList<>();
            long left = skipCount;
            int pos = 0;
            while (pos < subsequences.size() && subsequences.get(pos).getCount() <= left) {
                left -= subsequences.get(pos).getCount();
                pos++;
            }
            if (pos == subsequences.size()) {
                return EMPTY_SEQUENCE;
            }
            newSubsequences.add(subsequences.get(pos).skip(left));
            for (pos++; pos < subsequences.size(); pos++) {
                newSubsequences.add(subsequences.get(pos));
            }
            return new CompositeSequence(newSubsequences);
        }

        @Override
        public NumberSequence step(long step) {
            List<NumberSequence> newSubsequences = new ArrayList<>();
            long skip = 0;
            for (NumberSequence nextSeq : subsequences) {
                long count = nextSeq.getCount();
                if (skip < count) {
                    newSubsequences.add(nextSeq.skip(skip).step(step));
                    skip = (int) ((count - skip) % step);
                } else {
                    skip -= count;
                }
            }
            if (newSubsequences.isEmpty()) {
                return EMPTY_SEQUENCE;
            }
            if (newSubsequences.size() == 1) {
                return newSubsequences.get(0);
            }
            return new CompositeSequence(newSubsequences);
        }

        @Override
        public String toString() {
            return "CompositeSequence{subsequences=" + subsequences + "}";
        }
    }
}
