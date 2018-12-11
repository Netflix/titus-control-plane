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

package com.netflix.titus.testkit.perf.load.plan;

import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

public class ExecutionStep {

    public static final String NAME_LOOP = "loop";
    public static final String NAME_DELAY = "delay";

    private final String name;

    public ExecutionStep(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecutionStep that = (ExecutionStep) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "ExecutionStep{" +
                "name='" + name + '\'' +
                '}';
    }

    public static ExecutionStep named(String name) {
        return new ExecutionStep(name);
    }

    public static LoopStep loop(int position, int times) {
        return new LoopStep(position, times);
    }

    public static DelayStep delay(long delay, TimeUnit timeUnit) {
        return new DelayStep(timeUnit.toMillis(delay));
    }

    public static DelayStep delay(Duration delay) {
        return new DelayStep(delay.toMillis());
    }

    public static ExecutionStep randomDelayStep(Duration lowerBound, Duration upperBound) {
        return new DelayStep(lowerBound, upperBound);
    }

    public static class LoopStep extends ExecutionStep {
        private final int position;
        private final int times;

        public LoopStep(int position, int times) {
            super(NAME_LOOP);
            this.position = position;
            this.times = times;
        }

        public int getPosition() {
            return position;
        }

        public int getTimes() {
            return times;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LoopStep loopStep = (LoopStep) o;
            return position == loopStep.position &&
                    times == loopStep.times;
        }

        @Override
        public int hashCode() {
            return Objects.hash(position, times);
        }

        @Override
        public String toString() {
            return "LoopStep{" +
                    "position=" + position +
                    ", times=" + times +
                    '}';
        }
    }

    public static class DelayStep extends ExecutionStep {

        private final long lowerBoundMs;
        private final int rangeMs;
        private final Random random = new Random();

        public DelayStep(long delayMs) {
            super(NAME_DELAY);
            this.lowerBoundMs = delayMs;
            this.rangeMs = 0;
        }

        public DelayStep(Duration lowerBound, Duration upperBound) {
            super(NAME_DELAY);
            Preconditions.checkArgument(lowerBound.toMillis() <= upperBound.toMillis());

            this.lowerBoundMs = lowerBound.toMillis();
            this.rangeMs = (int) (upperBound.toMillis() - lowerBoundMs);
        }

        public long getDelayMs() {
            return rangeMs == 0
                    ? lowerBoundMs
                    : lowerBoundMs + random.nextInt(rangeMs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            DelayStep delayStep = (DelayStep) o;
            return lowerBoundMs == delayStep.lowerBoundMs &&
                    rangeMs == delayStep.rangeMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), lowerBoundMs, rangeMs);
        }

        @Override
        public String toString() {
            return "DelayStep{" +
                    "lowerBoundMs=" + lowerBoundMs +
                    ", rangeMs=" + rangeMs +
                    '}';
        }
    }
}
