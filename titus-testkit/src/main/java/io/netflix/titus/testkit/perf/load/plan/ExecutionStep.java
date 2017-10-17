/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.perf.load.plan;

import java.util.concurrent.TimeUnit;

public abstract class ExecutionStep {

    private static final ExecutionStep SUBMIT_STEP = new SubmitStep();
    private static final ExecutionStep TERMINATE_STEP = new TerminateStep();
    private static final ExecutionStep KILL_RANDOM_TASK_STEP = new KillRandomTaskStep();
    private static final ExecutionStep TERMINATE_AND_SHRINK_RANDOM_TASK_STEP = new TerminateAndShrinkRandomTaskStep();
    private static final ExecutionStep AWAIT_COMPLETION_STEP = new AwaitCompletionStep();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{}";
    }

    public static class LoopStep extends ExecutionStep {
        private final int position;
        private final int times;

        public LoopStep(int position, int times) {
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

            if (position != loopStep.position) {
                return false;
            }
            return times == loopStep.times;
        }

        @Override
        public int hashCode() {
            int result = position;
            result = 31 * result + times;
            return result;
        }

        @Override
        public String toString() {
            return "LoopStep{" +
                    "position=" + position +
                    ", times=" + times +
                    '}';
        }
    }

    public static class SubmitStep extends ExecutionStep {
    }

    public static class TerminateStep extends ExecutionStep {
    }

    public static class KillRandomTaskStep extends ExecutionStep {
    }

    public static class TerminateAndShrinkRandomTaskStep extends ExecutionStep {
    }

    public static class AwaitCompletionStep extends ExecutionStep {
    }

    public static class ScaleUpStep extends ExecutionStep {
        private final int delta;

        public ScaleUpStep(int delta) {
            this.delta = delta;
        }

        public int getDelta() {
            return delta;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ScaleUpStep that = (ScaleUpStep) o;

            return delta == that.delta;
        }

        @Override
        public int hashCode() {
            return delta;
        }

        @Override
        public String toString() {
            return "ScaleUpStep{" +
                    "delta=" + delta +
                    '}';
        }
    }

    public static class ScaleDownStep extends ExecutionStep {
        private final int delta;

        public ScaleDownStep(int delta) {
            this.delta = delta;
        }

        public int getDelta() {
            return delta;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ScaleDownStep that = (ScaleDownStep) o;

            return delta == that.delta;
        }

        @Override
        public int hashCode() {
            return delta;
        }

        @Override
        public String toString() {
            return "ScaleDownStep{" +
                    "delta=" + delta +
                    '}';
        }
    }

    public static class DelayStep extends ExecutionStep {
        private final long delayMs;

        public DelayStep(long delayMs) {
            this.delayMs = delayMs;
        }

        public long getDelayMs() {
            return delayMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DelayStep delayStep = (DelayStep) o;

            return delayMs == delayStep.delayMs;
        }

        @Override
        public int hashCode() {
            return (int) (delayMs ^ (delayMs >>> 32));
        }

        @Override
        public String toString() {
            return "DelayStep{" +
                    "delayMs=" + delayMs +
                    '}';
        }
    }

    public static ExecutionStep submit() {
        return SUBMIT_STEP;
    }

    public static ExecutionStep terminate() {
        return TERMINATE_STEP;
    }

    public static LoopStep loop(int position, int times) {
        return new LoopStep(position, times);
    }

    public static ExecutionStep killRandomTask() {
        return KILL_RANDOM_TASK_STEP;
    }

    public static ExecutionStep terminateAndShrinkRandomTask() {
        return TERMINATE_AND_SHRINK_RANDOM_TASK_STEP;
    }

    public static ExecutionStep awaitCompletion() {
        return AWAIT_COMPLETION_STEP;
    }

    public static ExecutionStep scaleUp(int delta) {
        return new ScaleUpStep(delta);
    }

    public static ExecutionStep scaleDown(int delta) {
        return new ScaleDownStep(delta);
    }

    public static DelayStep delayStep(long delay, TimeUnit timeUnit) {
        return new DelayStep(timeUnit.toMillis(delay));
    }
}
