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

import java.util.concurrent.TimeUnit;

public abstract class JobExecutionStep {

    private static final JobExecutionStep TERMINATE_STEP = new TerminateStep();
    private static final JobExecutionStep FIND_OWN_JOB_STEP = new FindOwnJobStep();
    private static final JobExecutionStep FIND_OWN_TASKS_STEP = new FindOwnTasksStep();
    private static final JobExecutionStep KILL_RANDOM_TASK_STEP = new KillRandomTaskStep();
    private static final JobExecutionStep EVICT_RANDOM_TASK_STEP = new EvictRandomTaskStep();
    private static final JobExecutionStep TERMINATE_AND_SHRINK_RANDOM_TASK_STEP = new TerminateAndShrinkRandomTaskStep();
    private static final JobExecutionStep AWAIT_COMPLETION_STEP = new AwaitCompletionStep();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{}";
    }

    public static class LoopStep extends JobExecutionStep {
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

    public static class TerminateStep extends JobExecutionStep {
    }

    public static class FindOwnJobStep extends JobExecutionStep {
    }

    public static class FindOwnTasksStep extends JobExecutionStep {
    }

    public static class KillRandomTaskStep extends JobExecutionStep {
    }

    public static class EvictRandomTaskStep extends JobExecutionStep {
    }

    public static class TerminateAndShrinkRandomTaskStep extends JobExecutionStep {
    }

    public static class AwaitCompletionStep extends JobExecutionStep {
    }

    public static class ScaleUpStep extends JobExecutionStep {
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

    public static class ScaleDownStep extends JobExecutionStep {
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

    public static class DelayStep extends JobExecutionStep {
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

    public static JobExecutionStep terminate() {
        return TERMINATE_STEP;
    }

    public static LoopStep loop(int position, int times) {
        return new LoopStep(position, times);
    }

    public static JobExecutionStep findOwnJob() {
        return FIND_OWN_JOB_STEP;
    }

    public static JobExecutionStep findOwnTasks() {
        return FIND_OWN_TASKS_STEP;
    }

    public static JobExecutionStep killRandomTask() {
        return KILL_RANDOM_TASK_STEP;
    }

    public static JobExecutionStep evictRandomTask() {
        return EVICT_RANDOM_TASK_STEP;
    }

    public static JobExecutionStep terminateAndShrinkRandomTask() {
        return TERMINATE_AND_SHRINK_RANDOM_TASK_STEP;
    }

    public static JobExecutionStep awaitCompletion() {
        return AWAIT_COMPLETION_STEP;
    }

    public static JobExecutionStep scaleUp(int delta) {
        return new ScaleUpStep(delta);
    }

    public static JobExecutionStep scaleDown(int delta) {
        return new ScaleDownStep(delta);
    }

    public static DelayStep delayStep(long delay, TimeUnit timeUnit) {
        return new DelayStep(timeUnit.toMillis(delay));
    }
}
