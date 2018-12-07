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

public abstract class JobExecutionStep extends ExecutionStep {

    public static final String NAME_TERMINATE = "terminate";
    public static final String NAME_SCALE_UP = "scaleUp";
    public static final String NAME_SCALE_DOWN = "scaleDown";
    public static final String NAME_FIND_OWN_JOB = "findOwnJob";
    public static final String NAME_FIND_OWN_TASK = "findOwnTask";
    public static final String NAME_KILL_RANDOM_TASK = "killRandomTask";
    public static final String NAME_EVICT_RANDOM_TASK = "evictRandomTask";
    public static final String NAME_TERMINATE_AND_SHRINK_RANDOM_TASK = "terminateAndShrinkRandomTask";
    public static final String NAME_AWAIT_COMPLETION = "awaitCompletion";

    private static final ExecutionStep STEP_TERMINATE = new ExecutionStep(NAME_TERMINATE);
    private static final ExecutionStep STEP_FIND_OWN_JOB = new ExecutionStep(NAME_FIND_OWN_JOB);
    private static final ExecutionStep STEP_FIND_OWN_TASKS = new ExecutionStep(NAME_FIND_OWN_TASK);
    private static final ExecutionStep STEP_KILL_RANDOM_TASK = new ExecutionStep(NAME_KILL_RANDOM_TASK);
    private static final ExecutionStep STEP_EVICT_RANDOM_TASK = new ExecutionStep(NAME_EVICT_RANDOM_TASK);
    private static final ExecutionStep STEP_TERMINATE_AND_SHRINK_RANDOM_TASK = new ExecutionStep(NAME_TERMINATE_AND_SHRINK_RANDOM_TASK);
    private static final ExecutionStep STEP_AWAIT_COMPLETION = new ExecutionStep(NAME_AWAIT_COMPLETION);

    protected JobExecutionStep(String name) {
        super(name);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{}";
    }

    public static class ScaleUpStep extends JobExecutionStep {
        private final int delta;

        public ScaleUpStep(int delta) {
            super(NAME_SCALE_UP);
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
            super(NAME_SCALE_DOWN);
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

    public static ExecutionStep terminate() {
        return STEP_TERMINATE;
    }

    public static ExecutionStep findOwnJob() {
        return STEP_FIND_OWN_JOB;
    }

    public static ExecutionStep findOwnTasks() {
        return STEP_FIND_OWN_TASKS;
    }

    public static ExecutionStep killRandomTask() {
        return STEP_KILL_RANDOM_TASK;
    }

    public static ExecutionStep evictRandomTask() {
        return STEP_EVICT_RANDOM_TASK;
    }

    public static ExecutionStep terminateAndShrinkRandomTask() {
        return STEP_TERMINATE_AND_SHRINK_RANDOM_TASK;
    }

    public static ExecutionStep awaitCompletion() {
        return STEP_AWAIT_COMPLETION;
    }

    public static JobExecutionStep scaleUp(int delta) {
        return new ScaleUpStep(delta);
    }

    public static JobExecutionStep scaleDown(int delta) {
        return new ScaleDownStep(delta);
    }
}
