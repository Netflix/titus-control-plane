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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

public class ExecutionPlan {

    private final List<ExecutionStep> steps;

    public ExecutionPlan(List<ExecutionStep> steps) {
        this.steps = steps;
    }

    public Iterator<ExecutionStep> newInstance() {
        return new PlanIterator();
    }

    public static ExecutionPlanBuilder newBuilder() {
        return new ExecutionPlanBuilder();
    }

    public static class ExecutionPlanBuilder {

        private final List<ExecutionStep> steps = new ArrayList<>();
        private final Map<String, Integer> labelPos = new HashMap<>();

        public ExecutionPlanBuilder() {
            steps.add(ExecutionStep.submit());
        }

        public ExecutionPlanBuilder label(String name) {
            labelPos.put(name, steps.size());
            return this;
        }

        public ExecutionPlanBuilder killRandomTask() {
            steps.add(ExecutionStep.killRandomTask());
            return this;
        }

        public ExecutionPlanBuilder terminateAndShrinkRandomTask() {
            steps.add(ExecutionStep.terminateAndShrinkRandomTask());
            return this;
        }

        public ExecutionPlanBuilder scaleUp(int delta) {
            steps.add(ExecutionStep.scaleUp(delta));
            return this;
        }

        public ExecutionPlanBuilder scaleDown(int delta) {
            steps.add(ExecutionStep.scaleDown(delta));
            return this;
        }

        public ExecutionPlanBuilder loop(String label) {
            return loop(label, -1);
        }

        public ExecutionPlanBuilder loop(String label, int times) {
            Preconditions.checkArgument(labelPos.containsKey(label), "Execution plan has no label " + label);
            steps.add(ExecutionStep.loop(labelPos.get(label), times));
            return this;
        }

        public ExecutionPlanBuilder delay(long duration, TimeUnit timeUnit) {
            steps.add(ExecutionStep.delayStep(duration, timeUnit));
            return this;
        }

        public ExecutionPlanBuilder awaitCompletion() {
            steps.add(ExecutionStep.awaitCompletion());
            return this;
        }

        public ExecutionPlanBuilder terminate() {
            steps.add(ExecutionStep.terminate());
            return this;
        }

        public ExecutionPlan build() {
            if (steps.get(steps.size() - 1) != ExecutionStep.terminate()) {
                steps.add(ExecutionStep.terminate());
            }
            return new ExecutionPlan(steps);
        }
    }

    private class PlanIterator implements Iterator<ExecutionStep> {

        private final int[] counters;
        private int nextIdx;

        private PlanIterator() {
            this.counters = new int[steps.size()];
        }

        @Override
        public boolean hasNext() {
            return nextIdx < steps.size();
        }

        @Override
        public ExecutionStep next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ExecutionStep step = steps.get(nextIdx);

            if (step instanceof ExecutionStep.LoopStep) {
                ExecutionStep.LoopStep loopStep = (ExecutionStep.LoopStep) step;
                if (loopStep.getTimes() < 0) {
                    nextIdx = loopStep.getPosition();
                } else if (loopStep.getTimes() > counters[nextIdx]) {
                    counters[nextIdx] = counters[nextIdx] + 1;
                    nextIdx = loopStep.getPosition();
                    return next();
                } else {
                    counters[nextIdx] = 0;
                    nextIdx++;
                }
                return next();
            }

            nextIdx++;
            return step;
        }
    }
}
