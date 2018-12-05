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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

public class JobExecutionPlan {

    private final Duration totalRunningTime;
    private final List<JobExecutionStep> steps;

    public JobExecutionPlan(Duration totalRunningTime, List<JobExecutionStep> steps) {
        this.totalRunningTime = totalRunningTime;
        this.steps = steps;
    }

    public Duration getTotalRunningTime() {
        return totalRunningTime;
    }

    public Iterator<JobExecutionStep> newInstance() {
        return new PlanIterator();
    }

    public static ExecutionPlanBuilder newBuilder() {
        return new ExecutionPlanBuilder();
    }

    public static class ExecutionPlanBuilder {

        private Duration totalRunningTime = Duration.ofDays(30);
        private final List<JobExecutionStep> steps = new ArrayList<>();
        private final Map<String, Integer> labelPos = new HashMap<>();

        public ExecutionPlanBuilder totalRunningTime(Duration totalRunningTime) {
            this.totalRunningTime = totalRunningTime;
            return this;
        }

        public ExecutionPlanBuilder label(String name) {
            labelPos.put(name, steps.size());
            return this;
        }

        public ExecutionPlanBuilder findOwnJob() {
            steps.add(JobExecutionStep.findOwnJob());
            return this;
        }

        public ExecutionPlanBuilder findOwnTasks() {
            steps.add(JobExecutionStep.findOwnTasks());
            return this;
        }

        public ExecutionPlanBuilder killRandomTask() {
            steps.add(JobExecutionStep.killRandomTask());
            return this;
        }

        public ExecutionPlanBuilder evictRandomTask() {
            steps.add(JobExecutionStep.evictRandomTask());
            return this;
        }

        public ExecutionPlanBuilder terminateAndShrinkRandomTask() {
            steps.add(JobExecutionStep.terminateAndShrinkRandomTask());
            return this;
        }

        public ExecutionPlanBuilder scaleUp(int delta) {
            steps.add(JobExecutionStep.scaleUp(delta));
            return this;
        }

        public ExecutionPlanBuilder scaleDown(int delta) {
            steps.add(JobExecutionStep.scaleDown(delta));
            return this;
        }

        public ExecutionPlanBuilder loop(String label) {
            return loop(label, -1);
        }

        public ExecutionPlanBuilder loop(String label, int times) {
            Preconditions.checkArgument(labelPos.containsKey(label), "Execution plan has no label " + label);
            steps.add(JobExecutionStep.loop(labelPos.get(label), times));
            return this;
        }

        public ExecutionPlanBuilder delay(long duration, TimeUnit timeUnit) {
            steps.add(JobExecutionStep.delayStep(duration, timeUnit));
            return this;
        }

        public ExecutionPlanBuilder delay(Duration duration) {
            steps.add(JobExecutionStep.delayStep(duration.toMillis(), TimeUnit.MILLISECONDS));
            return this;
        }

        public ExecutionPlanBuilder awaitCompletion() {
            steps.add(JobExecutionStep.awaitCompletion());
            return this;
        }

        public ExecutionPlanBuilder terminate() {
            steps.add(JobExecutionStep.terminate());
            return this;
        }

        public JobExecutionPlan build() {
            if (steps.get(steps.size() - 1) != JobExecutionStep.terminate()) {
                steps.add(JobExecutionStep.terminate());
            }
            return new JobExecutionPlan(totalRunningTime, steps);
        }
    }

    private class PlanIterator implements Iterator<JobExecutionStep> {

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
        public JobExecutionStep next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            JobExecutionStep step = steps.get(nextIdx);

            if (step instanceof JobExecutionStep.LoopStep) {
                JobExecutionStep.LoopStep loopStep = (JobExecutionStep.LoopStep) step;
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
