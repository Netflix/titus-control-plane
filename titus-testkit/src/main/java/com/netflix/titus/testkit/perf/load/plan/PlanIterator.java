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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class PlanIterator implements Iterator<ExecutionStep> {

    private final int[] counters;
    private final List<ExecutionStep> steps;
    private int nextIdx;

    PlanIterator(List<ExecutionStep> steps) {
        this.counters = new int[steps.size()];
        this.steps = steps;
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
