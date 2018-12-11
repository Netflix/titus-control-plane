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

public class JobExecutionPlanBuilder extends ExecutionPlanBuilder<JobExecutionPlanBuilder> {

    public JobExecutionPlanBuilder findOwnJob() {
        steps.add(JobExecutionStep.findOwnJob());
        return this;
    }

    public JobExecutionPlanBuilder findOwnTasks() {
        steps.add(JobExecutionStep.findOwnTasks());
        return this;
    }

    public JobExecutionPlanBuilder killRandomTask() {
        steps.add(JobExecutionStep.killRandomTask());
        return this;
    }

    public JobExecutionPlanBuilder evictRandomTask() {
        steps.add(JobExecutionStep.evictRandomTask());
        return this;
    }

    public JobExecutionPlanBuilder terminateAndShrinkRandomTask() {
        steps.add(JobExecutionStep.terminateAndShrinkRandomTask());
        return this;
    }

    public JobExecutionPlanBuilder scaleUp(int delta) {
        steps.add(JobExecutionStep.scaleUp(delta));
        return this;
    }

    public JobExecutionPlanBuilder scaleDown(int delta) {
        steps.add(JobExecutionStep.scaleDown(delta));
        return this;
    }

    public JobExecutionPlanBuilder awaitCompletion() {
        steps.add(JobExecutionStep.awaitCompletion());
        return this;
    }

    public JobExecutionPlanBuilder terminate() {
        steps.add(JobExecutionStep.terminate());
        return this;
    }

    public ExecutionPlan build() {
        if (steps.get(steps.size() - 1) != JobExecutionStep.terminate()) {
            steps.add(JobExecutionStep.terminate());
        }
        return new ExecutionPlan(totalRunningTime, steps);
    }
}
