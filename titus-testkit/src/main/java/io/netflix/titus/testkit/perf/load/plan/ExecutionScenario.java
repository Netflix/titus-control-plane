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
import java.util.List;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.testkit.perf.load.plan.scenario.ConstantLoadScenario;
import io.netflix.titus.testkit.perf.load.plan.scenario.MultipleScenarios;
import rx.Observable;

public abstract class ExecutionScenario {

    public static class Executable {
        private final String owner;
        private final JobDescriptor<?> jobSpec;
        private final ExecutionPlan executionPlan;

        public Executable(String owner, JobDescriptor<?> jobSpec, ExecutionPlan executionPlan) {
            this.owner = owner;
            this.jobSpec = jobSpec;
            this.executionPlan = executionPlan;
        }

        public String getOwner() {
            return owner;
        }

        public JobDescriptor<?> getJobSpec() {
            return jobSpec;
        }

        public ExecutionPlan getExecutionPlan() {
            return executionPlan;
        }
    }

    public abstract Observable<Executable> executionPlans();

    public abstract void completed(Executable executable);

    public static ExecutionScenarioBuilder newBuilder() {
        return new ExecutionScenarioBuilder();
    }

    public static class ExecutionScenarioBuilder {

        private final List<ExecutionScenario> scenarios = new ArrayList<>();

        public ExecutionScenarioBuilder constantLoad(JobDescriptor<?> jobSpec, ExecutionPlan plan, int instances) {
            scenarios.add(new ConstantLoadScenario("scenario#" + scenarios.size(), jobSpec, plan, instances));
            return this;
        }

        public ExecutionScenario build() {
            return new MultipleScenarios(new ArrayList<>(scenarios));
        }
    }
}
