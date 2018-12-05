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

import java.util.ArrayList;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.testkit.perf.load.plan.generator.ConstantLoadJobExecutableGenerator;
import com.netflix.titus.testkit.perf.load.plan.generator.CompositeJobExecutableGenerator;
import rx.Observable;

public abstract class JobExecutableGenerator {

    public static class Executable {
        private final String owner;
        private final JobDescriptor<?> jobSpec;
        private final JobExecutionPlan jobExecutionPlan;

        public Executable(String owner, JobDescriptor<?> jobSpec, JobExecutionPlan executionPlan) {
            this.owner = owner;
            this.jobSpec = jobSpec;
            this.jobExecutionPlan = executionPlan;
        }

        public String getOwner() {
            return owner;
        }

        public JobDescriptor<?> getJobSpec() {
            return jobSpec;
        }

        public JobExecutionPlan getJobExecutionPlan() {
            return jobExecutionPlan;
        }
    }

    public abstract Observable<Executable> executionPlans();

    public abstract void completed(Executable executable);

    public static ExecutionScenarioBuilder newBuilder() {
        return new ExecutionScenarioBuilder();
    }

    public static class ExecutionScenarioBuilder {

        private final List<JobExecutableGenerator> scenarios = new ArrayList<>();

        public ExecutionScenarioBuilder constantLoad(JobDescriptor<?> jobSpec, JobExecutionPlan plan, int instances) {
            scenarios.add(new ConstantLoadJobExecutableGenerator("scenario#" + scenarios.size(), jobSpec, plan, instances));
            return this;
        }

        public JobExecutableGenerator build() {
            return new CompositeJobExecutableGenerator(new ArrayList<>(scenarios));
        }
    }
}
