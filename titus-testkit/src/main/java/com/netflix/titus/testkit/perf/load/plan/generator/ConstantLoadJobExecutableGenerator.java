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

package com.netflix.titus.testkit.perf.load.plan.generator;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.JobExecutableGenerator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.UnicastProcessor;

public class ConstantLoadJobExecutableGenerator extends JobExecutableGenerator {

    private final Executable executable;
    private final FluxProcessor<Executable, Executable> executionPlans;

    public ConstantLoadJobExecutableGenerator(String owner, JobDescriptor<?> jobSpec, ExecutionPlan plan, int numberOfJobs) {
        this.executable = new Executable(owner, jobSpec, plan);
        this.executionPlans = UnicastProcessor.<Executable>create().serialize();
        for (int i = 0; i < numberOfJobs; i++) {
            this.executionPlans.onNext(executable);
        }
    }

    @Override
    public Flux<Executable> executionPlans() {
        return executionPlans;
    }

    @Override
    public void completed(Executable executable) {
        if (executable == this.executable) {
            executionPlans.onNext(executable);
        }
    }
}
