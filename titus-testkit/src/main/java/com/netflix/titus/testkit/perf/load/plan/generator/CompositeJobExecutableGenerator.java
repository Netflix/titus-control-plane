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

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.testkit.perf.load.plan.JobExecutableGenerator;
import rx.Observable;

public class CompositeJobExecutableGenerator extends JobExecutableGenerator {

    private final Observable<Executable> mergedPlans;
    private final List<JobExecutableGenerator> jobExecutableGenerators;

    public CompositeJobExecutableGenerator(List<JobExecutableGenerator> jobExecutableGenerators) {
        this.mergedPlans = Observable.merge(
                jobExecutableGenerators.stream().map(JobExecutableGenerator::executionPlans).collect(Collectors.toList())
        );
        this.jobExecutableGenerators = jobExecutableGenerators;
    }

    @Override
    public Observable<Executable> executionPlans() {
        return mergedPlans;
    }

    @Override
    public void completed(Executable executable) {
        jobExecutableGenerators.forEach(s -> s.completed(executable));
    }
}
