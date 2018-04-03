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

package com.netflix.titus.testkit.perf.load.plan.scenario;

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import rx.Observable;

public class MultipleScenarios extends ExecutionScenario {

    private final Observable<Executable> mergedPlans;
    private final List<ExecutionScenario> executionScenarios;

    public MultipleScenarios(List<ExecutionScenario> executionScenarios) {
        this.mergedPlans = Observable.merge(
                executionScenarios.stream().map(ExecutionScenario::executionPlans).collect(Collectors.toList())
        );
        this.executionScenarios = executionScenarios;
    }

    @Override
    public Observable<Executable> executionPlans() {
        return mergedPlans;
    }

    @Override
    public void completed(Executable executable) {
        executionScenarios.forEach(s -> s.completed(executable));
    }
}
