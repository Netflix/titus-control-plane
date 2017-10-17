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
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import rx.Observable;
import rx.Subscriber;
import rx.observers.SerializedSubscriber;

public abstract class ExecutionScenario {

    public static class Executable {
        private final String owner;
        private final TitusJobSpec jobSpec;
        private final ExecutionPlan executionPlan;

        private Executable(String owner, TitusJobSpec jobSpec, ExecutionPlan executionPlan) {
            this.owner = owner;
            this.jobSpec = jobSpec;
            this.executionPlan = executionPlan;
        }

        public String getOwner() {
            return owner;
        }

        public TitusJobSpec getJobSpec() {
            return jobSpec;
        }

        public ExecutionPlan getExecutionPlan() {
            return executionPlan;
        }
    }

    public abstract Observable<Executable> executionPlans();

    public abstract void completed(Executable executable);

    private static class ConstantLoadScenario extends ExecutionScenario {

        private final Executable executable;
        private final int size;

        private volatile Subscriber<? super Executable> scenarioSubscriber;

        ConstantLoadScenario(String owner, TitusJobSpec jobSpec, ExecutionPlan plan, int size) {
            this.executable = new Executable(owner, jobSpec, plan);
            this.size = size;
        }

        @Override
        public Observable<Executable> executionPlans() {
            return Observable.unsafeCreate(subscriber -> {
                // FIXME This is prone to race conditions.
                Preconditions.checkState(scenarioSubscriber == null, "Expected single subscription");
                scenarioSubscriber = new SerializedSubscriber<>(subscriber);
                for (int i = 0; i < size; i++) {
                    subscriber.onNext(executable);
                }
            });
        }

        @Override
        public void completed(Executable executable) {
            if (scenarioSubscriber != null && executable == this.executable) {
                scenarioSubscriber.onNext(executable);
            }
        }
    }

    private static class MultipleScenarios extends ExecutionScenario {

        private final Observable<Executable> mergedPlans;
        private final List<ExecutionScenario> executionScenarios;

        MultipleScenarios(List<ExecutionScenario> executionScenarios) {
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

    public static ExecutionScenarioBuilder newBuilder() {
        return new ExecutionScenarioBuilder();
    }

    public static class ExecutionScenarioBuilder {

        private final List<ExecutionScenario> scenarios = new ArrayList<>();

        public ExecutionScenarioBuilder constantLoad(TitusJobSpec jobSpec, ExecutionPlan plan, int instances) {
            scenarios.add(new ConstantLoadScenario("scenario#" + scenarios.size(), jobSpec, plan, instances));
            return this;
        }

        public ExecutionScenario build() {
            return new MultipleScenarios(new ArrayList<>(scenarios));
        }
    }
}
