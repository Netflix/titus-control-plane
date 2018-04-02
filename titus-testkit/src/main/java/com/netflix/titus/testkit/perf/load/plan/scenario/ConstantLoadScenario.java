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

import com.google.common.base.Preconditions;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import rx.Observable;
import rx.Subscriber;
import rx.observers.SerializedSubscriber;

public class ConstantLoadScenario extends ExecutionScenario {

    private final Executable executable;
    private final int size;

    private volatile Subscriber<? super Executable> scenarioSubscriber;

    public ConstantLoadScenario(String owner, JobDescriptor<?> jobSpec, ExecutionPlan plan, int size) {
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
