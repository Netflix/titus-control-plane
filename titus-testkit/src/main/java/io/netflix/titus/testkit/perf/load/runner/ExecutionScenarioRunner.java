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

package io.netflix.titus.testkit.perf.load.runner;

import java.util.HashMap;
import java.util.Map;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import io.netflix.titus.testkit.perf.load.job.ActiveJobsMonitor;
import io.netflix.titus.testkit.perf.load.job.BatchJobExecutor;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent;
import io.netflix.titus.testkit.perf.load.job.JobExecutor;
import io.netflix.titus.testkit.perf.load.job.ServiceJobExecutor;
import io.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import rx.Observable;
import rx.Scheduler;

public class ExecutionScenarioRunner {

    private final Observable<JobChangeEvent> scenarioObservable;
    private final ExecutionContext context;

    public ExecutionScenarioRunner(ExecutionScenario scenario,
                                   ActiveJobsMonitor activeJobsMonitor,
                                   ExecutionContext context,
                                   Scheduler scheduler) {
        this.context = context;
        this.scenarioObservable = scenario.executionPlans().flatMap((ExecutionScenario.Executable executable) -> {
            TitusJobSpec jobSpec = tagged(executable.getJobSpec());
            TitusJobType type = jobSpec.getType();
            JobExecutor executor = type == TitusJobType.batch
                    ? new BatchJobExecutor(jobSpec, activeJobsMonitor, context)
                    : new ServiceJobExecutor(jobSpec, activeJobsMonitor, context);
            ExecutionPlanRunner runner = new ExecutionPlanRunner(executor, executable.getExecutionPlan(), scheduler);
            return runner.updates()
                    .doOnSubscribe(() -> {
                        runner.start();
                    })
                    .doOnUnsubscribe(() -> {
                        runner.stop();
                        scenario.completed(executable);
                    });
        }).share();
    }

    private TitusJobSpec tagged(TitusJobSpec jobSpec) {
        Map<String, String> labels = new HashMap<>();
        if (jobSpec.getLabels() != null) {
            labels.putAll(jobSpec.getLabels());
        }
        labels.put(ExecutionContext.LABEL_SESSION, context.getSessionId());
        return new TitusJobSpec.Builder(jobSpec).labels(labels).build();
    }

    public Observable<JobChangeEvent> start() {
        return scenarioObservable;
    }
}
