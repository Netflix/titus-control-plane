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

package com.netflix.titus.testkit.perf.load.runner;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import com.netflix.titus.testkit.perf.load.plan.JobExecutableGenerator;
import com.netflix.titus.testkit.perf.load.runner.job.BatchJobExecutor;
import com.netflix.titus.testkit.perf.load.runner.job.JobExecutor;
import com.netflix.titus.testkit.perf.load.runner.job.ServiceJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

public class ScenarioRunner {

    private static final Logger logger = LoggerFactory.getLogger(Orchestrator.class);

    private final Subscription scenarioSubscription;

    private final AtomicInteger nextSequenceId = new AtomicInteger();
    private final String scenarioExecutionId;
    private final Map<String, Object> requestContext;

    public ScenarioRunner(String scenarioExecutionId,
                          Map<String, Object> requestContext,
                          JobExecutableGenerator jobExecutableGenerator,
                          ExecutionContext context) {
        this.scenarioExecutionId = scenarioExecutionId;
        this.requestContext = requestContext;
        this.scenarioSubscription = startExecutionScenario(jobExecutableGenerator, context).subscribe(
                () -> logger.info("Orchestrator's scenario subscription completed"),
                e -> logger.error("Orchestrator's scenario subscription terminated with an error", e)
        );
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(scenarioSubscription);
    }

    public String getScenarioExecutionId() {
        return scenarioExecutionId;
    }

    public Map<String, Object> getRequestContext() {
        return requestContext;
    }

    private Completable startExecutionScenario(JobExecutableGenerator jobExecutableGenerator, ExecutionContext context) {
        return jobExecutableGenerator.executionPlans()
                .flatMap(executable -> {
                    JobDescriptor<?> jobSpec = tagged(newJobDescriptor(executable));
                    Observable<? extends JobExecutor> executorObservable = JobFunctions.isBatchJob(jobSpec)
                            ? BatchJobExecutor.submitJob((JobDescriptor<BatchJobExt>) jobSpec, context)
                            : ServiceJobExecutor.submitJob((JobDescriptor<ServiceJobExt>) jobSpec, context);

                    return executorObservable
                            .retryWhen(RetryHandlerBuilder.retryHandler()
                                    .withUnlimitedRetries()
                                    .withDelay(1_000, 30_000, TimeUnit.MILLISECONDS)
                                    .buildExponentialBackoff()
                            )
                            .flatMap(executor -> {
                                        JobExecutionPlanRunner runner = new JobExecutionPlanRunner(executor, executable.getJobExecutionPlan(), context, Schedulers.computation());
                                        return runner.awaitJobCompletion()
                                                .doOnSubscribe(subscription -> runner.start())
                                                .doOnUnsubscribe(() -> {
                                                    logger.info("Creating new replacement job...");
                                                    runner.stop();
                                                    jobExecutableGenerator.completed(executable);
                                                }).toObservable();
                                    }
                            );
                }).toCompletable();
    }

    private JobDescriptor<?> newJobDescriptor(JobExecutableGenerator.Executable executable) {
        JobDescriptor<?> jobDescriptor = executable.getJobSpec();
        if (JobFunctions.isBatchJob(jobDescriptor)) {
            return jobDescriptor;
        }
        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        String seq = jobGroupInfo.getSequence() + nextSequenceId.getAndIncrement();
        return jobDescriptor.toBuilder().withJobGroupInfo(
                jobGroupInfo.toBuilder().withSequence(seq).build()
        ).build();
    }

    private JobDescriptor<?> tagged(JobDescriptor<?> jobSpec) {
        return jobSpec.toBuilder().withAttributes(
                CollectionsExt.copyAndAdd(jobSpec.getAttributes(), ExecutionContext.LABEL_SESSION, scenarioExecutionId)
        ).build();
    }
}
