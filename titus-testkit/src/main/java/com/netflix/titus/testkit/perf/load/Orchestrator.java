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

package com.netflix.titus.testkit.perf.load;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.testkit.perf.load.catalog.ExecutionScenarioCatalog;
import com.netflix.titus.testkit.perf.load.job.BatchJobExecutor;
import com.netflix.titus.testkit.perf.load.job.JobExecutor;
import com.netflix.titus.testkit.perf.load.job.ServiceJobExecutor;
import com.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import com.netflix.titus.testkit.perf.load.report.MetricsCollector;
import com.netflix.titus.testkit.perf.load.report.TextReporter;
import com.netflix.titus.testkit.perf.load.runner.ExecutionPlanRunner;
import com.netflix.titus.testkit.perf.load.runner.Terminator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

@Singleton
public class Orchestrator {

    private static final Logger logger = LoggerFactory.getLogger(Orchestrator.class);

    private final Subscription scenarioSubscription;
    private final MetricsCollector metricsCollector;
    private final TextReporter textReporter;

    private final CountDownLatch doneLatch = new CountDownLatch(1);
    private final AtomicInteger nextSequenceId = new AtomicInteger();

    @Inject
    public Orchestrator(LoadConfiguration configuration,
                        Terminator terminator,
                        ExecutionContext context) {
        if (configuration.isClean()) {
            terminator.doClean();
        }

        this.scenarioSubscription = startExecutionScenario(newExecutionScenario(configuration), context).subscribe(
                () -> logger.info("Orchestrator's scenario subscription completed"),
                e -> logger.error("Orchestrator's scenario subscription terminated with an error", e)
        );
        this.metricsCollector = new MetricsCollector();
        metricsCollector.watch(context);
        this.textReporter = new TextReporter(metricsCollector, Schedulers.computation());
        textReporter.start();
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(scenarioSubscription);
    }

    protected ExecutionScenario newExecutionScenario(LoadConfiguration configuration) {
//                ExecutionScenarioCatalog.oneAutoScalingService(configuration.getScaleFactor()),
//                ExecutionScenarioCatalog.oneScalingServiceWihTerminateAndShrink(configuration.getScaleFactor()),
//                ExecutionScenarioCatalog.mixedLoad(configuration.getScaleFactor()),
//        return ExecutionScenarioCatalog.batchJob(1, configuration.getScaleFactor());
        return ExecutionScenarioCatalog.evictions(1, configuration.getScaleFactor());
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public void awaitTermination() {
        try {
            doneLatch.await();
        } catch (InterruptedException ignore) {
        }
    }

    private Completable startExecutionScenario(ExecutionScenario executionScenario, ExecutionContext context) {
        return executionScenario.executionPlans()
                .flatMap(executable -> {
                    JobDescriptor<?> jobSpec = tagged(newJobDescriptor(executable), context);
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
                                        ExecutionPlanRunner runner = new ExecutionPlanRunner(executor, executable.getExecutionPlan(), Schedulers.computation());
                                        return runner.awaitJobCompletion()
                                                .doOnSubscribe(subscription -> runner.start())
                                                .doOnUnsubscribe(() -> {
                                                    logger.info("Creating new replacement job...");
                                                    runner.stop();
                                                    executionScenario.completed(executable);
                                                }).toObservable();
                                    }
                            );
                }).toCompletable();
    }

    private JobDescriptor<?> newJobDescriptor(ExecutionScenario.Executable executable) {
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

    private JobDescriptor<?> tagged(JobDescriptor<?> jobSpec, ExecutionContext context) {
        return jobSpec.toBuilder().withAttributes(
                CollectionsExt.copyAndAdd(jobSpec.getAttributes(), ExecutionContext.LABEL_SESSION, context.getSessionId())
        ).build();
    }
}
