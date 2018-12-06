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

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import com.netflix.titus.testkit.perf.load.plan.JobExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.JobExecutionStep;
import com.netflix.titus.testkit.perf.load.runner.job.JobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;

public class JobExecutionPlanRunner {

    private final static Logger logger = LoggerFactory.getLogger(JobExecutionPlanRunner.class);

    private static final Random random = new Random();

    private static final Page PAGE_OF_500_ITEMS = Page.newBuilder().setPageSize(500).build();

    private final JobExecutor executor;
    private final ExecutionContext context;
    private final Scheduler.Worker worker;
    private final long executionDeadline;
    private final Iterator<JobExecutionStep> planIterator;

    public JobExecutionPlanRunner(JobExecutor jobExecutor,
                                  JobExecutionPlan jobExecutionPlan,
                                  ExecutionContext context,
                                  Scheduler scheduler) {
        this.executor = jobExecutor;
        this.executionDeadline = System.currentTimeMillis() + jobExecutionPlan.getTotalRunningTime().toMillis();
        this.planIterator = jobExecutionPlan.newInstance();
        this.context = context;

        this.worker = scheduler.createWorker();
    }

    public void start() {
        runNext();
    }

    public void stop() {
        executor.shutdown();
    }

    public Completable awaitJobCompletion() {
        return executor.awaitJobCompletion();
    }

    private void runNext() {
        JobExecutionStep step = planIterator.next();
        logger.info("Executing step {}", step);

        if (step instanceof JobExecutionStep.TerminateStep || executionDeadline < System.currentTimeMillis()) {
            terminateJob();
            return;
        }

        Observable<Void> action;
        if (step instanceof JobExecutionStep.ScaleUpStep) {
            action = doScaleUp((JobExecutionStep.ScaleUpStep) step);
        } else if (step instanceof JobExecutionStep.ScaleDownStep) {
            action = doScaleDown((JobExecutionStep.ScaleDownStep) step);
        } else if (step instanceof JobExecutionStep.FindOwnJobStep) {
            action = doFindOwnJob();
        } else if (step instanceof JobExecutionStep.FindOwnTasksStep) {
            action = doFindOwnTasks();
        } else if (step instanceof JobExecutionStep.KillRandomTaskStep) {
            action = doKillRandomTask();
        } else if (step instanceof JobExecutionStep.EvictRandomTaskStep) {
            action = ReactorExt.toObservable(doEvictRandomTask());
        } else if (step instanceof JobExecutionStep.TerminateAndShrinkRandomTaskStep) {
            action = doTerminateAndShrinkRandomTask();
        } else if (step instanceof JobExecutionStep.DelayStep) {
            action = doDelay((JobExecutionStep.DelayStep) step);
        } else if (step instanceof JobExecutionStep.AwaitCompletionStep) {
            action = doAwaitCompletion();
        } else {
            throw new IllegalStateException("Unknown execution step " + step);
        }

        long startTime = worker.now();
        action.subscribe(
                never -> {
                },
                e -> {
                    logger.warn("Execution plan action for job {} failed with an error {}", executor.getName(), e.getMessage());
                    terminateJob();
                },
                () -> {
                    logger.info("Finished executing step {} in {}[ms]", step, worker.now() - startTime);
                    worker.schedule(this::runNext);
                }
        );
    }

    private Observable<Void> doScaleUp(JobExecutionStep.ScaleUpStep step) {
        return executor.scaleUp(step.getDelta());
    }

    private Observable<Void> doScaleDown(JobExecutionStep.ScaleDownStep step) {
        return executor.scaleDown(step.getDelta());
    }

    private Observable<Void> doFindOwnJob() {
        return context.getJobManagementClient()
                .findJobs(JobQuery.newBuilder()
                        .putFilteringCriteria("jobIds", executor.getJobId())
                        .setPage(PAGE_OF_500_ITEMS)
                        .build()
                )
                .ignoreElements()
                .cast(Void.class);
    }

    private Observable<Void> doFindOwnTasks() {
        return context.getJobManagementClient()
                .findTasks(TaskQuery.newBuilder()
                        .putFilteringCriteria("jobIds", executor.getJobId())
                        .setPage(PAGE_OF_500_ITEMS)
                        .build()
                )
                .ignoreElements()
                .cast(Void.class);
    }

    private Observable<Void> doKillRandomTask() {
        List<Task> activeTasks = executor.getActiveTasks();
        if (activeTasks.isEmpty()) {
            return Observable.empty();
        }

        Task task = activeTasks.get(random.nextInt(activeTasks.size()));
        return executor.killTask(task.getId());
    }

    private Mono<Void> doEvictRandomTask() {
        List<Task> activeTasks = executor.getActiveTasks();
        if (activeTasks.isEmpty()) {
            return Mono.empty();
        }

        Task task = activeTasks.get(random.nextInt(activeTasks.size()));
        return executor.evictTask(task.getId());
    }

    private Observable<Void> doTerminateAndShrinkRandomTask() {
        List<Task> activeTasks = executor.getActiveTasks();
        if (activeTasks.isEmpty()) {
            return Observable.empty();
        }

        Task task = activeTasks.get(random.nextInt(activeTasks.size()));
        return executor.terminateAndShrink(task.getId());
    }

    private Observable<Void> doDelay(JobExecutionStep.DelayStep delayStep) {
        return Observable.timer(delayStep.getDelayMs(), TimeUnit.MILLISECONDS).ignoreElements().cast(Void.class);
    }

    private Observable<Void> doAwaitCompletion() {
        return Observable.never();
    }

    private void terminateJob() {
        if (executor.isSubmitted()) {
            executor.killJob().subscribe(
                    ignore -> {
                    },
                    e -> logger.warn("Cannot kill job {}: {}", executor.getJobId(), e.getMessage())
            );
        }
        worker.unsubscribe();
    }
}
