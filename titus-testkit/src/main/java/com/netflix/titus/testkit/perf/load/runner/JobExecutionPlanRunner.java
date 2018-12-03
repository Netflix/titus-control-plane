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
import com.netflix.titus.testkit.perf.load.job.JobExecutor;
import com.netflix.titus.testkit.perf.load.plan.JobExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.ExecutionStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;

public class JobExecutionPlanRunner {

    private final static Logger logger = LoggerFactory.getLogger(JobExecutionPlanRunner.class);

    private static final Random random = new Random();

    private final JobExecutor executor;
    private final Scheduler.Worker worker;
    private final Iterator<ExecutionStep> planIterator;

    public JobExecutionPlanRunner(JobExecutor jobExecutor,
                                  JobExecutionPlan jobExecutionPlan,
                                  Scheduler scheduler) {
        this.executor = jobExecutor;
        this.planIterator = jobExecutionPlan.newInstance();

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
        ExecutionStep step = planIterator.next();
        logger.info("Executing step {}", step);

        if (step instanceof ExecutionStep.TerminateStep) {
            terminateJob();
            return;
        }

        Observable<Void> action;
        if (step instanceof ExecutionStep.ScaleUpStep) {
            action = doScaleUp((ExecutionStep.ScaleUpStep) step);
        } else if (step instanceof ExecutionStep.ScaleDownStep) {
            action = doScaleDown((ExecutionStep.ScaleDownStep) step);
        } else if (step instanceof ExecutionStep.KillRandomTaskStep) {
            action = doKillRandomTask();
        } else if (step instanceof ExecutionStep.EvictRandomTaskStep) {
            action = ReactorExt.toObservable(doEvictRandomTask());
        } else if (step instanceof ExecutionStep.TerminateAndShrinkRandomTaskStep) {
            action = doTerminateAndShrinkRandomTask();
        } else if (step instanceof ExecutionStep.DelayStep) {
            action = doDelay((ExecutionStep.DelayStep) step);
        } else if (step instanceof ExecutionStep.AwaitCompletionStep) {
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

    private Observable<Void> doScaleUp(ExecutionStep.ScaleUpStep step) {
        return executor.scaleUp(step.getDelta());
    }

    private Observable<Void> doScaleDown(ExecutionStep.ScaleDownStep step) {
        return executor.scaleDown(step.getDelta());
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

    private Observable<Void> doDelay(ExecutionStep.DelayStep delayStep) {
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
