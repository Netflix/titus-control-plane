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

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.testkit.perf.load.job.JobExecutor;
import io.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import io.netflix.titus.testkit.perf.load.plan.ExecutionStep;
import io.netflix.titus.testkit.perf.load.plan.ExecutionStep.AwaitCompletionStep;
import io.netflix.titus.testkit.perf.load.plan.ExecutionStep.DelayStep;
import io.netflix.titus.testkit.perf.load.plan.ExecutionStep.KillRandomTaskStep;
import io.netflix.titus.testkit.perf.load.plan.ExecutionStep.ScaleDownStep;
import io.netflix.titus.testkit.perf.load.plan.ExecutionStep.ScaleUpStep;
import io.netflix.titus.testkit.perf.load.plan.ExecutionStep.TerminateAndShrinkRandomTaskStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;

public class ExecutionPlanRunner {

    private final static Logger logger = LoggerFactory.getLogger(ExecutionPlanRunner.class);

    private static final Random random = new Random();

    private final JobExecutor executor;
    private final Scheduler.Worker worker;
    private final Iterator<ExecutionStep> planIterator;

    public ExecutionPlanRunner(JobExecutor jobExecutor,
                               ExecutionPlan executionPlan,
                               Scheduler scheduler) {
        this.executor = jobExecutor;
        this.planIterator = executionPlan.newInstance();

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
        if (step instanceof ScaleUpStep) {
            action = doScaleUp((ScaleUpStep) step);
        } else if (step instanceof ScaleDownStep) {
            action = doScaleDown((ScaleDownStep) step);
        } else if (step instanceof KillRandomTaskStep) {
            action = doKillRandomTask();
        } else if (step instanceof TerminateAndShrinkRandomTaskStep) {
            action = doTerminateAndShrinkRandomTask();
        } else if (step instanceof DelayStep) {
            action = doDelay((DelayStep) step);
        } else if (step instanceof AwaitCompletionStep) {
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

    private Observable<Void> doScaleUp(ScaleUpStep step) {
        return executor.scaleUp(step.getDelta());
    }

    private Observable<Void> doScaleDown(ScaleDownStep step) {
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

    private Observable<Void> doTerminateAndShrinkRandomTask() {
        List<Task> activeTasks = executor.getActiveTasks();
        if (activeTasks.isEmpty()) {
            return Observable.empty();
        }

        Task task = activeTasks.get(random.nextInt(activeTasks.size()));
        return executor.terminateAndShrink(task.getId());
    }

    private Observable<Void> doDelay(DelayStep delayStep) {
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
