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
import java.util.Optional;
import java.util.Random;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.ExecutionStep;
import com.netflix.titus.testkit.perf.load.plan.JobExecutionStep;
import com.netflix.titus.testkit.perf.load.runner.job.JobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Observable;
import rx.Scheduler;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toPage;

public class JobExecutionPlanRunner extends ExecutionPlanRunner {

    private final static Logger logger = LoggerFactory.getLogger(JobExecutionPlanRunner.class);

    private static final Random random = new Random();

    private final JobExecutor executor;
    private final ExecutionContext context;
    private final Scheduler.Worker worker;
    private final long executionDeadline;
    private final Iterator<ExecutionStep> planIterator;

    public JobExecutionPlanRunner(JobExecutor jobExecutor,
                                  ExecutionPlan jobExecutionPlan,
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
        worker.unsubscribe();
    }

    public Mono<Void> awaitJobCompletion() {
        return executor.awaitJobCompletion();
    }


    private void runNext() {
        ExecutionStep step = planIterator.next();
        logger.info("Executing step {}", step);

        if (executionDeadline < System.currentTimeMillis()) {
            logger.info("Job execution time limit passed; terminating: jobId={}", executor.getJobId());
            terminateJob();
            return;
        }
        if (step.getName().equals(JobExecutionStep.NAME_TERMINATE)) {
            terminateJob();
            return;
        }

        Observable<Void> action = toCommonAction(step)
                .orElseGet(() -> toJobAction(step)
                        .map(ReactorExt::toObservable)
                        .orElseThrow(() -> new IllegalStateException("Unknown execution step " + step))
                );

        long startTime = worker.now();
        action.subscribe(
                never -> {
                },
                e -> {
                    logger.warn("Execution plan action failed for job " + executor.getName(), e);
                    terminateJob();
                },
                () -> {
                    logger.info("Finished executing step {} in {}[ms]", step, worker.now() - startTime);
                    worker.schedule(this::runNext);
                }
        );
    }

    private Optional<Mono<Void>> toJobAction(ExecutionStep step) {
        switch (step.getName()) {
            case JobExecutionStep.NAME_SCALE_UP:
                return Optional.ofNullable(doScaleUp((JobExecutionStep.ScaleUpStep) step));
            case JobExecutionStep.NAME_SCALE_DOWN:
                return Optional.ofNullable(doScaleDown((JobExecutionStep.ScaleDownStep) step));
            case JobExecutionStep.NAME_FIND_OWN_JOB:
                return Optional.of(doFindOwnJob());
            case JobExecutionStep.NAME_FIND_OWN_TASK:
                return Optional.of(doFindOwnTasks());
            case JobExecutionStep.NAME_KILL_RANDOM_TASK:
                return Optional.ofNullable(doKillRandomTask());
            case JobExecutionStep.NAME_EVICT_RANDOM_TASK:
                return Optional.of(doEvictRandomTask());
            case JobExecutionStep.NAME_TERMINATE_AND_SHRINK_RANDOM_TASK:
                return Optional.ofNullable(doTerminateAndShrinkRandomTask());
            case JobExecutionStep.NAME_AWAIT_COMPLETION:
                return Optional.of(doAwaitCompletion());
        }
        return Optional.empty();
    }

    private Mono<Void> doScaleUp(JobExecutionStep.ScaleUpStep step) {
        return executor.scaleUp(step.getDelta());
    }

    private Mono<Void> doScaleDown(JobExecutionStep.ScaleDownStep step) {
        return executor.scaleDown(step.getDelta());
    }

    private Mono<Void> doFindOwnJob() {
        return context.getJobManagementClient()
                .findJobs(CollectionsExt.asMap("jobIds", executor.getJobId()), toPage(PAGE_OF_500_ITEMS))
                .then();
    }

    private Mono<Void> doFindOwnTasks() {
        return context.getJobManagementClient()
                .findTasks(CollectionsExt.asMap("jobIds", executor.getJobId()), toPage(PAGE_OF_500_ITEMS))
                .then();
    }

    private Mono<Void> doKillRandomTask() {
        List<Task> activeTasks = executor.getActiveTasks();
        if (activeTasks.isEmpty()) {
            return Mono.empty();
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

    private Mono<Void> doTerminateAndShrinkRandomTask() {
        List<Task> activeTasks = executor.getActiveTasks();
        if (activeTasks.isEmpty()) {
            return Mono.empty();
        }

        Task task = activeTasks.get(random.nextInt(activeTasks.size()));
        return executor.terminateAndShrink(task.getId());
    }

    private Mono<Void> doAwaitCompletion() {
        return Mono.never();
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
