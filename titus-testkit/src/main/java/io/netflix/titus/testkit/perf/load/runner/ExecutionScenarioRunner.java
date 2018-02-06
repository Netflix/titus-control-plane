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

import com.netflix.titus.grpc.protogen.JobChangeNotification;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import io.netflix.titus.testkit.perf.load.job.BatchJobExecutor;
import io.netflix.titus.testkit.perf.load.job.JobExecutor;
import io.netflix.titus.testkit.perf.load.job.ServiceJobExecutor;
import io.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;

public class ExecutionScenarioRunner {

    private final ExecutionContext context;
    private final Completable action;
    private final Observable<JobManagerEvent<?>> jobsObservable;

    public ExecutionScenarioRunner(ExecutionScenario scenario,
                                   ExecutionContext context,
                                   Scheduler scheduler) {
        this.context = context;
        this.jobsObservable = observeJobs(context).share();
        this.action = scenario.executionPlans().flatMap((ExecutionScenario.Executable executable) -> {
            JobDescriptor<?> jobSpec = tagged(executable.getJobSpec());
            JobExecutor executor = JobFunctions.isBatchJob(jobSpec)
                    ? new BatchJobExecutor((JobDescriptor<BatchJobExt>) jobSpec, jobsObservable, context)
                    : new ServiceJobExecutor((JobDescriptor<ServiceJobExt>) jobSpec, jobsObservable, context);
            ExecutionPlanRunner runner = new ExecutionPlanRunner(executor, executable.getExecutionPlan(), scheduler);
            return runner.updates()
                    .ignoreElements()
                    .doOnSubscribe(runner::start)
                    .doOnUnsubscribe(() -> {
                        runner.stop();
                        scenario.completed(executable);
                    });
        }).toCompletable();
    }

    private JobDescriptor<?> tagged(JobDescriptor<?> jobSpec) {
        return jobSpec.toBuilder().withAttributes(
                CollectionsExt.copyAndAdd(jobSpec.getAttributes(), ExecutionContext.LABEL_SESSION, context.getSessionId())
        ).build();
    }

    public Observable<JobManagerEvent<?>> start() {
        action.subscribe();
        return jobsObservable;
    }

    private Observable<JobManagerEvent<?>> observeJobs(ExecutionContext context) {
        return context.getJobManagementClient().observeJobs()
                .filter(this::isJobOrTaskUpdate)
                .compose(ObservableExt.mapWithState(new HashMap<>(), this::toCoreEvent));
    }

    private Pair<JobManagerEvent<?>, Map<String, Object>> toCoreEvent(JobChangeNotification event, Map<String, Object> state) {
        if (event.getNotificationCase() == JobChangeNotification.NotificationCase.JOBUPDATE) {
            Job<?> job = V3GrpcModelConverters.toCoreJob(event.getJobUpdate().getJob());

            Object previous = state.get(job.getId());
            state.put(job.getId(), job);

            if (previous == null) {
                return Pair.of(JobUpdateEvent.newJob(job), state);
            }
            return Pair.of(JobUpdateEvent.jobChange(job, (Job<?>) previous), state);
        }

        // Task update
        Task task = V3GrpcModelConverters.toCoreTask(event.getTaskUpdate().getTask());
        Job<?> job = (Job<?>) state.get(task.getJobId());

        Object previous = state.get(task.getId());
        state.put(task.getId(), task);

        if (previous == null) {
            return Pair.of(TaskUpdateEvent.newTask(job, task), state);
        }
        return Pair.of(TaskUpdateEvent.taskChange(job, task, (Task) previous), state);
    }

    private boolean isJobOrTaskUpdate(JobChangeNotification event) {
        return event.getNotificationCase() == JobChangeNotification.NotificationCase.JOBUPDATE || event.getNotificationCase() == JobChangeNotification.NotificationCase.TASKUPDATE;
    }
}
