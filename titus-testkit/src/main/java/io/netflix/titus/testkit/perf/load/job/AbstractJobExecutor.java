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

package io.netflix.titus.testkit.perf.load.job;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

public abstract class AbstractJobExecutor implements JobExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJobExecutor.class);

    protected final TitusJobSpec jobSpec;
    private final Supplier<Observable<ActiveJobsMonitor.ActiveJobs>> activeJobsSupplier;
    protected final ExecutionContext context;
    protected volatile String name;
    protected volatile String jobId;
    protected volatile boolean doRun = true;

    protected volatile List<TaskInfo> activeTasks = Collections.emptyList();

    protected volatile long lastChangeTimestamp = -1;
    protected volatile TitusJobInfo lastJobInfo;

    protected volatile Subscription activeJobsSubscription;
    protected final JobReconciler jobReconciler;
    protected final Subject<JobChangeEvent, JobChangeEvent> updateSubject = new SerializedSubject<>(PublishSubject.create());

    protected AbstractJobExecutor(TitusJobSpec jobSpec, Supplier<Observable<ActiveJobsMonitor.ActiveJobs>> activeJobsSupplier, ExecutionContext context) {
        this.jobSpec = jobSpec;
        this.name = buildJobUniqueName(jobSpec);
        this.activeJobsSupplier = activeJobsSupplier;
        this.context = context;
        // TODO batch/service
        this.jobReconciler = new JobReconciler(jobSpec.getInstances());
    }

    protected AbstractJobExecutor(TitusJobSpec jobSpec,
                                  ActiveJobsMonitor activeJobsMonitor,
                                  ExecutionContext context) {
        this(jobSpec, activeJobsMonitor::activeJobs, context);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean isSubmitted() {
        return doRun && jobId != null;
    }

    @Override
    public List<TaskInfo> getActiveTasks() {
        return activeTasks;
    }

    @Override
    public Observable<JobChangeEvent> updates() {
        return updateSubject.asObservable();
    }

    @Override
    public void shutdown() {
        if (doRun) {
            doRun = false;
            if (activeJobsSubscription != null) {
                this.activeTasks = Collections.emptyList();
                activeJobsSubscription.unsubscribe();
                context.getClient().killJob(jobId).subscribe(
                        ignore -> {
                        },
                        e -> logger.debug("Job {} cleanup failure", jobId, e)
                );
            }
        }
    }

    @Override
    public Observable<Void> submitJob() {
        Preconditions.checkState(doRun, "Job executor shut down already");
        return context.getClient()
                .submitJob(jobSpec)
                .doOnNext(jobId -> {
                    logger.info("Submitted job {}", jobId);
                    this.jobId = jobId;
                    this.name = name + '(' + jobId + ')';
                    jobReconciler.jobSubmitted(jobId).forEach(updateSubject::onNext);
                    startMonitoring();
                })
                .ignoreElements()
                .cast(Void.class)
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to submit job " + name, e)))
                .doOnSubscribe(() -> lastChangeTimestamp = -1)
                .doOnTerminate(() -> lastChangeTimestamp = System.currentTimeMillis());
    }

    @Override
    public Observable<Void> killJob() {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getClient()
                .killJob(jobId)
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to kill job " + name, e)))
                .doOnCompleted(() -> {
                    logger.info("Killed job {}", jobId);
                    jobReconciler.jobKillRequested().forEach(updateSubject::onNext);
                })
                .doOnSubscribe(() -> lastChangeTimestamp = -1)
                .doOnTerminate(() -> lastChangeTimestamp = System.currentTimeMillis());
    }

    @Override
    public Observable<Void> killTask(String taskId) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getClient()
                .killTask(taskId)
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to kill task " + taskId + " of job " + name, e)))
                .doOnCompleted(() -> {
                    logger.info("Killed task {}", taskId);
                    jobReconciler.taskKillRequested(taskId).forEach(updateSubject::onNext);
                })
                .doOnSubscribe(() -> lastChangeTimestamp = -1)
                .doOnTerminate(() -> lastChangeTimestamp = System.currentTimeMillis());
    }

    private static String buildJobUniqueName(TitusJobSpec jobSpec) {
        StringBuilder sb = new StringBuilder(jobSpec.getAppName());
        if (jobSpec.getJobGroupStack() != null) {
            sb.append('-').append(jobSpec.getJobGroupStack());
        }
        if (jobSpec.getJobGroupDetail() != null) {
            sb.append('-').append(jobSpec.getJobGroupDetail());
        }
        if (jobSpec.getJobGroupSequence() != null) {
            sb.append('-').append(jobSpec.getJobGroupSequence());
        }
        return sb.toString();
    }

    private void startMonitoring() {
        this.activeJobsSubscription = activeJobsSupplier.get()
                .flatMap(update -> {
                    if (lastChangeTimestamp > 0 && update.getTimestamp() > lastChangeTimestamp) {
                        TitusJobInfo jobInfo = update.getJobs().get(jobId);
                        if (jobInfo != null && !hasJobInfoChanged(jobInfo)) {
                            return Observable.empty();
                        }
                        return context.getClient().findJob(jobId, true);
                    }
                    return Observable.empty();
                })
                .subscribe(
                        changedJobInfo -> {
                            TitusJobState state = changedJobInfo.getState();
                            if (state == TitusJobState.FINISHED || state == TitusJobState.FAILED) {
                                activeTasks = Collections.emptyList();
                                logger.info("Job {} finished with state {}", jobId, state);
                                jobReconciler.jobFinished(changedJobInfo).forEach(updateSubject::onNext);
                                updateSubject.onCompleted();
                                activeJobsSubscription.unsubscribe();
                            } else {
                                activeTasks = changedJobInfo.getTasks();
                                List<JobChangeEvent> events = jobReconciler.reconcile(changedJobInfo);
                                if (!events.isEmpty()) {
                                    logger.info("{} state change(s) in job {}: {}", events.size(), jobId, events);
                                    events.forEach(updateSubject::onNext);
                                }
                            }
                        },
                        e -> logger.error("Unexpected error in job {} status update pipeline", jobId, e)
                );
    }

    private boolean hasJobInfoChanged(TitusJobInfo newJobInfo) {
        if (lastJobInfo == null) {
            return true;
        }

        try {
            List<TaskInfo> lastTasks = lastJobInfo.getTasks();
            List<TaskInfo> newTasks = newJobInfo.getTasks();

            boolean lastEmpty = CollectionsExt.isNullOrEmpty(lastTasks);
            boolean newEmpty = CollectionsExt.isNullOrEmpty(newTasks);


            if (lastEmpty && !newEmpty || !lastEmpty && newEmpty) {
                return true;
            }

            for (TaskInfo lasTask : lastTasks) {
                TaskInfo newTask = newTasks.stream().filter(t -> t.getId().equals(lasTask.getId())).findFirst().orElse(null);
                if (newTask == null) {
                    return true;
                }
                if (lasTask.getState() != newTask.getState()) {
                    return true;
                }
            }

            return false;
        } finally {
            this.lastJobInfo = newJobInfo;
        }
    }
}
