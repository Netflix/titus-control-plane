/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.store;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.ExecutableStatus;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.master.MetricConstants.METRIC_JOB_MANAGER;

@Singleton
public class ArchivedTasksGc {
    private static final Logger logger = LoggerFactory.getLogger(ArchivedTasksGc.class);

    private final V3JobOperations jobOperations;
    private final JobStore jobStore;
    private final TitusRuntime titusRuntime;
    private final ArchivedTasksGcConfiguration configuration;

    private final Gauge jobsEvaluatedGauge;
    private final Gauge jobsNeedingGcGauge;
    private final Gauge tasksNeedingGcGauge;
    private final Gauge tasksToGcGauge;

    private ScheduleReference schedulerRef;

    @Inject
    public ArchivedTasksGc(ArchivedTasksGcConfiguration configuration,
                           V3JobOperations jobOperations,
                           JobStore jobStore,
                           TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.jobOperations = jobOperations;
        this.jobStore = jobStore;
        this.titusRuntime = titusRuntime;

        Registry registry = titusRuntime.getRegistry();
        String metricNamePrefix = METRIC_JOB_MANAGER + "archivedTasksGc.";
        jobsEvaluatedGauge = registry.gauge(metricNamePrefix + "jobsEvaluated");
        jobsNeedingGcGauge = registry.gauge(metricNamePrefix + "jobsNeedingGc");
        tasksNeedingGcGauge = registry.gauge(metricNamePrefix + "tasksNeedingGc");
        tasksToGcGauge = registry.gauge(metricNamePrefix + "tasksToGc");
    }

    @Activator
    public void enterActiveMode() {
        ScheduleDescriptor scheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName("gcArchivedTasks")
                .withDescription("GC oldest archived pasts once the criteria is met")
                .withInitialDelay(Duration.ofMillis(configuration.getGcInitialDelayMs()))
                .withInterval(Duration.ofMillis(configuration.getGcIntervalMs()))
                .withTimeout(Duration.ofMinutes(configuration.getGcTimeoutMs()))
                .build();

        this.schedulerRef = titusRuntime.getLocalScheduler().schedule(
                scheduleDescriptor,
                e -> gc(),
                ExecutorsExt.namedSingleThreadExecutor(ArchivedTasksGc.class.getSimpleName())
        );
    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
    }

    @VisibleForTesting
    void gc() {
        if (!configuration.isGcEnabled()) {
            logger.info("GC is not enabled");
            return;
        }

        long maxNumberOfArchivedTasksPerJob = configuration.getMaxNumberOfArchivedTasksPerJob();

        List<String> jobIds = jobOperations.getJobs().stream().map(Job::getId).collect(Collectors.toList());
        logger.info("Evaluating {} jobs for GC", jobIds.size());
        jobsEvaluatedGauge.set(jobIds.size());

        List<Observable<Pair<String, Long>>> archivedTaskCountObservables = jobIds.stream()
                .map(jobId -> jobStore.retrieveArchivedTaskCountForJob(jobId).map(count -> Pair.of(jobId, count)))
                .collect(Collectors.toList());

        List<Pair<String, Long>> archivedTaskCountsPerJob = Observable.merge(archivedTaskCountObservables, configuration.getMaxRxConcurrency())
                .toList().toBlocking().singleOrDefault(Collections.emptyList());
        logger.debug("archivedTaskCountsPerJob: {}", archivedTaskCountsPerJob);

        List<String> jobsNeedingGc = new ArrayList<>();
        for (Pair<String, Long> archivedTaskCountPair : archivedTaskCountsPerJob) {
            if (archivedTaskCountPair.getRight() > maxNumberOfArchivedTasksPerJob) {
                jobsNeedingGc.add(archivedTaskCountPair.getLeft());
            }
        }
        logger.info("{} jobs need GC: {}", jobsNeedingGc.size(), jobsNeedingGc);
        jobsNeedingGcGauge.set(jobsNeedingGc.size());

        List<Observable<Pair<String, List<Task>>>> archivedTaskObservables = jobsNeedingGc.stream()
                .map(jobId -> jobStore.retrieveArchivedTasksForJob(jobId).toList().map(l -> Pair.of(jobId, l)))
                .collect(Collectors.toList());

        List<Pair<String, List<Task>>> archivedTasksPerJob = Observable.merge(archivedTaskObservables, configuration.getMaxRxConcurrency())
                .toList().toBlocking().singleOrDefault(Collections.emptyList());

        List<Task> archivedTasksNeedingGc = new ArrayList<>();
        for (Pair<String, List<Task>> archivedTasksPair : archivedTasksPerJob) {
            List<Task> archivedTasksToGc = getArchivedTasksToGc(archivedTasksPair.getRight(), maxNumberOfArchivedTasksPerJob);
            archivedTasksNeedingGc.addAll(archivedTasksToGc);
        }
        logger.info("{} tasks need GC", archivedTasksNeedingGc.size());
        tasksNeedingGcGauge.set(archivedTasksNeedingGc.size());

        List<Task> archivedTasksToGc = archivedTasksNeedingGc.stream()
                .limit(configuration.getMaxNumberOfArchivedTasksToGcPerIteration())
                .collect(Collectors.toList());
        List<String> archivedTasksToGcIds = archivedTasksToGc.stream().map(Task::getId).collect(Collectors.toList());
        logger.info("Starting to GC {} tasks: {}", archivedTasksToGc.size(), archivedTasksToGcIds);
        tasksToGcGauge.set(archivedTasksToGc.size());

        List<Completable> deleteArchivedTaskCompletables = archivedTasksToGc.stream()
                .map(t -> jobStore.deleteArchivedTask(t.getJobId(), t.getId())).collect(Collectors.toList());

        Completable.merge(Observable.from(deleteArchivedTaskCompletables), configuration.getMaxRxConcurrency()).await();
        logger.info("Finished GC");
    }

    static List<Task> getArchivedTasksToGc(List<Task> tasks, long maxNumberOfArchivedTasksPerJob) {
        int total = tasks.size();
        if (total > maxNumberOfArchivedTasksPerJob) {
            tasks.sort(Comparator.comparing(t -> JobFunctions.findTaskStatus(t, TaskState.Accepted).map(ExecutableStatus::getTimestamp).orElse(0L)));
            int numberOfTasksToGc = (int) (total - maxNumberOfArchivedTasksPerJob);
            return tasks.subList(0, numberOfTasksToGc);
        }
        return Collections.emptyList();
    }
}
