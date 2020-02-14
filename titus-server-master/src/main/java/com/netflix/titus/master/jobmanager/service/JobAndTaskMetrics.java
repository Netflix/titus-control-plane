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

package com.netflix.titus.master.jobmanager.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.histogram.Histogram;
import com.netflix.titus.common.util.histogram.HistogramDescriptor;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Supplementary metrics based on both job/task state, and elapsed time. These metrics cannot be computed only
 * in response to system state change events. Instead, they are recomputed at regular interval.
 * <p>
 */
@Singleton
public class JobAndTaskMetrics {

    private static final Logger logger = LoggerFactory.getLogger(JobAndTaskMetrics.class);

    private static final String JOBS_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.jobs";
    private static final String TASKS_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.tasks";
    private static final String TASK_IN_STATE_ROOT_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.taskLiveness.";
    private static final String TASK_IN_STATE_METRIC_NAME = TASK_IN_STATE_ROOT_METRIC_NAME + "duration";
    private static final String TASK_STATE_CHANGE_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.taskStateUpdates";

    private static final List<String> TRACKED_STATES = Arrays.asList(
            TaskState.Accepted.name(),
            TaskState.Launched.name(),
            TaskState.StartInitiated.name(),
            TaskState.Started.name(),
            TaskState.KillInitiated.name()
    );

    private static final HistogramDescriptor HISTOGRAM_DESCRIPTOR = HistogramDescriptor.histogramOf(
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.MINUTES.toMillis(15),
            TimeUnit.MINUTES.toMillis(30),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(6),
            TimeUnit.HOURS.toMillis(12),
            TimeUnit.HOURS.toMillis(24),
            TimeUnit.DAYS.toMillis(2),
            TimeUnit.DAYS.toMillis(3),
            TimeUnit.DAYS.toMillis(4),
            TimeUnit.DAYS.toMillis(5)
    );

    private final ApplicationSlaManagementService applicationSlaManagementService;
    private final V3JobOperations v3JobOperations;
    private final JobManagerConfiguration configuration;
    private final Registry registry;

    private final Map<String, Map<String, List<Gauge>>> capacityGroupsMetrics = new HashMap<>();
    private final Id jobCountId;
    private final Id taskCountId;

    private Subscription taskLivenessRefreshSubscription;
    private Subscription taskStateUpdateSubscription;

    @Inject
    public JobAndTaskMetrics(ApplicationSlaManagementService applicationSlaManagementService,
                             V3JobOperations v3JobOperations,
                             JobManagerConfiguration configuration,
                             Registry registry) {
        this.applicationSlaManagementService = applicationSlaManagementService;
        this.v3JobOperations = v3JobOperations;
        this.configuration = configuration;
        this.registry = registry;

        this.jobCountId = registry.createId(JOBS_METRIC_NAME);
        this.taskCountId = registry.createId(TASKS_METRIC_NAME);
    }

    @Activator
    public void enterActiveMode() {
        long intervalMs = Math.max(1_000, configuration.getTaskLivenessPollerIntervalMs());

        this.taskStateUpdateSubscription = v3JobOperations.observeJobs().subscribe(
                event -> {
                    if (event instanceof TaskUpdateEvent) {
                        updateTaskMetrics((TaskUpdateEvent) event);
                    }
                },
                e -> logger.error("Event stream terminated with an error", e),
                () -> logger.info("Event stream completed")
        );

        this.taskLivenessRefreshSubscription = ObservableExt.schedule(
                TASK_IN_STATE_ROOT_METRIC_NAME + "scheduler", registry, "TaskLivenessRefreshAction",
                Completable.fromAction(this::refresh), intervalMs, intervalMs, TimeUnit.MILLISECONDS, Schedulers.computation()
        ).subscribe(result ->
                result.ifPresent(error -> logger.warn("Task liveness metrics refresh error", error))
        );
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(taskStateUpdateSubscription, taskLivenessRefreshSubscription);
    }

    private void updateTaskMetrics(TaskUpdateEvent event) {
        Pair<Tier, String> assignment = JobManagerUtil.getTierAssignment(event.getCurrentJob(), applicationSlaManagementService);
        Task task = event.getCurrentTask();
        registry.counter(
                TASK_STATE_CHANGE_METRIC_NAME,
                "tier", assignment.getLeft().name(),
                "capacityGroup", assignment.getRight(),
                "state", task.getStatus().getState().name(),
                "kubeScheduler", "" + JobFunctions.hasOwnedByKubeSchedulerAttribute(task)
        ).increment();
    }

    private void refresh() {
        Map<String, Tier> tierMap = buildTierMap();

        List<Pair<Job, List<Task>>> jobsAndTasks = v3JobOperations.getJobsAndTasks();
        List<Job> jobs = v3JobOperations.getJobs();
        List<Task> tasks = v3JobOperations.getTasks();

        updateJobCounts(jobsAndTasks);
        updateTaskCounts(tasks);

        Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms = buildCapacityGroupsHistograms(tierMap.keySet(), jobs);
        resetDroppedCapacityGroups(capacityGroupsHistograms.keySet());
        updateCapacityGroupCounters(capacityGroupsHistograms, tierMap);
    }

    private void resetDroppedCapacityGroups(Set<String> knownCapacityGroups) {
        CollectionsExt.copyAndRemove(capacityGroupsMetrics.keySet(), knownCapacityGroups).forEach(absent -> {
            Map<String, List<Gauge>> removed = capacityGroupsMetrics.remove(absent);
            removed.values().forEach(gauges -> gauges.forEach(gauge -> gauge.set(0)));
        });
    }

    private void updateCapacityGroupCounters(Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms, Map<String, Tier> tierMap) {
        capacityGroupsHistograms.forEach((capacityGroup, histograms) -> {
            Id baseId = registry.createId(
                    TASK_IN_STATE_METRIC_NAME,
                    "tier", tierMap.get(capacityGroup).name(),
                    "capacityGroup", capacityGroup
            );
            Map<String, List<Gauge>> capacityMetricsByState = capacityGroupsMetrics.computeIfAbsent(capacityGroup, k -> new HashMap<>());
            for (String state : TRACKED_STATES) {
                List<Gauge> updatedGauges = updateStateCounters(baseId, state, histograms.get(state), capacityMetricsByState.get(state));
                if (updatedGauges.isEmpty()) {
                    capacityMetricsByState.remove(capacityGroup);
                } else {
                    capacityMetricsByState.put(state, updatedGauges);
                }
            }
        });
    }

    private List<Gauge> updateStateCounters(Id baseId, String state, Histogram.Builder histogramBuilder, List<Gauge> gauges) {
        if (histogramBuilder == null) {
            // Nothing running for this state, reset gauges
            if (gauges != null) {
                gauges.forEach(g -> g.set(0));
            }
            return Collections.emptyList();
        }

        List<Long> counters = histogramBuilder.build().getCounters();

        // First time we have data for this capacity group.
        if (gauges == null) {
            Id id = baseId.withTag("state", state);
            List<Long> valueBounds = HISTOGRAM_DESCRIPTOR.getValueBounds();
            List<Gauge> newGauges = new ArrayList<>();
            for (int i = 0; i <= valueBounds.size(); i++) {
                Gauge newGauge;
                if (i < valueBounds.size()) {
                    long delayMs = valueBounds.get(i);
                    newGauge = registry.gauge(id.withTag("delay", DateTimeExt.toTimeUnitString(delayMs)));
                } else {
                    newGauge = registry.gauge(id.withTag("delay", "Unlimited"));
                }
                newGauge.set(counters.get(i));
                newGauges.add(newGauge);
            }
            return newGauges;
        }

        // Update gauges
        for (int i = 0; i < counters.size(); i++) {
            gauges.get(i).set(counters.get(i));
        }

        return gauges;
    }

    /**
     * Traverse all active jobs and update the count metrics
     */
    private void updateJobCounts(List<Pair<Job, List<Task>>> jobsAndTasks) {
        int emptyJobs = 0;
        int serviceJobsOwnedByKubeScheduler = 0;
        int serviceJobsOwnedByFenzo = 0;
        int batchJobsOwnedByKubeScheduler = 0;
        int batchJobsOwnedByFenzo = 0;

        for (Pair<Job, List<Task>> jobAndTasks : jobsAndTasks) {
            Job job = jobAndTasks.getLeft();
            List<Task> tasks = jobAndTasks.getRight();

            if (JobFunctions.getJobDesiredSize(job) == 0) {
                emptyJobs++;
            } else {
                boolean ownedByKubeScheduler = tasks.stream().anyMatch(JobFunctions::hasOwnedByKubeSchedulerAttribute);
                boolean serviceJob = JobFunctions.isServiceJob(job);

                if (ownedByKubeScheduler) {
                    if (serviceJob) {
                        serviceJobsOwnedByKubeScheduler++;
                    } else {
                        batchJobsOwnedByKubeScheduler++;
                    }
                } else {
                    if (serviceJob) {
                        serviceJobsOwnedByFenzo++;
                    } else {
                        batchJobsOwnedByFenzo++;
                    }
                }
            }
        }

        registry.gauge(jobCountId.withTag("emptyJobs", "true")).set(emptyJobs);

        registry.gauge(jobCountId.withTags(
                "jobType", "service",
                "kubeScheduler", "true"
        )).set(serviceJobsOwnedByKubeScheduler);
        registry.gauge(jobCountId.withTags(
                "jobType", "service",
                "kubeScheduler", "false"
        )).set(serviceJobsOwnedByFenzo);

        registry.gauge(jobCountId.withTags(
                "jobType", "batch",
                "kubeScheduler", "true"
        )).set(batchJobsOwnedByKubeScheduler);
        registry.gauge(jobCountId.withTags(
                "jobType", "batch",
                "kubeScheduler", "false"
        )).set(batchJobsOwnedByFenzo);
    }

    /**
     * Traverse all tasks and update the count metrics
     */
    private void updateTaskCounts(List<Task> tasks) {
        int tasksOwnedByKubeScheduler = 0;
        for (Task task : tasks) {
            if (JobFunctions.hasOwnedByKubeSchedulerAttribute(task)) {
                tasksOwnedByKubeScheduler++;
            }
        }
        registry.gauge(taskCountId.withTag("kubeScheduler", "true")).set(tasksOwnedByKubeScheduler);
        registry.gauge(taskCountId.withTag("kubeScheduler", "false")).set(tasks.size() - tasksOwnedByKubeScheduler);
    }

    /**
     * Traverse all active tasks and collect their state and the time they stayed in this state (the latter in form of histogram).
     *
     * @return mapOf(capacityGroupName - > mapOf ( taskState, histogram))
     */
    private Map<String, Map<String, Histogram.Builder>> buildCapacityGroupsHistograms(Set<String> capacityGroups, List<Job> jobs) {
        Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms = newCapacityHistograms(capacityGroups);
        jobs.forEach(job -> resolveCapacityGroup(job, capacityGroupsHistograms).ifPresent(capacityGroup ->
                buildCapacityGroupHistogram(job, capacityGroup, capacityGroupsHistograms)
        ));
        return capacityGroupsHistograms;
    }

    private void buildCapacityGroupHistogram(Job<?> job, String capacityGroup, Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms) {
        // 'capacityGroupsHistograms' is pre-initialized, but to avoid race condition we make extra check here.
        Map<String, Histogram.Builder> capacityGroupHistograms = capacityGroupsHistograms.computeIfAbsent(capacityGroup, k -> new HashMap<>());
        List<Task> tasks;
        try {
            tasks = v3JobOperations.getTasks(job.getId());
        } catch (Exception e) {
            // We work on live data, which may be removed at any point in time.
            return;
        }
        tasks.forEach(task -> {
            long timestamp = task.getStatus().getTimestamp();
            if (timestamp > 0) {
                long durationMs = System.currentTimeMillis() - timestamp;
                capacityGroupHistograms.computeIfAbsent(
                        task.getStatus().getState().name(),
                        name -> Histogram.newBuilder(HISTOGRAM_DESCRIPTOR)
                ).increment(durationMs);
            }
        });
    }

    private Optional<String> resolveCapacityGroup(Job<?> job, Map<String, Map<String, Histogram.Builder>> capacityHistograms) {
        String capacityGroup = job.getJobDescriptor().getCapacityGroup();
        if (StringExt.isEmpty(capacityGroup)) {
            capacityGroup = job.getJobDescriptor().getApplicationName();
        }
        if (StringExt.isEmpty(capacityGroup)) {
            return Optional.of(ApplicationSlaManagementService.DEFAULT_APPLICATION);
        }
        return Optional.of(capacityHistograms.containsKey(capacityGroup) ? capacityGroup : ApplicationSlaManagementService.DEFAULT_APPLICATION);
    }

    private Map<String, Tier> buildTierMap() {
        return applicationSlaManagementService.getApplicationSLAs().stream()
                .collect(Collectors.toMap(ApplicationSLA::getAppName, ApplicationSLA::getTier));
    }

    private Map<String, Map<String, Histogram.Builder>> newCapacityHistograms(Set<String> capacityGroups) {
        return capacityGroups.stream().collect(Collectors.toMap(name -> name, name -> new HashMap<>()));
    }
}
