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

package com.netflix.titus.master.eviction.service.quota;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.ReactorRetriers;
import com.netflix.titus.master.eviction.service.quota.job.JobQuotaController;
import com.netflix.titus.master.eviction.service.quota.system.SystemQuotaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

@Singleton
public class TitusQuotasManager {

    private static final Logger logger = LoggerFactory.getLogger(TitusQuotasManager.class);

    private static final String NAME = TitusQuotasManager.class.getSimpleName();

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(5);

    private static final ConsumptionResult UNKNOWN_JOB = ConsumptionResult.rejected("Unknown job");

    private final V3JobOperations jobOperations;
    private final ContainerHealthService containerHealthService;
    private final SystemQuotaController systemQuotaController;
    private final TitusRuntime titusRuntime;

    private final ConcurrentMap<String, JobQuotaController> jobQuotaControllersByJobId = new ConcurrentHashMap<>();

    private final Object lock = new Object();

    private Disposable jobUpdateDisposable;

    @Inject
    public TitusQuotasManager(V3JobOperations jobOperations,
                              ContainerHealthService containerHealthService,
                              SystemQuotaController systemQuotaController,
                              TitusRuntime titusRuntime) {
        this.containerHealthService = containerHealthService;
        this.systemQuotaController = systemQuotaController;
        this.jobOperations = jobOperations;
        this.titusRuntime = titusRuntime;
    }

    @Activator
    public void enterActiveMode() {
        this.jobUpdateDisposable = jobOperations.observeJobsReactor()
                .filter(event -> event instanceof JobUpdateEvent)
                .map(event -> (Job) event.getCurrent())
                .compose(ReactorExt.head(jobOperations::getJobs))
                .compose(ReactorRetriers.instrumentedRetryer(NAME, RETRY_INTERVAL, logger))
                .subscribe(this::updateJobController);
    }

    @PreDestroy
    public void shutdown() {
        ReactorExt.safeDispose(jobUpdateDisposable);
    }

    public ConsumptionResult tryConsumeQuota(Job<?> job, Task task) {
        JobQuotaController jobQuotaController = jobQuotaControllersByJobId.get(job.getId());
        if (jobQuotaController == null) {
            return UNKNOWN_JOB;
        }

        String taskId = task.getId();

        synchronized (lock) {
            ConsumptionResult systemResult = systemQuotaController.consume(taskId);
            ConsumptionResult jobResult = jobQuotaController.consume(taskId);

            if (systemResult.isApproved() && jobResult.isApproved()) {
                return jobResult;
            }

            if (!systemResult.isApproved() && !jobResult.isApproved()) {
                return ConsumptionResult.rejected(String.format(
                        "No job and system quota: {systemQuota=%s, jobQuota=%s}",
                        systemResult.getRejectionReason().get(), jobResult.getRejectionReason().get()
                ));
            }

            if (systemResult.isApproved()) {
                systemQuotaController.giveBackConsumedQuota(taskId);
                return jobResult;
            }

            jobQuotaController.giveBackConsumedQuota(taskId);
            return systemResult;
        }
    }

    public EvictionQuota getSystemEvictionQuota() {
        return EvictionQuota.systemQuota(systemQuotaController.getQuota());
    }

    public Optional<EvictionQuota> findJobEvictionQuota(String jobId) {
        JobQuotaController jobQuotaController = jobQuotaControllersByJobId.get(jobId);
        return jobQuotaController == null ? Optional.empty() : Optional.of(EvictionQuota.jobQuota(jobId, jobQuotaController.getQuota()));
    }

    private void updateJobController(Job newJob) {
        if (newJob.getStatus().getState() != JobState.Finished) {
            updateRunningJobController(newJob);
        } else {
            jobQuotaControllersByJobId.remove(newJob.getId());
        }
    }

    private void updateRunningJobController(Job<?> newJob) {
        JobQuotaController jobQuotaController = jobQuotaControllersByJobId.get(newJob.getId());

        if (jobQuotaController != null) {
            jobQuotaControllersByJobId.put(newJob.getId(), jobQuotaController.update(newJob));
        } else {
            jobQuotaControllersByJobId.put(newJob.getId(), new JobQuotaController(newJob, jobOperations, containerHealthService, titusRuntime));
        }
    }
}
