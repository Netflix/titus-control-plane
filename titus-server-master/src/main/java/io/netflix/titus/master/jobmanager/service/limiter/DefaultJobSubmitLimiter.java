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

package io.netflix.titus.master.jobmanager.service.limiter;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.event.JobClosedEvent;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.event.JobStateChangeEvent;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

@Singleton
public class DefaultJobSubmitLimiter implements JobSubmitLimiter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobSubmitLimiter.class);

    private final JobManagerConfiguration configuration;
    private final V2JobOperations v2JobOperations;
    private final RxEventBus eventBus;
    private final V3JobOperations v3JobOperations;

    private final ConcurrentMap<String, String> jobGroupId2JobId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> jobIdToJobGroupId = new ConcurrentHashMap<>();

    private Subscription v2Subscription;
    private Subscription v3Subscription;

    private final Object lock = new Object();

    @Inject
    public DefaultJobSubmitLimiter(JobManagerConfiguration configuration,
                                   V2JobOperations v2JobOperations,
                                   RxEventBus eventBus,
                                   V3JobOperations v3JobOperations) {
        this.configuration = configuration;
        this.v2JobOperations = v2JobOperations;
        this.eventBus = eventBus;
        this.v3JobOperations = v3JobOperations;
    }

    @Activator
    public void enterActiveMode() {
        v2JobOperations.getAllJobMgrs().forEach(jm -> {
            String jobIdSequence = Parameters.getJobIdSequence(jm.getJobMetadata().getParameters());
            addIfNotNull(jm.getJobMetadata().getJobId(), jobIdSequence);
        });
        v3JobOperations.getJobs().forEach(job ->
                addIfNotNull(job.getId(), formatJobGroupName(job.getJobDescriptor()))
        );

        this.v2Subscription = eventBus.listen(getClass().getSimpleName(), JobStateChangeEvent.class).subscribe(
                this::handleV2JobUpdateEvent,
                e -> logger.error("Error in V2 job event stream", e),
                () -> logger.info("V3 job event stream closed")
        );
        this.v3Subscription = v3JobOperations.observeJobs().subscribe(
                this::handleV3JobUpdateEvent,
                e -> logger.error("Error in V3 job event stream", e),
                () -> logger.info("V3 job event stream closed")
        );
    }

    @PreDestroy
    public void shutdown() {
        if (v2Subscription != null) {
            v2Subscription.unsubscribe();
        }
        if (v3Subscription != null) {
            v3Subscription.unsubscribe();
        }
    }

    @Override
    public <JOB_DESCR> Optional<String> checkIfAllowed(JOB_DESCR jobDescriptor) {
        Preconditions.checkArgument(
                jobDescriptor instanceof JobDescriptor || jobDescriptor instanceof V2JobDefinition,
                "Not V2 or V3 job descriptor"
        );

        Optional<String> activeJobLimit = checkActiveJobLimit();
        if (activeJobLimit.isPresent()) {
            return activeJobLimit;
        }
        return checkJobIdSequence(jobDescriptor);

    }

    private Optional<String> checkActiveJobLimit() {
        int totalJobs = v2JobOperations.getAllJobMgrs().size() + v3JobOperations.getJobs().size();
        long limit = configuration.getMaxActiveJobs();
        if (totalJobs >= limit) {
            return Optional.of(String.format("Reached a limit of active jobs Titus can run (active=%d, limit=%d)", totalJobs, limit));
        }
        return Optional.empty();
    }

    private <JOB_DESCR> Optional<String> checkJobIdSequence(JOB_DESCR jobDescriptor) {
        String jobIdSequence = jobDescriptor instanceof JobDescriptor
                ? formatJobGroupName((JobDescriptor<?>) jobDescriptor)
                : Parameters.getJobIdSequence(((V2JobDefinition) jobDescriptor).getParameters());

        if (jobIdSequence == null) {
            return Optional.empty();
        }

        String existingJob = jobGroupId2JobId.get(jobIdSequence);
        if (existingJob != null) {
            return Optional.of(String.format("Constraint violation - job with group sequence %s exists (%s)", jobIdSequence, existingJob));
        }
        return Optional.empty();
    }

    private static String formatJobGroupName(JobDescriptor<?> jobDescriptor) {
        JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        if (jobGroupInfo.getSequence().isEmpty()) {
            return null;
        }
        return String.join("-",
                jobDescriptor.getApplicationName(),
                jobGroupInfo.getStack(),
                jobGroupInfo.getDetail(),
                jobGroupInfo.getSequence()
        );
    }

    private void handleV2JobUpdateEvent(JobStateChangeEvent jobEvent) {
        switch (jobEvent.getJobState()) {
            case Created:
                if (jobEvent.getSource() == null) {
                    return;
                }
                V2JobMetadata jobMetadata = (V2JobMetadata) jobEvent.getSource();
                String jobIdSequence = Parameters.getJobIdSequence(jobMetadata.getParameters());
                addIfNotNull(jobEvent.getJobId(), jobIdSequence);
                break;
            case Finished:
                remove(jobEvent.getJobId());
        }
    }

    private void handleV3JobUpdateEvent(JobManagerEvent event) {
        if (event instanceof JobUpdateEvent) {
            JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
            if (!jobUpdateEvent.getJob().isPresent()) {
                return;
            }
            Job job = jobUpdateEvent.getJob().get();
            if (job.getStatus().getState() == JobState.Accepted) {
                addIfNotNull(event.getId(), formatJobGroupName(job.getJobDescriptor()));
            }
        } else if (event instanceof JobClosedEvent) {
            remove(event.getId());
        }
    }

    private void addIfNotNull(String jobId, String jobIdSequence) {
        if (jobIdSequence != null) {
            synchronized (lock) {
                jobGroupId2JobId.put(jobIdSequence, jobId);
                jobIdToJobGroupId.put(jobId, jobIdSequence);
            }
        }
    }

    private void remove(String jobId) {
        String groupName = jobIdToJobGroupId.get(jobId);
        if (groupName != null) {
            synchronized (lock) {
                jobGroupId2JobId.remove(groupName);
                jobIdToJobGroupId.remove(jobId);
            }
        }
    }
}
