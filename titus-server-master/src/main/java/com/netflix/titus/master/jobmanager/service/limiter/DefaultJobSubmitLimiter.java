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

package com.netflix.titus.master.jobmanager.service.limiter;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;

@Singleton
public class DefaultJobSubmitLimiter implements JobSubmitLimiter {

    private final JobManagerConfiguration configuration;
    private final V3JobOperations v3JobOperations;

    private final ConcurrentMap<String, Boolean> reservedJobIdSequences = new ConcurrentHashMap<>();

    @Inject
    public DefaultJobSubmitLimiter(JobManagerConfiguration configuration,
                                   V3JobOperations v3JobOperations) {
        this.configuration = configuration;
        this.v3JobOperations = v3JobOperations;
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

    @Override
    public <JOB_DESCR> Optional<String> reserveId(JOB_DESCR jobDescriptor) {
        String idSeq = createJobIdSequenceFrom(jobDescriptor);
        if (idSeq == null) {
            return Optional.empty();
        }
        if (reservedJobIdSequences.putIfAbsent(idSeq, true) == null) {
            return Optional.empty();
        }
        return Optional.of("Job sequence id reserved by another pending job create request: " + idSeq);
    }

    @Override
    public <JOB_DESCR> void releaseId(JOB_DESCR jobDescriptor) {
        String idSeq = createJobIdSequenceFrom(jobDescriptor);
        if (idSeq != null) {
            reservedJobIdSequences.remove(idSeq);
        }
    }

    private Optional<String> checkActiveJobLimit() {
        int totalJobs = v3JobOperations.getJobs().size();
        long limit = configuration.getMaxActiveJobs();
        if (totalJobs >= limit) {
            return Optional.of(String.format("Reached a limit of active jobs Titus can run (active=%d, limit=%d)", totalJobs, limit));
        }
        return Optional.empty();
    }

    private <JOB_DESCR> Optional<String> checkJobIdSequence(JOB_DESCR jobDescriptor) {
        String jobIdSequence = createJobIdSequenceFrom(jobDescriptor);
        if (jobIdSequence == null) {
            return Optional.empty();
        }

        return isJobSequenceUsed(jobIdSequence).map(existingJobId ->
                String.format("Constraint violation - job with group sequence %s exists (%s)", jobIdSequence, existingJobId)
        );
    }

    private <JOB_DESCR> String createJobIdSequenceFrom(JOB_DESCR jobDescriptor) {
        return jobDescriptor instanceof JobDescriptor
                ? formatJobGroupName((JobDescriptor<?>) jobDescriptor)
                : Parameters.getJobIdSequence(((V2JobDefinition) jobDescriptor).getParameters());
    }

    private Optional<String> isJobSequenceUsed(String newJobIdSequence) {
        return v3JobOperations.getJobs().stream()
                .filter(j -> {
                    String v3JobIdSequence = formatJobGroupName(j.getJobDescriptor());
                    return v3JobIdSequence != null && v3JobIdSequence.equals(newJobIdSequence);
                })
                .map(Job::getId)
                .findFirst();
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
}
