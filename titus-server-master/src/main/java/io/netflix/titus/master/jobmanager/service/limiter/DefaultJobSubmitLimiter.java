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

import java.util.ArrayList;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.master.job.JobMgr;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;

@Singleton
public class DefaultJobSubmitLimiter implements JobSubmitLimiter {

    private final JobManagerConfiguration configuration;
    private final V2JobOperations v2JobOperations;
    private final V3JobOperations v3JobOperations;

    @Inject
    public DefaultJobSubmitLimiter(JobManagerConfiguration configuration,
                                   V2JobOperations v2JobOperations,
                                   V3JobOperations v3JobOperations) {
        this.configuration = configuration;
        this.v2JobOperations = v2JobOperations;
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

        Optional<String> existingV2Job = isJobSequenceInV2Engine(jobIdSequence);
        if (existingV2Job.isPresent()) {
            return Optional.of(String.format("Constraint violation - job with group sequence %s exists (%s)", jobIdSequence, existingV2Job.get()));
        }
        return isJobSequenceInV3Engine(jobIdSequence).map(existingJobId ->
                String.format("Constraint violation - job with group sequence %s exists (%s)", jobIdSequence, existingJobId)
        );
    }

    private Optional<String> isJobSequenceInV2Engine(String newJobIdSequence) {
        return new ArrayList<>(v2JobOperations.getAllJobMgrs()).stream()
                .filter(j -> {
                    V2JobMetadata jobDescriptor = j.getJobMetadata();
                    if (jobDescriptor == null) {
                        return false;
                    }
                    String v2JobIdSequence = Parameters.getJobIdSequence(jobDescriptor.getParameters());
                    return v2JobIdSequence != null && v2JobIdSequence.equals(newJobIdSequence);
                })
                .map(JobMgr::getJobId)
                .findFirst();
    }

    private Optional<String> isJobSequenceInV3Engine(String newJobIdSequence) {
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
