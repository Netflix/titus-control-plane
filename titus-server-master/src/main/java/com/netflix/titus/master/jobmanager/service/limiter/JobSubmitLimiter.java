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

import com.netflix.titus.api.jobmanager.service.JobManagerException;

public interface JobSubmitLimiter {

    /**
     * Check if it is ok to schedule a given job. If not, the result contains a reason message.
     */
    <JOB_DESCR> Optional<JobManagerException> checkIfAllowed(JOB_DESCR jobDescriptor);

    /**
     * Reserve job id sequence. If reservation fails, the result contains a reason message.
     */
    <JOB_DESCR> Optional<String> reserveId(JOB_DESCR jobDescriptor);

    /**
     * Release job id sequence.
     */
    <JOB_DESCR> void releaseId(JOB_DESCR jobDescriptor);
}
