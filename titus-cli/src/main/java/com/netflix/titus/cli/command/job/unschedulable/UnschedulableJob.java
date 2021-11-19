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

package com.netflix.titus.cli.command.job.unschedulable;

class UnschedulableJob {

    private final String jobId;
    private final String reason;

    public UnschedulableJob(String jobId, String reason) {
        this.jobId = jobId;
        this.reason = reason;
    }

    public String getJobId() {
        return jobId;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "UnschedulableJob{" +
                "jobId='" + jobId + '\'' +
                ", reason='" + reason + '\'' +
                '}';
    }
}
