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

package io.netflix.titus.master.endpoint.v2.rest.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JobKillCmd {

    private final String jobId;
    private final String user;

    @JsonCreator
    public JobKillCmd(@JsonProperty("user") String user, @JsonProperty("jobId") String jobId) {
        this.jobId = jobId;
        this.user = user;
    }

    public String getJobId() {
        return jobId;
    }

    public String getUser() {
        return user;
    }

    @Override
    public String toString() {
        return "JobKillCmd{" +
                "jobId='" + jobId + '\'' +
                ", user='" + user + '\'' +
                '}';
    }
}
