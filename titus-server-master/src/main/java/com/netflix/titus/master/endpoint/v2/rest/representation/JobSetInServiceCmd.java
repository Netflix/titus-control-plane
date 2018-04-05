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

package com.netflix.titus.master.endpoint.v2.rest.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JobSetInServiceCmd {

    private final String user;
    private final String jobId;
    private final boolean inService;

    @JsonCreator
    public JobSetInServiceCmd(@JsonProperty("user") String user,
                              @JsonProperty("jobId") String jobId,
                              @JsonProperty("inService") boolean inService) {

        this.user = user;
        this.jobId = jobId;
        this.inService = inService;
    }

    public String getUser() {
        return user;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isInService() {
        return inService;
    }

    @Override
    public String toString() {
        return "JobSetInServiceCmd{" +
                "user='" + user + '\'' +
                ", jobId='" + jobId + '\'' +
                ", inService=" + inService +
                '}';
    }
}
