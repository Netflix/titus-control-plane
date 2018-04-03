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

public class JobSetInstanceCountsCmd {

    private final String user;
    private final String jobId;
    private final int instancesDesired;
    private final int instancesMin;
    private final int instancesMax;

    @JsonCreator
    public JobSetInstanceCountsCmd(@JsonProperty("user") String user,
                                   @JsonProperty("jobId") String jobId,
                                   @JsonProperty("instancesDesired") int instancesDesired,
                                   @JsonProperty("instancesMin") int instancesMin,
                                   @JsonProperty("instancesMax") int instancesMax) {
        this.user = user;
        this.jobId = jobId;
        this.instancesDesired = instancesDesired;
        this.instancesMin = instancesMin;
        this.instancesMax = instancesMax;
    }

    public String getUser() {
        return user;
    }

    public String getJobId() {
        return jobId;
    }

    public int getInstancesDesired() {
        return instancesDesired;
    }

    public int getInstancesMin() {
        return instancesMin;
    }

    public int getInstancesMax() {
        return instancesMax;
    }

    @Override
    public String toString() {
        return "JobSetInstanceCountsCmd{" +
                "user='" + user + '\'' +
                ", jobId='" + jobId + '\'' +
                ", instancesDesired=" + instancesDesired +
                ", instancesMin=" + instancesMin +
                ", instancesMax=" + instancesMax +
                '}';
    }
}
