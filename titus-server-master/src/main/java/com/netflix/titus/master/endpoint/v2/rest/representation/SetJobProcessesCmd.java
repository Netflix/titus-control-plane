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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SetJobProcessesCmd {
    private static Logger logger = LoggerFactory.getLogger(SetJobProcessesCmd.class);
    private String user;
    private String jobId;
    private boolean disableIncreaseDesired;
    private boolean disableDecreaseDesired;

    @JsonCreator
    public SetJobProcessesCmd(@JsonProperty("user") String user,
                              @JsonProperty("jobId") String jobId,
                              @JsonProperty("disableIncreaseDesired") boolean disableIncreaseDesired,
                              @JsonProperty("disableDecreaseDesired") boolean disableDecreaseDesired) {
        this.user = user;
        this.jobId = jobId;
        this.disableIncreaseDesired = disableIncreaseDesired;
        this.disableDecreaseDesired = disableDecreaseDesired;
    }

    public String getUser() {
        return user;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isDisableIncreaseDesired() {
        return disableIncreaseDesired;
    }

    public boolean isDisableDecreaseDesired() {
        return disableDecreaseDesired;
    }

    @Override
    public String toString() {
        return "SetJobProcessesCmd{" +
                "user='" + user + '\'' +
                ", jobId='" + jobId + '\'' +
                ", disableIncreaseDesired=" + disableIncreaseDesired +
                ", disableDecreaseDesired=" + disableDecreaseDesired +
                '}';
    }
}
