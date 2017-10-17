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

package io.netflix.titus.api.endpoint.v2.rest.representation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerStatusRepresentation {

    private final boolean leader;
    private final String uptime;
    private final String electionTimeStamp;
    private final String activeTimeStamp;
    private final String activationTime;
    private final List<ServiceActivation> serviceActionTime;

    @JsonCreator
    public ServerStatusRepresentation(
            @JsonProperty("upTime") String uptime,
            @JsonProperty("leader") boolean leader,
            @JsonProperty("electionTimeStamp") String electionTimeStamp,
            @JsonProperty("activeTimeStamp") String activeTimeStamp,
            @JsonProperty("activationTime") String activationTime,
            @JsonProperty("serviceActionTime") List<ServiceActivation> serviceActionTime) {
        this.leader = leader;
        this.uptime = uptime;
        this.electionTimeStamp = electionTimeStamp;
        this.activeTimeStamp = activeTimeStamp;
        this.activationTime = activationTime;
        this.serviceActionTime = serviceActionTime;
    }

    public boolean isLeader() {
        return leader;
    }

    public String getUptime() {
        return uptime;
    }

    public String getElectionTimeStamp() {
        return electionTimeStamp;
    }

    public String getActiveTimeStamp() {
        return activeTimeStamp;
    }

    public String getActivationTime() {
        return activationTime;
    }

    public List<ServiceActivation> getServiceActionTime() {
        return serviceActionTime;
    }

    public static class ServiceActivation {
        private final String service;
        private final String duration;

        @JsonCreator
        public ServiceActivation(
                @JsonProperty("service") String service,
                @JsonProperty("duration") String duration) {
            this.service = service;
            this.duration = duration;
        }

        public String getService() {
            return service;
        }

        public String getDuration() {
            return duration;
        }
    }
}
