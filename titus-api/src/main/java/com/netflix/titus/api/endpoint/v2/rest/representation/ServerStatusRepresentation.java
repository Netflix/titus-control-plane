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

package com.netflix.titus.api.endpoint.v2.rest.representation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerStatusRepresentation {

    private final boolean leader;
    private final boolean active;
    private final String uptime;
    private final String electionTimeStamp;
    private final String activeTimeStamp;
    private final String activationTime;
    private final List<ServiceActivation> serviceActivationTimes;
    private final List<String> serviceActivationOrder;

    @JsonCreator
    public ServerStatusRepresentation(
            @JsonProperty("leader") boolean leader,
            @JsonProperty("active") boolean active,
            @JsonProperty("upTime") String uptime,
            @JsonProperty("electionTimeStamp") String electionTimeStamp,
            @JsonProperty("activeTimeStamp") String activeTimeStamp,
            @JsonProperty("activationTime") String activationTime,
            @JsonProperty("serviceActivationTimes") List<ServiceActivation> serviceActivationTimes,
            @JsonProperty("serviceActivationOrder") List<String> serviceActivationOrder) {
        this.leader = leader;
        this.active = active;
        this.uptime = uptime;
        this.electionTimeStamp = electionTimeStamp;
        this.activeTimeStamp = activeTimeStamp;
        this.activationTime = activationTime;
        this.serviceActivationTimes = serviceActivationTimes;
        this.serviceActivationOrder = serviceActivationOrder;
    }

    public boolean isLeader() {
        return leader;
    }

    public boolean isActive() {
        return active;
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

    public List<ServiceActivation> getServiceActivationTimes() {
        return serviceActivationTimes;
    }

    public List<String> getServiceActivationOrder() {
        return serviceActivationOrder;
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
