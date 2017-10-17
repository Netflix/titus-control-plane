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

package io.netflix.titus.api.model.v2.descriptor;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.MachineDefinition;

public class StageSchedulingInfo {

    private int numberOfInstances;
    private MachineDefinition machineDefinition;
    private List<JobConstraints> hardConstraints;
    private List<JobConstraints> softConstraints;
    private List<String> securityGroups;
    private boolean allocateIP;
    private StageScalingPolicy scalingPolicy;
    private boolean scalable;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageSchedulingInfo(@JsonProperty("numberOfInstances") int numberOfInstances,
                               @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
                               @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                               @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                               @JsonProperty("securityGroups") List<String> securityGroups,
                               @JsonProperty("allocateIP") boolean allocateIP,
                               @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
                               @JsonProperty("scalable") boolean scalable) {
        this.numberOfInstances = numberOfInstances;
        this.machineDefinition = machineDefinition;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.securityGroups = securityGroups;
        this.allocateIP = allocateIP;
        this.scalingPolicy = scalingPolicy;
        this.scalable = scalable;
    }

    public int getNumberOfInstances() {
        return numberOfInstances;
    }

    public MachineDefinition getMachineDefinition() {
        return machineDefinition;
    }

    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    public List<String> getSecurityGroups() {
        return securityGroups;
    }

    public boolean getAllocateIP() {
        return allocateIP;
    }

    public StageScalingPolicy getScalingPolicy() {
        return scalingPolicy;
    }

    public boolean getScalable() {
        return scalable;
    }

    public void setScalingPolicy(StageScalingPolicy scalingPolicy) {
        this.scalingPolicy = scalingPolicy;
    }

    public static void main(String[] args) {
        String json = "{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":2048.0,\"diskMB\":1.0,\"numPorts\":1},\"hardConstraints\":[\"UniqueHost\"],\"softConstraints\":[\"ExclusiveHost\"],\"scalable\":\"true\"}";
        ObjectMapper mapper = new ObjectMapper();
        try {
            StageSchedulingInfo info = mapper.readValue(json, StageSchedulingInfo.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
