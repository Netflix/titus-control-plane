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

package com.netflix.titus.api.model.v2;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MachineDefinition {

    private static final double defaultMbps = 0.0;
    private static final int minPorts = 0;
    private final double cpuCores;
    private final double memoryMB;
    private final double networkMbps;
    private final double diskMB;
    private final int numPorts;
    private final Map<String, Double> scalars;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MachineDefinition(@JsonProperty("cpuCores") double cpuCores,
                             @JsonProperty("memoryMB") double memoryMB,
                             @JsonProperty("networkMbps") double networkMbps,
                             @JsonProperty("diskMB") double diskMB,
                             @JsonProperty("numPorts") int numPorts,
                             @JsonProperty("scalars") Map<String, Double> scalars
    ) {
        this.cpuCores = cpuCores;
        this.memoryMB = memoryMB;
        this.networkMbps = networkMbps == 0 ? defaultMbps : networkMbps;
        this.diskMB = diskMB;
        this.numPorts = Math.max(minPorts, numPorts);
        this.scalars = scalars == null ? new HashMap<>() : scalars;
    }

    public MachineDefinition(double cpuCores, double memoryMB, double diskMB, int numPorts) {
        this.cpuCores = cpuCores;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.numPorts = Math.max(minPorts, numPorts);
        this.networkMbps = 128.0;
        this.scalars = null;
    }

    public double getCpuCores() {
        return cpuCores;
    }

    public double getMemoryMB() {
        return memoryMB;
    }

    public double getNetworkMbps() {
        return networkMbps;
    }

    public double getDiskMB() {
        return diskMB;
    }

    public int getNumPorts() {
        return numPorts;
    }

    public Map<String, Double> getScalars() {
        return scalars;
    }
}
