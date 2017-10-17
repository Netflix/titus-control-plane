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

package io.netflix.titus.testkit.embedded.cloud.endpoint.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.mesos.Protos;

public class SimulatedContainerRepresentation {

    private final String taskId;
    private final String jobId;
    private final Protos.TaskState state;
    private final double cpu;
    private final double gpu;
    private final double memoryMB;
    private final double diskMB;
    private final double networkMbs;
    private final String containerIp;

    @JsonCreator
    public SimulatedContainerRepresentation(@JsonProperty("taskId") String taskId,
                                            @JsonProperty("jobId") String jobId,
                                            @JsonProperty("state") Protos.TaskState state,
                                            @JsonProperty("cpu") double cpu,
                                            @JsonProperty("gpu") double gpu,
                                            @JsonProperty("memoryMB") double memoryMB,
                                            @JsonProperty("diskMB") double diskMB,
                                            @JsonProperty("networkMbps") double networkMbps,
                                            @JsonProperty("containerIp") String containerIp) {

        this.taskId = taskId;
        this.jobId = jobId;
        this.state = state;
        this.cpu = cpu;
        this.gpu = gpu;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.networkMbs = networkMbps;
        this.containerIp = containerIp;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public Protos.TaskState getState() {
        return state;
    }

    public double getCpu() {
        return cpu;
    }

    public double getGpu() {
        return gpu;
    }

    public double getMemoryMB() {
        return memoryMB;
    }

    public double getDiskMB() {
        return diskMB;
    }

    public double getNetworkMbs() {
        return networkMbs;
    }

    public String getContainerIp() {
        return containerIp;
    }
}
