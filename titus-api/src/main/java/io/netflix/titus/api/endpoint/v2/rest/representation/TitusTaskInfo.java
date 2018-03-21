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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public class TitusTaskInfo {
    private final String id;
    private final String instanceId;
    private final String jobId;
    private final String cell;
    private final TitusTaskState state;
    private final String applicationName;
    private final double cpu;
    private final double memory;
    private final double networkMbps;
    private final double disk;
    private final Map<Integer, Integer> ports;
    private final int gpu;
    private final Map<String, String> env;
    private final String version;
    private final String entryPoint;
    private final String host;
    private final String submittedAt;
    private final String launchedAt;
    private final String startedAt;
    private final String finishedAt;
    private final String migrationDeadline;
    private final String message;
    private final String stdoutLive;
    private final String logs;
    private final String snapshots;
    private String hostInstanceId;
    private final String itype;
    private final Object data;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public TitusTaskInfo(@JsonProperty("id") String id,
                         @JsonProperty("instanceId") String instanceId,
                         @JsonProperty("jobId") String jobId,
                         @JsonProperty("cell") @JsonInclude(NON_NULL) String cell,
                         @JsonProperty("state") TitusTaskState state,
                         @JsonProperty("applicationName") String applicationName,
                         @JsonProperty("cpu") double cpu,
                         @JsonProperty("memory") double memory,
                         @JsonProperty("networkMbps") double networkMbps,
                         @JsonProperty("disk") double disk,
                         @JsonProperty("ports") Map<Integer, Integer> ports,
                         @JsonProperty("gpu") int gpu,
                         @JsonProperty("env") Map<String, String> env,
                         @JsonProperty("version") String version,
                         @JsonProperty("entryPoint") String entryPoint,
                         @JsonProperty("host") String host,
                         @JsonProperty("submittedAt") String submittedAt,
                         @JsonProperty("launchedAt") String launchedAt,
                         @JsonProperty("startedAt") String startedAt,
                         @JsonProperty("finishedAt") String finishedAt,
                         @JsonProperty("migrationDeadline") String migrationDeadline,
                         @JsonProperty("message") String message,
                         @JsonProperty("data") Object data,
                         @JsonProperty("stdoutLive") String stdoutLive,
                         @JsonProperty("logs") String logs,
                         @JsonProperty("snapshots") String snapshots,
                         @JsonProperty("hostInstanceId") String hostInstanceId,
                         @JsonProperty("itype") String itype
    ) {
        this.id = id;
        this.instanceId = instanceId;
        this.jobId = jobId;
        this.cell = cell;
        this.state = state;
        this.applicationName = applicationName;
        this.cpu = cpu;
        this.memory = memory;
        this.networkMbps = networkMbps;
        this.disk = disk;
        this.ports = ports;
        this.gpu = gpu;
        this.env = env;
        this.version = version;
        this.entryPoint = entryPoint;
        this.host = host;
        this.submittedAt = submittedAt;
        this.launchedAt = launchedAt;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.migrationDeadline = migrationDeadline;
        this.message = message;
        this.data = data;
        this.stdoutLive = stdoutLive;
        this.logs = logs;
        this.snapshots = snapshots;
        this.hostInstanceId = hostInstanceId;
        this.itype = itype;
    }

    public String getId() {
        return id;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getCell() {
        return cell;
    }

    public TitusTaskState getState() {
        return state;
    }

    public String getHost() {
        return host;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public double getCpu() {
        return cpu;
    }

    public double getMemory() {
        return memory;
    }

    public double getNetworkMbps() {
        return networkMbps;
    }

    public double getDisk() {
        return disk;
    }

    public Map<Integer, Integer> getPorts() {
        return ports;
    }

    public int getGpu() {
        return gpu;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public String getVersion() {
        return version;
    }

    public String getEntryPoint() {
        return entryPoint;
    }

    public String getSubmittedAt() {
        return submittedAt;
    }

    public String getLaunchedAt() {
        return launchedAt;
    }

    public String getStartedAt() {
        return startedAt;
    }

    public String getFinishedAt() {
        return finishedAt;
    }

    public String getMigrationDeadline() {
        return migrationDeadline;
    }

    public String getMessage() {
        return message;
    }

    public String getStdoutLive() {
        return stdoutLive;
    }

    public String getLogs() {
        return logs;
    }

    public String getSnapshots() {
        return snapshots;
    }

    public Object getData() {
        return data;
    }

    public String getHostInstanceId() {
        return hostInstanceId;
    }

    public String getItype() {
        return itype;
    }
}
