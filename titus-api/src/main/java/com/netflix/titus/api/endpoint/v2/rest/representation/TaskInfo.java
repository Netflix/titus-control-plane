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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public class TaskInfo {

    private final String id;
    private final String instanceId;
    private final String cell;
    private final TitusTaskState state;
    private final String host;
    private final String hostInstanceId;
    private final String itype;
    private final String region;
    private final String zone;
    private final String submittedAt;
    private final String launchedAt;
    private final String startedAt;
    private final String finishedAt;
    private final String migrationDeadline;
    private final String message;
    private final Object data;
    private final String stdoutLive;
    private final String logs;
    private final String snapshots;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public TaskInfo(@JsonProperty("id") String id,
                    @JsonProperty("instanceId") String instanceId,
                    @JsonProperty("cell") @JsonInclude(NON_NULL) String cell,
                    @JsonProperty("state") TitusTaskState state,
                    @JsonProperty("host") String host,
                    @JsonProperty("hostInstanceId") String hostInstanceId,
                    @JsonProperty("itype") String itype,
                    @JsonProperty("region") String region,
                    @JsonProperty("zone") String zone,
                    @JsonProperty("submittedAt") String submittedAt,
                    @JsonProperty("launchedAt") String launchedAt,
                    @JsonProperty("startedAt") String startedAt,
                    @JsonProperty("finishedAt") String finishedAt,
                    @JsonProperty("migrationDeadline") String migrationDeadline,
                    @JsonProperty("message") String message,
                    @JsonProperty("data") Object data,
                    @JsonProperty("stdoutLive") String stdoutLive,
                    @JsonProperty("logs") String logs,
                    @JsonProperty("snapshots") String snapshots

    ) {
        this.id = id;
        this.instanceId = instanceId;
        this.cell = cell;
        this.state = state;
        this.host = host;
        this.hostInstanceId = hostInstanceId;
        this.itype = itype;
        this.region = region;
        this.zone = zone;
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
    }

    public String getId() {
        return id;
    }

    public String getInstanceId() {
        return instanceId;
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

    public String getHostInstanceId() {
        return hostInstanceId;
    }

    public String getItype() {
        return itype;
    }

    public String getRegion() {
        return region;
    }

    public String getZone() {
        return zone;
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

    public Object getData() {
        return data;
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
}
