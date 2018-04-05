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
import com.fasterxml.jackson.annotation.JsonProperty;

public class LogLinksRepresentation {
    private final String stdoutLive;
    private final String logs;
    private final String snapshots;

    @JsonCreator
    public LogLinksRepresentation(@JsonProperty("stdoutLive") String stdoutLive,
                                  @JsonProperty("logs") String logs,
                                  @JsonProperty("snapshots") String snapshots) {

        this.stdoutLive = stdoutLive;
        this.logs = logs;
        this.snapshots = snapshots;
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
