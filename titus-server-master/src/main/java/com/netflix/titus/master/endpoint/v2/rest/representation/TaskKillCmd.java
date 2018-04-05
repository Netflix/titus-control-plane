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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskKillCmd {

    private final String user;
    private final String taskId;
    private final List<String> taskIds;
    private final boolean shrink;
    private final boolean strict;

    @JsonCreator
    public TaskKillCmd(@JsonProperty("user") String user,
                       @JsonProperty("taskId") String taskId,
                       @JsonProperty("taskIds") List<String> taskIds,
                       @JsonProperty("shrink") boolean shrink,
                       @JsonProperty("strict") boolean strict) {

        this.user = user;
        this.taskId = taskId;
        this.taskIds = taskIds;
        this.shrink = shrink;
        this.strict = strict;
    }

    public String getUser() {
        return user;
    }

    public String getTaskId() {
        return taskId;
    }

    public List<String> getTaskIds() {
        return taskIds;
    }

    public boolean isShrink() {
        return shrink;
    }

    public boolean isStrict() {
        return strict;
    }

    @Override
    public String toString() {
        return "TaskKillCmd{" +
                "user='" + user + '\'' +
                ", taskId='" + taskId + '\'' +
                ", taskIds=" + taskIds +
                ", shrink=" + shrink +
                ", strict=" + strict +
                '}';
    }
}
