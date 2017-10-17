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

package io.netflix.titus.master.endpoint.common;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.queues.TaskQueue;

public class TaskSummary {

    private final String name;
    private final Map<TaskQueue.TaskState, Integer> counts;

    @JsonCreator
    public TaskSummary(@JsonProperty("name") String name,
                       @JsonProperty("counts") Map<TaskQueue.TaskState, Integer> counts
    ) {
        this.name = name;
        this.counts = counts;
    }

    public String getName() {
        return name;
    }

    public Map<TaskQueue.TaskState, Integer> getCounts() {
        return counts;
    }
}
