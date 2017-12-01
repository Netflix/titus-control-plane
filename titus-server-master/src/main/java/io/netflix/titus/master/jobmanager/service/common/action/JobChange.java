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

package io.netflix.titus.master.jobmanager.service.common.action;

import io.netflix.titus.api.jobmanager.service.V3JobOperations;

/**
 * Describes a job or task change action.
 */
public class JobChange {

    private final V3JobOperations.Trigger trigger;
    private final String id;
    private final String name;
    private final String summary;

    public JobChange(V3JobOperations.Trigger trigger, String id, String summary) {
        this.trigger = trigger;
        this.id = id;
        this.name = "???";
        this.summary = summary;
    }

    public JobChange(V3JobOperations.Trigger trigger, String id, String name, String summary) {
        this.trigger = trigger;
        this.id = id;
        this.name = name;
        this.summary = summary;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public V3JobOperations.Trigger getTrigger() {
        return trigger;
    }

    public String getSummary() {
        return summary;
    }
}
