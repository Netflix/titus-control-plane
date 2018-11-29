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

package com.netflix.titus.common.framework.scheduler.endpoint.representation;

public class ScheduleDescriptorRepresentation {

    private final String name;
    private final String description;
    private final String initialDelay;
    private final String interval;
    private final String timeout;

    public ScheduleDescriptorRepresentation(String name, String description, String initialDelay, String interval, String timeout) {
        this.name = name;
        this.description = description;
        this.initialDelay = initialDelay;
        this.interval = interval;
        this.timeout = timeout;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getInitialDelay() {
        return initialDelay;
    }

    public String getInterval() {
        return interval;
    }

    public String getTimeout() {
        return timeout;
    }
}
