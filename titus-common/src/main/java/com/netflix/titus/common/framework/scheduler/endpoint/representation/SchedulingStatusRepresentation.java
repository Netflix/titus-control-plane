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

import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;

public class SchedulingStatusRepresentation {

    private final SchedulingState state;
    private final String timestamp;
    private final String expectedStartTime;
    private final String error;

    public SchedulingStatusRepresentation(SchedulingState state, String timestamp, String expectedStartTime, String error) {
        this.state = state;
        this.timestamp = timestamp;
        this.expectedStartTime = expectedStartTime;
        this.error = error;
    }

    public SchedulingState getState() {
        return state;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getExpectedStartTime() {
        return expectedStartTime;
    }

    public String getError() {
        return error;
    }
}
