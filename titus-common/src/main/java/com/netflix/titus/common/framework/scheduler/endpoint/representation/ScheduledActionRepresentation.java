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

import java.util.List;

public class ScheduledActionRepresentation {

    private final String id;
    private final String executionId;
    private final SchedulingStatusRepresentation status;
    private final List<SchedulingStatusRepresentation> statusHistory;

    public ScheduledActionRepresentation(String id,
                                         String executionId,
                                         SchedulingStatusRepresentation status,
                                         List<SchedulingStatusRepresentation> statusHistory) {

        this.id = id;
        this.executionId = executionId;
        this.status = status;
        this.statusHistory = statusHistory;
    }

    public String getId() {
        return id;
    }

    public String getExecutionId() {
        return executionId;
    }

    public SchedulingStatusRepresentation getStatus() {
        return status;
    }

    public List<SchedulingStatusRepresentation> getStatusHistory() {
        return statusHistory;
    }
}
