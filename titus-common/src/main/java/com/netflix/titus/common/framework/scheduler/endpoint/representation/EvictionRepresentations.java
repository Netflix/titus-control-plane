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

import java.util.stream.Collectors;

import com.netflix.titus.common.framework.scheduler.model.ExecutionId;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus;
import com.netflix.titus.common.util.DateTimeExt;

public final class EvictionRepresentations {

    private EvictionRepresentations() {
    }

    public static ScheduleRepresentation toRepresentation(Schedule core) {
        return new ScheduleRepresentation(
                core.getId(),
                toRepresentation(core.getDescriptor()),
                toRepresentation(core.getCurrentAction()),
                core.getCompletedActions().stream().map(EvictionRepresentations::toRepresentation).collect(Collectors.toList())
        );
    }

    private static ScheduleDescriptorRepresentation toRepresentation(ScheduleDescriptor descriptor) {
        return new ScheduleDescriptorRepresentation(
                descriptor.getName(),
                descriptor.getDescription(),
                DateTimeExt.toTimeUnitString(descriptor.getInterval().toMillis()),
                DateTimeExt.toTimeUnitString(descriptor.getTimeout().toMillis())
        );
    }

    private static ScheduledActionRepresentation toRepresentation(ScheduledAction currentAction) {
        return new ScheduledActionRepresentation(
                currentAction.getId(),
                doFormat(currentAction.getExecutionId()),
                toRepresentation(currentAction.getStatus()),
                currentAction.getStatusHistory().stream().map(EvictionRepresentations::toRepresentation).collect(Collectors.toList())
        );
    }

    private static String doFormat(ExecutionId executionId) {
        return String.format("%s.%s(%s)", executionId.getId(), executionId.getAttempt(), executionId.getTotal());
    }

    private static SchedulingStatusRepresentation toRepresentation(SchedulingStatus status) {
        return new SchedulingStatusRepresentation(
                status.getState(),
                DateTimeExt.toUtcDateTimeString(status.getTimestamp()),
                DateTimeExt.toUtcDateTimeString(status.getExpectedStartTime()),
                status.getError().map(Throwable::getMessage).orElse(null)
        );
    }
}
