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

public class ScheduleRepresentation {

    private final String id;
    private final ScheduleDescriptorRepresentation scheduleDescriptor;
    private final ScheduledActionRepresentation currentAction;
    private final List<ScheduledActionRepresentation> completedActions;

    public ScheduleRepresentation(String id,
                                  ScheduleDescriptorRepresentation scheduleDescriptor,
                                  ScheduledActionRepresentation currentAction,
                                  List<ScheduledActionRepresentation> completedActions) {
        this.id = id;
        this.scheduleDescriptor = scheduleDescriptor;
        this.currentAction = currentAction;
        this.completedActions = completedActions;
    }

    public String getId() {
        return id;
    }

    public ScheduleDescriptorRepresentation getScheduleDescriptor() {
        return scheduleDescriptor;
    }

    public ScheduledActionRepresentation getCurrentAction() {
        return currentAction;
    }

    public List<ScheduledActionRepresentation> getCompletedActions() {
        return completedActions;
    }
}
