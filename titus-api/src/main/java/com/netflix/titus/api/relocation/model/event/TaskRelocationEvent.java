/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.relocation.model.event;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;

public abstract class TaskRelocationEvent {

    public static TaskRelocationSnapshotEndEvent SNAPSHOT_END_EVENT = new TaskRelocationSnapshotEndEvent();
    public static TaskRelocationKeepAliveEvent KEEP_ALIVE_EVENT = new TaskRelocationKeepAliveEvent();

    public static TaskRelocationEvent newSnapshotEndEvent() {
        return SNAPSHOT_END_EVENT;
    }

    public static TaskRelocationEvent newKeepAliveEvent() {
        return KEEP_ALIVE_EVENT;
    }

    public static TaskRelocationPlanUpdateEvent taskRelocationPlanUpdated(TaskRelocationPlan plan) {
        return new TaskRelocationPlanUpdateEvent(plan);
    }

    public static TaskRelocationPlanRemovedEvent taskRelocationPlanRemoved(String taskId) {
        return new TaskRelocationPlanRemovedEvent(taskId);
    }

    private static class TaskRelocationSnapshotEndEvent extends TaskRelocationEvent {
        @Override
        public boolean equals(Object obj) {
            return obj instanceof TaskRelocationSnapshotEndEvent;
        }
    }

    private static class TaskRelocationKeepAliveEvent extends TaskRelocationEvent {
        @Override
        public boolean equals(Object obj) {
            return obj instanceof TaskRelocationKeepAliveEvent;
        }
    }
}
